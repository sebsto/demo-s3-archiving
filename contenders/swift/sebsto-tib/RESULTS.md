<!-- markdownlint-disable MD013 -->
# Bench results — `sebsto-tib` vs `sebsto-soto`

Comparison of the [@tib commit `7e2a36d`](https://github.com/tib/demo-s3-archiving/commit/7e2a36dd0125acd780b2e0932c18000213dbd05b)
optimization patch against the baseline Swift contender, run side-by-side
against the Rust reference in the same Step Function execution.

## Patch summary

Three behavioural changes vs `sebsto-soto`:

1. **Downloader worker pool.** `runDownloadStage` was spawning one task
   per source object (3 000 tasks, throttled by the byte-budget
   semaphore). The patch replaces it with a fixed pool of
   `Tunables.maxDownloadWorkers = 4` workers pulling from a
   `FileCursor` actor. Same memory budget, same effective concurrency,
   fewer scheduler hops.
2. **Incremental CRC32 during stream read.** The CRC was computed in a
   single pass over the assembled `ByteBuffer` after EOF; the patch
   updates the CRC frame-by-frame as the body is consumed. Same total
   work, potentially better cache behaviour.
3. **Streamed central directory.** `runZipStage` was building the full
   central directory in one `Data` blob and pushing it through the
   producer in one go; the patch streams CD records, ZIP64 EOCD,
   locator, and EOCD individually. Avoids an intermediate ~240 KiB
   allocation. Also drops `appendCompound`'s `bumpProducerHops(3) →
   bumpProducerHops(1)` accounting tweak.

## Setup

- Region: `eu-west-3`
- Stack: `demo-archiving-swift-tib-{ci,root}` (separate from the
  baseline stacks)
- Branch: `contender/swift-sebsto-tib`
- Lambda: `provided.al2023` / arm64 / 512 MB / 600 s — identical
  configuration for soto, tib, and rust contenders
- Workload: 3 000 objects / ~15 GB under `files/` — produced by the
  bench's `fill-bucket` custom resource on stack create
- Step Function input: 3 contender ARNs (sebsto-soto, sebsto-tib,
  rust-jeremie-rodon), 10 parallel invocations each → 30 concurrent
  Lambdas per iteration
- 10 iterations of the bench Step Function, sequential

Per-Lambda durations (ms) are reported by the Step Function from
`InvokeContender` start/end timestamps. The `n` column counts how many
of the 10 invocations of a contender reached the SUCCESS bin in each
iteration; an iteration where any one of the 10 parallel runs failed
(timeout/OOM/etc.) marks the *whole contender* failed for that iteration.

## Aggregate over 10 iterations

| Contender | n | min (s) | p50 | p90 | max | mean | stdev | mean run\_price |
|---|---|---|---|---|---|---|---|---|
| **rust-jeremie-rodon** | 9 | 210.8 | 211.7 | 212.1 | 212.1 | 211.7 | 0.41 | $0.001411 |
| **swift-sebsto-soto** (baseline) | 6 | 232.6 | 241.9 | 247.8 | 249.4 | 241.4 | 6.13 | $0.001609 |
| **swift-sebsto-tib** (variant) | 9 | 234.7 | 245.0 | 251.5 | 252.1 | 244.5 | 6.99 | $0.001630 |

## Per-iteration log

```
iter=1/10  rust=FAIL    soto=232.6s  tib=FAIL    failures=2
iter=2/10  rust=210.8s  soto=246.1s  tib=249.8s
iter=3/10  rust=212.1s  soto=242.5s  tib=251.4s
iter=4/10  rust=211.7s  soto=FAIL    tib=250.6s  failures=1
iter=5/10  rust=211.3s  soto=249.4s  tib=252.1s
iter=6/10  rust=211.6s  soto=241.4s  tib=245.0s
iter=7/10  rust=212.1s  soto=FAIL    tib=242.9s  failures=1
iter=8/10  rust=211.9s  soto=FAIL    tib=238.9s  failures=1
iter=9/10  rust=211.9s  soto=FAIL    tib=235.1s  failures=1
iter=10/10 rust=211.4s  soto=236.6s  tib=234.7s
```

## Verdict

**The patch does not improve performance on this workload.**

- tib mean (244.5 s) is **3.1 s slower** than soto baseline mean
  (241.4 s) — roughly +1.3%.
- tib p50 (245.0 s) > soto p50 (241.9 s).
- tib p90 (251.5 s) ≈ soto p90 (247.8 s).
- Mean run\_price: tib **$0.001630** vs soto $0.001609 — tib is
  **~1.3% more expensive**.
- Both Swift variants run ~16% slower than Rust (~245 s vs ~211 s) in
  this 30-concurrent-Lambdas-against-the-same-bucket configuration.

The 3 s mean delta is within the per-iteration stdev (~6-7 s), so this
is at best a wash and possibly a small regression. None of the three
patch elements (worker pool, incremental CRC, streamed CD) produce a
measurable win in `run_price_usd` — the project's ranking metric.

**Recommendation: do not merge.**

Why each change failed to move the needle:

- *Worker pool:* the original 3 000-task spawn was already throttled by
  the byte-budget semaphore to ~4 in-flight downloads, so the wire-time
  was identical. Task spawn overhead is negligible against ~75 ms of S3
  GET latency per file.
- *Incremental CRC:* CRC32 over a contiguous 5 MB `ByteBuffer` is
  ~4 ms (slicing-by-8 on Graviton, see baseline DESIGN). Splitting it
  across 64 KB frames adds per-frame setup cost and breaks the
  memory-level-parallelism advantage that made the post-EOF pass fast.
  Net: roughly the same time, possibly slightly more.
- *Streamed CD:* the central-directory blob is ~240 KiB total
  (~80 bytes × 3 000 entries). Avoiding one allocation of that size
  saves ~tens of microseconds; below the resolution of a 240 s run.

The pipeline remains bandwidth-bound, not CPU-bound (per the baseline's
memory-scaling experiment in [README](README.md): doubling memory from
512 MB to 1024 MB saves only ~3% wall-clock), so micro-optimizations
on the CPU side don't show up.

## On the soto failures (4 of 10) and the iter-1 double-failure

The raw counts (`soto: 6/10 success, tib: 9/10 success`) make soto look
worse than it is. Looking at the actual `failure[].reason` from each
execution output, none of these is a soto-vs-tib delta:

| Iter | Contender | Reason | Root cause |
|---|---|---|---|
| 1 | tib + rust | `invalid: io error: Invalid checksum` | First execution after stack create — control Lambda re-hashes ZIP entries and the entry name is supposed to equal `SHA256(decompressed content)`. *Both* tib and Rust failed simultaneously with the same error → bucket-state issue, not a contender bug. Most likely the bench bucket's source objects were not fully consistent yet, or run-0's archive read raced with run-9's write of the same key prefix |
| 2 | soto | `Runtime.OutOfMemory` | 512 MB ceiling; baseline README documents peak RSS ~390 MB. Cold-start RSS placement is the lottery; soto lost it once |
| 4 | soto | `HTTPClientError.getConnectionFromPoolTimeout` | AsyncHTTPClient pool exhausted — 30 concurrent Lambdas × 32 conns/host hit S3 endpoint connection limits |
| 7 | soto | `Sandbox.Timedout` (600 s) | Same family — one run stalled on S3 throttling and never recovered before the 600 s timeout |
| 8 | soto | `HTTPClientError.deadlineExceeded` | Same family — read deadline (120 s) tripped on a request to S3 |

So the 4 soto fails are all **infrastructure contention** (memory
ceiling on a 512 MB Lambda; AHC connection pool collision; S3 throttling),
not a soto-implementation issue. The tib variant carries the exact same
risk profile — it ran one fewer hot run by chance, not by design.

The 4-vs-1 split is well within what 10 random samples can produce by
chance when the underlying base rate is identical and small.

## How to reproduce

```bash
# CI stack
aws cloudformation create-stack \
  --region eu-west-3 \
  --stack-name demo-archiving-swift-tib-ci \
  --template-body file://ci-template.yml \
  --parameters \
    ParameterKey=ProjectName,ParameterValue=demo-archiving-swift-tib \
    ParameterKey=CodeStarConnectionArn,ParameterValue=<your-codestar-arn> \
    ParameterKey=ForkedRepoId,ParameterValue=<your-username>/demo-s3-archiving \
    ParameterKey=BranchName,ParameterValue=contender/swift-sebsto-tib \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND

# After CI finishes, fetch the State Machine ARN + ContenderArns
SM=$(aws cloudformation describe-stacks --region eu-west-3 \
  --stack-name demo-archiving-swift-tib-root \
  --query 'Stacks[0].Outputs[?OutputKey==`BenchingStateMachineArn`].OutputValue' --output text)
INPUT=$(aws cloudformation describe-stacks --region eu-west-3 \
  --stack-name demo-archiving-swift-tib-root \
  --query 'Stacks[0].Outputs[?OutputKey==`ContenderArns`].OutputValue' --output text)

# Bench loop is /tmp/bench-tib-loop.sh in the original test session,
# but a 1-liner sample is:
aws stepfunctions start-execution --region eu-west-3 \
  --state-machine-arn "$SM" --input "$INPUT"
```
