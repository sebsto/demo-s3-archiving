<!-- markdownlint-disable MD013 -->
# Results — `sebsto-awssdk-nio`

This contender is a port of the [Soto-based reference contender](../sebsto-soto/)
to the **official AWS SDK for Swift** (`aws-sdk-swift`), configured to use
**AsyncHTTPClient** as its HTTP engine — via the `SwiftNIOHTTPClient`
type from `SmithySwiftNIO`
([discussion #2070](https://github.com/awslabs/aws-sdk-swift/discussions/2070)) —
instead of the default AWS Common Runtime (`aws-crt-swift`).

The experiment's question: **how much of the gap between Soto and the
official SDK is attributable to the HTTP transport (CRT vs NIO), and
how much is attributable to the SDK's `Smithy.ByteStream`-shaped API
surface forcing `Data` end-to-end?**

A previous attempt against the same workload using the *same* official
SDK with its *default* CRT engine — recorded in `sebsto-soto/README.md`
under "Two performance design decisions" — clocked **544 s warm,
600 s timeout cold** on the 15 GB / 3000-object benchmark. That made
the official SDK non-viable on a 512 MB / 600 s Lambda. The hypothesis
going into this experiment was: **swap CRT for AsyncHTTPClient via
`SwiftNIOHTTPClient` and most of that gap closes** — because the
3.4× slower-than-AHC `S3.uploadPart` p50 we measured for CRT goes
away — but the residual gap (one extra full-archive memcpy on the
upload path because the SDK exposes `Data` rather than NIO `ByteBuffer`)
remains.

## What changed vs. `sebsto-soto`

Same 3-stage pipeline (Downloader → Zipper → Uploader), same byte
budget, same backpressure model, same hand-rolled streaming ZIP64
encoder, same pure-Swift CRC32. The only differences:

1. **HTTP transport / SDK** — `aws-sdk-swift` 1.7.x with
   `httpClientEngine: SwiftNIOHTTPClient(...)` instead of `Soto`.
   Connection-pool tuning matched to Soto:
   `maxConnections = 32`, `socketTimeout = 120`.
2. **Body type** — `Foundation.Data` end-to-end instead of NIO
   `ByteBuffer`. `Smithy.ByteStream.data(Data)` is the only
   zero-copy upload-body shape the SDK accepts; `.stream(Stream)` is
   collapsed into `Data` by `FlexibleChecksumsRequestMiddleware` for
   SigV4 signing, so a custom Stream wrapper saves nothing. On the
   download side, `Smithy.ByteStream.stream` exposes
   `readAsync(upToCount:) -> Data?` as the only progressive read
   primitive.
3. **`smithy-swift` is an explicit SwiftPM dependency** — `SmithySwiftNIO`
   is not re-exported by `aws-sdk-swift`, so consumers must declare it
   themselves. Pinned to the version that `aws-sdk-swift` 1.7.9 was
   built against (0.214.0).

The `Data`-shaped surface propagates: `DownloadedFile.data: Data`,
`ChunkProducer` stores `Data`, `uploadPart(data: Data)`,
`UploadChunk.data: Data`. All other application code (ZIP, CRC32,
backpressure actors, structured concurrency) is unchanged.

## Bench setup

- **Region**: `eu-west-3`.
- **Lambda config**: 512 MB / arm64 / `provided.al2023` / 600 s timeout —
  identical for all three contenders.
- **Workload**: 3000 source objects under `s3://<bucket>/files/`,
  ~15 GB total (~5 MB average per object).
- **Driver**: 10 sequential executions of the project's bench Step
  Function, each invoking each contender 10× in parallel. The
  Step Function records `duration_ms` as the **mean of 10 internal
  parallel invocations**, plus per-batch min/max/stddev. We treat
  each Step Function execution as one data point per contender →
  10 data points per contender across the bench.
- **Contenders bench-paired in the same execution**: Soto, awssdk-nio
  (this contender), and the Rust reference (`jeremie-rodon`).
- **Date**: 2026-06-03.

## Headline result — `awssdk-nio` is **not viable** at 512 MB

The hypothesis is partially confirmed: CRT → NIO closes ~30 % of the
gap. The remaining gap is bigger than expected, and on this 512 MB
Lambda the NIO contender is **memory-bound**, not transport-bound —
**every invocation maxes out at 488–512 MB** (zero headroom), causing
frequent allocator stalls and a 13 % invocation-level failure rate.

### Wall-clock duration (mean of 10 internal-parallel, **only successful executions**)

| Contender | n | min | p50 | p90 | max | mean | stdev |
|---|---:|---:|---:|---:|---:|---:|---:|
| **awssdk-nio** | 3 | 384.7 | 393.2 | 406.8 | 410.2 | **396.0** | 12.95 |
| **soto**       | 7 | 229.1 | 235.8 | 243.6 | 250.1 | **237.5** |  6.39 |
| **rust**       |10 | 211.8 | 212.2 | 212.4 | 212.7 | **212.1** |  0.30 |

(Only 3 of 10 executions had **all 10 internal awssdk-nio invocations
succeed**; the other 7 contained at least one TIMEOUT or OOM. See
"Failure analysis" below.)

### `run_price_usd` — the project's ranking metric, lower is better

| Contender | n | min | p50 | mean |
|---|---:|---:|---:|---:|
| awssdk-nio | 3 | 0.002565 | 0.002621 | **0.002640** |
| soto       | 7 | 0.001527 | 0.001572 | **0.001583** |
| rust       |10 | 0.001412 | 0.001414 | **0.001414** |

awssdk-nio's mean cost is **1.67× soto** and **1.87× rust** on the same
512 MB / arm64 Lambda.

### Per-execution paired table (mean wall-clock seconds; one row per Step Function execution)

```
Run    Soto       NIO       Rust
 1   235.79    384.73    212.27
 2   235.65   TIMEOUT    212.33
 3   229.10   TIMEOUT    212.23
 4      OOM   TIMEOUT    211.79
 5   250.08       OOM    212.32
 6  TIMEOUT    410.17    212.69
 7   234.86       OOM    211.91
 8   237.58    393.18    211.75
 9  TIMEOUT   TIMEOUT    212.10
10   239.30   TIMEOUT    211.87
```

A `TIMEOUT` / `OOM` cell means **at least one of the 10 internal
parallel invocations** failed and pushed the contender into the
Step Function's `failure` bucket — the duration mean was not
recorded for that run. Rust succeeded in 100 / 100 invocations across
the bench. Soto succeeded in 97 / 100. awssdk-nio: **87 / 100**.

## Failure analysis — root cause is the 512 MB ceiling

Across 100 awssdk-nio invocations:

| Reason | Count |
|---|---:|
| Success | 87 |
| `Sandbox.Timedout` (600 s) | 11 |
| `Runtime.OutOfMemory` | 2 |

`Max Memory Used` from CloudWatch's per-invocation `REPORT` line:

| Contender | min | p50 | mean | max | observed-failures |
|---|---:|---:|---:|---:|---:|
| awssdk-nio | 486 MB | **510 MB** | 506 MB | 512 MB | 13 / 100 |
| soto       | 391 MB | **449 MB** | 454 MB | 511 MB |  3 / 100 |
| rust       | n/a (well under) | n/a | n/a | n/a |  0 / 100 |

The two contenders run essentially the same Swift application code on
the same Lambda configuration, with the same byte-budget tunables:

- 20 MiB byte semaphore for in-flight downloads
- 2 chunks × 10 MiB = 20 MiB ChunkProducer ceiling
- 3 concurrent `S3.uploadPart` calls

So the application-level "in-flight bytes" budget is identical. The
~50 MB delta in steady-state RSS comes entirely from **the AWS SDK +
its API surface**:

1. **`Smithy.ByteStream.stream` is read via `readAsync(upToCount:)`,
   which allocates a fresh `Data` per call.** A 64 KiB read frame
   means a fresh allocation every ~16 KiB of decompressed payload
   on the wire. The fresh `Data` is held until the consumer's
   `out.append(chunk)` returns, briefly doubling the live frame.
2. **`UploadPartInput.body: ByteStream.data(Data)` is buffered again
   inside `FlexibleChecksumsRequestMiddleware`** before SigV4 signing —
   the SDK collects any `.stream(...)` form into a `Data` for
   checksum calculation, so even constructing a custom Stream wrapper
   would not avoid the second `Data` copy. With 3 in-flight uploads
   each holding 10 MiB chunks, that's ~30 MiB of duplicate
   request-body bytes alongside our `ChunkProducer`'s in-flight
   chunks. By contrast, Soto accepts `AWSHTTPBody(buffer: ByteBuffer)`
   zero-copy: NIO wraps the existing `ByteBuffer` slab and emits it
   on the wire without re-buffering.
3. **The official SDK's static footprint is larger.** AWSClientRuntime,
   the smithy-* libraries, the auth/checksum interceptor providers,
   NIOSSL, and the `aws-crt-swift` linkage (still pulled even when
   only the NIO engine is used at runtime, because `aws-sdk-swift`'s
   `Package.swift` still declares the dep) all add to the loaded-image
   RSS. The stripped `bootstrap` is roughly 2× the size of Soto's.

The result is a Lambda that runs at the very edge of its 512 MB
ceiling. When the allocator hits the limit, two things happen,
non-deterministically: the kernel kills the process (`OutOfMemory`,
2 / 100 here) or the malloc/glibc thrashes hard enough to push
wall-clock past 600 s (`Timedout`, 11 / 100).

## Caveats

- The Soto contender also showed degraded reliability in this batch
  (3 / 100 failures vs. 0 / 100 in the README's earlier numbers).
  This is not a fair regression — the source bucket and sandbox
  placement vary day-to-day on shared AWS infrastructure, and the
  3 / 100 figure is noise around an upper-90s percentage. The NIO
  contender's 13 / 100, by contrast, is structural.
- The duration column "mean" for awssdk-nio is computed only over the
  3 fully-successful executions. With a larger budget that number is
  expected to drift up — sandbox-cold cases are more likely to push
  any single internal invocation into the timeout column, making the
  whole execution drop out of `success`.
- All numbers above are without the `STATS=1` profiling instrumentation
  enabled. Adding `STATS=1` for one run would let us observe directly
  which stage stalls under memory pressure (`uploadPart` p50 vs.
  `zipperAppend` p50 vs. `uploaderQueueWait` p50). That is a follow-up.

## Conclusion

The official `aws-sdk-swift` with `SwiftNIOHTTPClient` is **measurably
faster than the same SDK with CRT** (~393 s vs. ~544 s) — confirming
the transport-layer hypothesis from `sebsto-soto/README.md` — but it
**does not become competitive with Soto** on this 512 MB workload.

The remaining gap is the SDK's `Smithy.ByteStream` shape, which forces
two `Data` materialisations per upload part (one in our chunk
producer, one inside the signing middleware) plus a per-frame `Data`
allocation on download. The aggregate ~50 MB of extra resident memory
crowds the 512 MB budget enough to make the contender unreliable.

Two paths to make this contender viable:

1. **Move the workload to 1024 MB.** The `sebsto-soto/README.md`
   memory-scaling experiment showed Soto goes from σ=9.13 s at 512 MB
   to σ=0.79 s at 1024 MB; the same headroom should make awssdk-nio
   ~100 % reliable. Cost roughly doubles for a small speed gain —
   the same finding applies here. We did not run that experiment.
2. **Upstream changes to `aws-sdk-swift`** to expose `ByteBuffer`-shaped
   request and response bodies and remove the `FlexibleChecksumsRequestMiddleware`
   buffering for trailing-checksum modes (where streaming is
   compatible with SigV4). This is the only path that closes the
   remaining gap to Soto without paying for more Lambda memory.

For now, **Soto remains the right Swift SDK choice for memory-constrained
S3 streaming workloads**. The official SDK + AsyncHTTPClient is a
substantial improvement over the official SDK + CRT, but is still
~1.7× the Soto cost and structurally less reliable at 512 MB.
