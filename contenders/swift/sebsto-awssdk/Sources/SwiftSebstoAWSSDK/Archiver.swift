import AWSS3
import Logging
import Smithy

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One downloaded file's bytes plus the byte-budget release amount.
//
// Carries `Data` because aws-sdk-swift's `ByteStream` API surface is
// `Data`-shaped throughout (see PERF-PLAN notes for the AWSSDK branch:
// `readAsync(upToCount:)` returns `Data?`, `UploadPartInput.body` only
// accepts `ByteStream.data(Data?)` zero-copy; `ByteStream.stream(...)`
// is collected into Data by FlexibleChecksumsRequestMiddleware anyway).
//
// CRC32 is computed in the downloader, in parallel across N tasks,
// rather than in the single-threaded zipper.
struct DownloadedFile: Sendable {
    let name: String
    let data: Data
    let crc32: UInt32
    let releaseBytes: Int
}

// Single-producer / single-consumer async channel.
final class FileChannel: @unchecked Sendable {
    let stream: AsyncStream<DownloadedFile>
    private let continuation: AsyncStream<DownloadedFile>.Continuation

    init() {
        var c: AsyncStream<DownloadedFile>.Continuation!
        self.stream = AsyncStream { c = $0 }
        self.continuation = c
    }
    func send(_ item: DownloadedFile) { continuation.yield(item) }
    func finish() { continuation.finish() }
}

func runArchiveJob(s3: S3Client, job: JobInfo, logger: Logger) async throws {
    let files = try await listFiles(
        s3: s3,
        bucket: job.bucket_name,
        filesPrefix: job.files_prefix,
        logger: logger
    )
    logger.info("archive: \(files.count) source objects")

    let upload = try await startMultipartUpload(
        s3: s3,
        bucket: job.bucket_name,
        key: job.archive_key,
        logger: logger
    )
    logger.info("archive: multipart upload started (id=\(upload.uploadId))")

    let totalBytes = files.reduce(0) { $0 + $1.size }
    let estimatedParts = max(1, totalBytes / Tunables.chunkSize + 8)
    let stats = Stats(estimatedFiles: files.count, estimatedParts: estimatedParts)
    do {
        let parts = try await runPipeline(
            s3: s3,
            bucket: job.bucket_name,
            files: files,
            upload: upload,
            stats: stats,
            logger: logger
        )
        try await completeMultipartUpload(s3: s3, upload: upload, parts: parts, logger: logger)
        logger.info("archive: completed (\(parts.count) parts)")
        stats.report(logger: logger)
    } catch {
        logger.error("archive: failed, aborting multipart upload: \(error)")
        await abortMultipartUpload(s3: s3, upload: upload, logger: logger)
        throw error
    }
}

private func runPipeline(
    s3: S3Client,
    bucket: String,
    files: [FileInfo],
    upload: MultipartUpload,
    stats: Stats,
    logger: Logger
) async throws -> [S3ClientTypes.CompletedPart] {
    let producer = ChunkProducer(
        chunkSize: Tunables.chunkSize,
        maxInFlight: Tunables.bufferChunksCount
    )
    let byteBudget = ByteSemaphore(capacity: Tunables.maxDownloadsMemory)
    let fileChannel = FileChannel()

    async let downloadDone: Void = runDownloadStage(
        s3: s3,
        bucket: bucket,
        files: files,
        byteBudget: byteBudget,
        out: fileChannel,
        stats: stats,
        logger: logger
    )
    async let zipDone: Void = runZipStage(
        files: files,
        in: fileChannel,
        producer: producer,
        byteBudget: byteBudget,
        stats: stats,
        logger: logger
    )
    async let uploadResult: [S3ClientTypes.CompletedPart] = runUploadStage(
        s3: s3,
        producer: producer,
        upload: upload,
        stats: stats,
        logger: logger
    )

    try await downloadDone
    try await zipDone
    return try await uploadResult
}

// ----- Stage A: downloader -----

private func runDownloadStage(
    s3: S3Client,
    bucket: String,
    files: [FileInfo],
    byteBudget: ByteSemaphore,
    out: FileChannel,
    stats: Stats,
    logger: Logger
) async throws {
    try await withThrowingTaskGroup(of: Void.self) { group in
        for file in files {
            await byteBudget.acquire(file.size)
            group.addTask {
                stats.incrementInFlight()
                let t0: UInt64 = Stats.enabled ? monoNs() : 0
                let (data, crc) = try await downloadFile(
                    s3: s3, bucket: bucket, key: file.key,
                    expectedSize: file.size, stats: stats, logger: logger
                )
                if Stats.enabled { stats.record(.downloadFile, ns: monoNs() - t0) }
                stats.decrementInFlight()
                out.send(DownloadedFile(name: file.name, data: data, crc32: crc, releaseBytes: file.size))
            }
        }
        try await group.waitForAll()
        out.finish()
    }
}

// Streams the body into a pre-allocated `Data` of exactly `expectedSize`,
// then runs CRC32 once at end-of-file.
//
// aws-sdk-swift response.body is `Smithy.ByteStream?`, an enum:
//   - `.data(Data?)` — small/buffered bodies
//   - `.stream(Stream)` — what S3 returns for non-trivial bodies. Read
//     via `readAsync(upToCount:)` which returns a fresh `Data` per call.
//   - `.noStream`
//
// Pre-allocating a single `Data(capacity: expectedSize)` and copying
// each chunk in via `data.append(chunk)` avoids growth-doubling on the
// destination, but each `readAsync` still allocates a fresh chunk Data
// (CRT-side allocation we can't avoid). This is the C2.5 hybrid pattern
// adapted to the aws-sdk-swift API.
private func downloadFile(
    s3: S3Client,
    bucket: String,
    key: String,
    expectedSize: Int,
    stats: Stats,
    logger: Logger
) async throws -> (Data, UInt32) {
    let response = try await s3.getObject(
        input: GetObjectInput(bucket: bucket, key: key)
    )
    guard let body = response.body else {
        throw ArchivingError.missingResponseBody(key: key)
    }

    var out = Data(capacity: expectedSize)
    switch body {
    case .data(let d):
        if let d { out.append(d) }
    case .stream(let stream):
        // Read in 64 KiB chunks (matches Smithy's CHUNK_SIZE_BYTES default).
        // Loop until readAsync returns nil (end-of-stream).
        while let chunk = try await stream.readAsync(upToCount: 64 * 1024) {
            if chunk.isEmpty { break }
            out.append(chunk)
        }
    case .noStream:
        break
    @unknown default:
        break
    }

    if out.count != expectedSize {
        throw ArchivingError.downloadShortRead(key: key, expected: expectedSize, got: out.count)
    }

    var crc = CRC32()
    if Stats.enabled {
        let t0 = monoNs()
        out.withUnsafeBytes { raw in
            if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                crc.update(UnsafeBufferPointer(start: base, count: out.count))
            }
        }
        stats.record(.downloadInFrame, ns: monoNs() - t0)
    } else {
        out.withUnsafeBytes { raw in
            if let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) {
                crc.update(UnsafeBufferPointer(start: base, count: out.count))
            }
        }
    }
    return (out, crc.value)
}

// ----- Stage B: zipper -----

private func runZipStage(
    files: [FileInfo],
    in fileChannel: FileChannel,
    producer: ChunkProducer,
    byteBudget: ByteSemaphore,
    stats: Stats,
    logger: Logger
) async throws {
    var entries: [ZipEntry] = []
    entries.reserveCapacity(files.count)
    var offset: UInt64 = 0
    var processed = 0

    var queueWaitStart: UInt64 = Stats.enabled ? monoNs() : 0
    for await file in fileChannel.stream {
        if Stats.enabled { stats.record(.zipperQueueWait, ns: monoNs() - queueWaitStart) }

        let bodySize = UInt64(file.data.count)
        let lfh = ZipHeaders.localFileHeader(name: file.name)
        let lfhOffset = offset
        let dd = ZipHeaders.dataDescriptor(crc32: file.crc32, size: bodySize)

        let appendStart: UInt64 = Stats.enabled ? monoNs() : 0
        await producer.appendCompound(lfh: lfh, body: file.data, dataDescriptor: dd)
        if Stats.enabled {
            stats.record(.zipperAppend, ns: monoNs() - appendStart)
            stats.bumpProducerHops(3)
        }

        offset += UInt64(lfh.count) + bodySize + UInt64(dd.count)
        await byteBudget.release(file.releaseBytes)

        entries.append(ZipEntry(
            name: file.name,
            crc32: file.crc32,
            size: bodySize,
            localHeaderOffset: lfhOffset
        ))
        processed += 1
        if processed % 200 == 0 {
            logger.info("zip: \(processed)/\(files.count) entries")
        }
        if Stats.enabled { queueWaitStart = monoNs() }
    }

    // Central directory + ZIP64 EOCD + locator + EOCD.
    let cdOffset = offset
    var cd = Data()
    cd.reserveCapacity(entries.count * 80)
    for entry in entries {
        cd.append(ZipHeaders.centralDirectoryHeader(entry))
    }
    await producer.append(cd)
    let cdSize = UInt64(cd.count)
    offset += cdSize

    let zip64Eocd = ZipHeaders.zip64EndOfCentralDirectory(
        entryCount: UInt64(entries.count),
        cdSize: cdSize,
        cdOffset: cdOffset
    )
    let zip64EocdOffset = offset
    await producer.append(zip64Eocd)
    offset += UInt64(zip64Eocd.count)

    await producer.append(ZipHeaders.zip64EndOfCentralDirectoryLocator(zip64EocdOffset: zip64EocdOffset))
    await producer.append(ZipHeaders.endOfCentralDirectory())

    await producer.finish()
}

// ----- Stage C: uploader -----

private func runUploadStage(
    s3: S3Client,
    producer: ChunkProducer,
    upload: MultipartUpload,
    stats: Stats,
    logger: Logger
) async throws -> [S3ClientTypes.CompletedPart] {
    var completed: [S3ClientTypes.CompletedPart] = []
    try await withThrowingTaskGroup(of: S3ClientTypes.CompletedPart.self) { group in
        var inFlight = 0
        var queueWaitStart: UInt64 = Stats.enabled ? monoNs() : 0
        for await chunk in producer.stream {
            if Stats.enabled { stats.record(.uploaderQueueWait, ns: monoNs() - queueWaitStart) }

            if inFlight >= Tunables.maxConcurrentUploads {
                if let p = try await group.next() {
                    completed.append(p)
                    inFlight -= 1
                }
            }
            group.addTask {
                let t0: UInt64 = Stats.enabled ? monoNs() : 0
                let cp = try await uploadPart(
                    s3: s3,
                    upload: upload,
                    partNumber: chunk.partNumber,
                    data: chunk.data,
                    logger: logger
                )
                if Stats.enabled { stats.record(.uploadPart, ns: monoNs() - t0) }
                await producer.releaseSlot()
                return cp
            }
            inFlight += 1
            if Stats.enabled { queueWaitStart = monoNs() }
        }
        while let p = try await group.next() {
            completed.append(p)
        }
    }
    return completed
}
