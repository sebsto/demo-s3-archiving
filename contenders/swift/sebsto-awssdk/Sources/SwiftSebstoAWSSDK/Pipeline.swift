import AWSS3
import Logging
import Smithy

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Tunables — same shape as the Soto baseline (sebsto-soto branch).
//
// Inherited from sebsto-soto Run 16:
//   maxDownloadsMemory = 20 MiB  (32 MiB OOM'd warm runs there too)
//   maxConcurrentUploads = 3
//   chunkSize = 10 MiB
//   bufferChunksCount = 2
//
// Whether the same numbers are right for aws-sdk-swift is an empirical
// question — the underlying network stack is different (CRT vs NIO).
// We start from the Soto-tuned values and adjust if measurements warrant.
enum Tunables {
    static let maxDownloadsMemory: Int = 20 * 1024 * 1024   // 20 MiB
    static let maxConcurrentUploads: Int = 3
    static let chunkSize: Int = 10 * 1024 * 1024            // 10 MiB
    static let bufferChunksCount: Int = 2                   // ChunkProducer in-flight ceiling
}

struct FileInfo: Sendable {
    let name: String
    let key: String
    let size: Int
}

struct JobInfo: Codable, Sendable {
    let bucket_name: String
    let files_prefix: String
    let archive_key: String
}

// Counting semaphore: throttles total in-flight bytes for downloads. Async by
// design — `acquire` suspends when the budget is exhausted.
actor ByteSemaphore {
    private let capacity: Int
    private var available: Int
    private var waiters: [(needed: Int, cont: CheckedContinuation<Void, Never>)] = []

    init(capacity: Int) {
        self.capacity = capacity
        self.available = capacity
    }

    func acquire(_ amount: Int) async {
        let needed = min(amount, capacity)
        if available >= needed {
            available -= needed
            return
        }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            waiters.append((needed, cont))
        }
        available -= needed
    }

    func release(_ amount: Int) {
        let toRelease = min(amount, capacity)
        available += toRelease
        while let head = waiters.first, available >= head.needed {
            waiters.removeFirst()
            head.cont.resume()
        }
    }
}

// ----- Listing -----

func listFiles(s3: S3Client, bucket: String, filesPrefix: String, logger: Logger) async throws -> [FileInfo] {
    let prefix = filesPrefix + "/"
    var files: [FileInfo] = []
    var continuationToken: String? = nil
    repeat {
        let response = try await s3.listObjectsV2(
            input: ListObjectsV2Input(
                bucket: bucket,
                continuationToken: continuationToken,
                prefix: prefix
            )
        )
        for object in response.contents ?? [] {
            guard let key = object.key, let size = object.size, !key.hasSuffix("/") else { continue }
            guard key.hasPrefix(prefix) else { continue }
            let name = String(key.dropFirst(prefix.count))
            if name.isEmpty { continue }
            files.append(FileInfo(name: name, key: key, size: Int(size)))
        }
        continuationToken = response.nextContinuationToken
    } while continuationToken != nil
    return files
}

// ----- Multipart upload helpers -----

struct MultipartUpload: Sendable {
    let bucket: String
    let key: String
    let uploadId: String
}

func startMultipartUpload(s3: S3Client, bucket: String, key: String, logger: Logger) async throws -> MultipartUpload {
    let response = try await s3.createMultipartUpload(
        input: CreateMultipartUploadInput(
            bucket: bucket,
            contentType: "application/zip",
            key: key
        )
    )
    guard let uploadId = response.uploadId else {
        throw ArchivingError.missingUploadId
    }
    return MultipartUpload(bucket: bucket, key: key, uploadId: uploadId)
}

// uploadPart takes Data because that is what aws-sdk-swift's
// `ByteStream.data(_:)` requires. Bridging via `.stream(...)` was
// investigated and rejected: FlexibleChecksumsRequestMiddleware collects
// any provided Stream into Data anyway for checksum computation, so
// there is no copy savings.
func uploadPart(
    s3: S3Client,
    upload: MultipartUpload,
    partNumber: Int,
    data: Data,
    logger: Logger
) async throws -> S3ClientTypes.CompletedPart {
    let response = try await s3.uploadPart(
        input: UploadPartInput(
            body: ByteStream.data(data),
            bucket: upload.bucket,
            contentLength: data.count,
            key: upload.key,
            partNumber: partNumber,
            uploadId: upload.uploadId
        )
    )
    guard let etag = response.eTag else {
        throw ArchivingError.missingETag(partNumber: partNumber)
    }
    return S3ClientTypes.CompletedPart(eTag: etag, partNumber: partNumber)
}

func completeMultipartUpload(
    s3: S3Client,
    upload: MultipartUpload,
    parts: [S3ClientTypes.CompletedPart],
    logger: Logger
) async throws {
    let sorted = parts.sorted { ($0.partNumber ?? 0) < ($1.partNumber ?? 0) }
    _ = try await s3.completeMultipartUpload(
        input: CompleteMultipartUploadInput(
            bucket: upload.bucket,
            key: upload.key,
            multipartUpload: S3ClientTypes.CompletedMultipartUpload(parts: sorted),
            uploadId: upload.uploadId
        )
    )
}

func abortMultipartUpload(s3: S3Client, upload: MultipartUpload, logger: Logger) async {
    do {
        _ = try await s3.abortMultipartUpload(
            input: AbortMultipartUploadInput(
                bucket: upload.bucket,
                key: upload.key,
                uploadId: upload.uploadId
            )
        )
    } catch {
        logger.error("abort multipart upload failed: \(error)")
    }
}

// ----- Errors -----

enum ArchivingError: Error, CustomStringConvertible {
    case missingUploadId
    case missingETag(partNumber: Int)
    case downloadShortRead(key: String, expected: Int, got: Int)
    case missingResponseBody(key: String)

    var description: String {
        switch self {
        case .missingUploadId: return "S3 createMultipartUpload returned no uploadId"
        case .missingETag(let n): return "uploadPart \(n) returned no ETag"
        case .downloadShortRead(let k, let e, let g): return "short read on \(k): expected \(e), got \(g)"
        case .missingResponseBody(let k): return "getObject \(k) returned no body"
        }
    }
}
