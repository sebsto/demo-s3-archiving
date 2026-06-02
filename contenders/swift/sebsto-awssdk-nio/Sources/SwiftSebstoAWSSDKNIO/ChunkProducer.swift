#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Async byte sink that buckets writes into fixed-size chunks and emits
// them on a `nonisolated AsyncStream<UploadChunk>` for the uploader.
// Decouples the synchronous ZIP writer from the asynchronous multipart
// uploader, with backpressure when the uploader can't keep up.
//
// Memory model: at most `maxInFlight` chunks of `chunkSize` bytes are
// outstanding (built but not yet uploaded). With `chunkSize` = 10 MiB
// and `maxInFlight` = 2 that's a 20 MiB ceiling for the producer→
// uploader path.
//
// Storage is `Foundation.Data` end-to-end. Unlike the Soto variant —
// which keeps `ByteBuffer` everywhere and feeds `S3.uploadPart` via
// `AWSHTTPBody(buffer:)` zero-copy — the official aws-sdk-swift only
// surfaces `Smithy.ByteStream.data(Data?)` for the upload body, and its
// `FlexibleChecksumsRequestMiddleware` collapses any custom `.stream(...)`
// into `Data` before signing. So the producer materialises bytes as
// `Data` directly: no Data→ByteBuffer detour, but also no benefit from
// NIO's slab allocator on the chunk buffer itself.
actor ChunkProducer {
    let chunkSize: Int
    private let maxInFlight: Int

    private var buffer: Data
    private var nextPartNumber: Int = 1
    private var inFlight: Int = 0
    private var slotWaiters: [CheckedContinuation<Void, Never>] = []
    private var closed = false

    private let continuation: AsyncStream<UploadChunk>.Continuation
    nonisolated let stream: AsyncStream<UploadChunk>

    struct UploadChunk: Sendable {
        let partNumber: Int
        let data: Data
    }

    init(chunkSize: Int = 10 * 1024 * 1024, maxInFlight: Int = 2) {
        self.chunkSize = chunkSize
        self.maxInFlight = maxInFlight
        var b = Data()
        b.reserveCapacity(chunkSize)
        self.buffer = b
        var continuationOut: AsyncStream<UploadChunk>.Continuation!
        self.stream = AsyncStream { c in continuationOut = c }
        self.continuation = continuationOut
    }

    /// Append a `Data` blob. The fast path (input fits in the current
    /// chunk) is a single `Data.append`. The spill path (input crosses
    /// one or more chunk boundaries) emits the current chunk and copies
    /// the remainder in chunk-sized passes.
    func append(_ bytes: Data) async {
        let total = bytes.count
        guard total > 0 else { return }
        var cursor = 0
        while cursor < total {
            let stillRoom = chunkSize - buffer.count
            let take = Swift.min(total - cursor, stillRoom)
            // `bytes.subdata(in:)` would allocate; range-subscript views
            // share the same storage and `Data.append(_:)` then memcpy's
            // `take` bytes once into `buffer`'s tail.
            let lo = bytes.index(bytes.startIndex, offsetBy: cursor)
            let hi = bytes.index(lo, offsetBy: take)
            buffer.append(bytes[lo..<hi])
            cursor += take
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
        }
    }

    /// Append the three byte ranges that make up one ZIP entry —
    /// local file header, body, then data descriptor — in order.
    func appendCompound(lfh: Data, body: Data, dataDescriptor: Data) async {
        await append(lfh)
        await append(body)
        await append(dataDescriptor)
    }

    /// Mark the producer as done. Emits any remaining partial chunk and
    /// closes the stream so the consumer's `for await` loop terminates.
    func finish() async {
        if buffer.count > 0 {
            await emitFullChunk()
        }
        closed = true
        continuation.finish()
    }

    /// Called by the uploader when it has finished sending a chunk so the
    /// producer can build the next ones.
    func releaseSlot() {
        if inFlight > 0 { inFlight -= 1 }
        if let w = slotWaiters.first {
            slotWaiters.removeFirst()
            w.resume()
        }
    }

    private func emitFullChunk() async {
        await waitForSlot()
        let chunk = UploadChunk(partNumber: nextPartNumber, data: buffer)
        nextPartNumber += 1
        inFlight += 1
        var fresh = Data()
        fresh.reserveCapacity(chunkSize)
        buffer = fresh
        continuation.yield(chunk)
    }

    private func waitForSlot() async {
        if inFlight < maxInFlight { return }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            slotWaiters.append(cont)
        }
    }
}
