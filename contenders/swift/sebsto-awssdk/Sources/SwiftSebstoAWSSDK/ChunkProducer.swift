#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Async byte sink that buckets writes into fixed-size chunks and emits them
// for upload. Decouples the synchronous ZIP writer from the asynchronous
// multipart uploader, with backpressure when the uploader lags.
//
// Memory model: at most `maxInFlight` chunks of `chunkSize` bytes are
// outstanding (= built but not yet uploaded). With chunkSize=10 MiB and
// maxInFlight=2 that's a 20 MiB ceiling for the producer→uploader path.
//
// Storage: `Data` end-to-end. aws-sdk-swift's `UploadPartInput.body`
// is `ByteStream.data(Data?)` zero-copy; the alternative
// `ByteStream.stream(Stream)` is collected into Data anyway by
// FlexibleChecksumsRequestMiddleware (verified by reading the SDK
// source). So `Data` here matches what the SDK consumes.
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
        var buf = Data()
        buf.reserveCapacity(chunkSize)
        self.buffer = buf
        var continuationOut: AsyncStream<UploadChunk>.Continuation!
        self.stream = AsyncStream { c in continuationOut = c }
        self.continuation = continuationOut
    }

    // Append a small Data (used for ZIP headers — LFH, DD, central directory
    // records, EOCDs) or a large file body Data.
    //
    // Hot path: total fits in current chunk. Single `Data.append(other:)`
    // is a memmove on a reserveCapacity'd Data. Spill path: split via
    // subdata views (no allocation; views share storage).
    func append(_ bytes: Data) async {
        let total = bytes.count
        guard total > 0 else { return }
        let room = chunkSize - buffer.count
        if total <= room {
            buffer.append(bytes)
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
            return
        }
        var cursor = 0
        while cursor < total {
            let stillRoom = chunkSize - buffer.count
            let take = Swift.min(total - cursor, stillRoom)
            // Slice via Data subscript — produces a view without copying.
            let start = bytes.startIndex.advanced(by: cursor)
            let end = bytes.startIndex.advanced(by: cursor + take)
            buffer.append(bytes[start..<end])
            cursor += take
            if buffer.count == chunkSize {
                await emitFullChunk()
            }
        }
    }

    // Coalesced LFH + body + data descriptor. Three actor hops/file.
    func appendCompound(lfh: Data, body: Data, dataDescriptor: Data) async {
        await append(lfh)
        await append(body)
        await append(dataDescriptor)
    }

    func finish() async {
        if !buffer.isEmpty {
            await emitFullChunk()
        }
        closed = true
        continuation.finish()
    }

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
        var newBuf = Data()
        newBuf.reserveCapacity(chunkSize)
        buffer = newBuf
        continuation.yield(chunk)
    }

    private func waitForSlot() async {
        if inFlight < maxInFlight { return }
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            slotWaiters.append(cont)
        }
    }
}
