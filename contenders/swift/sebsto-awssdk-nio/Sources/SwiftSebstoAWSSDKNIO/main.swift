import AWSLambdaRuntime
import AWSS3
import ClientRuntime           // HttpClientConfiguration
import Logging
import SmithySwiftNIO          // SwiftNIOHTTPClient

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One SwiftNIOHTTPClient + S3Client, instantiated once at cold start so warm
// invocations skip connection setup.
//
// This is the experiment: aws-sdk-swift's *default* HTTP engine is AWS-CRT,
// whose `S3.uploadPart` p50 ran ~3.4× slower than AsyncHTTPClient's on the
// 15 GB workload (CRT branch timed out warm at 600 s). Wiring `SwiftNIOHTTPClient`
// from `SmithySwiftNIO` swaps that transport for AsyncHTTPClient (the same
// stack Soto uses) without changing the request/response API surface — the
// SDK still hands us `Smithy.ByteStream`, so the body still flows as `Data`.
//
// `maxConnections: 32` and `socketTimeout: 120` mirror the Soto contender's
// AsyncHTTPClient settings (`concurrentHTTP1ConnectionsPerHostSoftLimit = 32`,
// `read = 120s`), keeping the comparison apples-to-apples.
let httpConfig = HttpClientConfiguration(
    socketTimeout: 120,
    maxConnections: 32
)
let nioClient = SwiftNIOHTTPClient(
    httpClientConfiguration: httpConfig,
    eventLoopGroup: nil
)

let region: String = ProcessInfo.processInfo.environment["AWS_REGION"] ?? "us-east-1"

let s3Config = try S3Client.S3ClientConfig(
    region: region,
    httpClientEngine: nioClient
)
let s3 = S3Client(config: s3Config)

let runtime = LambdaRuntime { (event: JobInfo, context: LambdaContext) async throws -> String in
    context.logger.info("event: bucket=\(event.bucket_name) prefix=\(event.files_prefix) archive=\(event.archive_key)")
    try await runArchiveJob(s3: s3, job: event, logger: context.logger)
    return "ok"
}

try await runtime.run()
