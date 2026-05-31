import AWSLambdaRuntime
import AWSS3
import Logging
import SmithyHTTPAPI

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One S3Client, instantiated once at cold start so warm invocations skip
// the connection setup. Region is read from AWS_REGION (set by Lambda).
//
// On Linux (Lambda runtime is amazonlinux2023 aarch64) aws-sdk-swift uses
// CRTClientEngine — connection pool default is maxConnections=50 per
// endpoint. We don't override it; that's already higher than the 32 we
// settled on for Soto.
let region: String = ProcessInfo.processInfo.environment["AWS_REGION"] ?? "us-east-1"

let s3 = try S3Client(
    config: S3Client.S3ClientConfig(region: region)
)

let runtime = LambdaRuntime { (event: JobInfo, context: LambdaContext) async throws -> String in
    context.logger.info("event: bucket=\(event.bucket_name) prefix=\(event.files_prefix) archive=\(event.archive_key)")
    try await runArchiveJob(s3: s3, job: event, logger: context.logger)
    return "ok"
}

try await runtime.run()
