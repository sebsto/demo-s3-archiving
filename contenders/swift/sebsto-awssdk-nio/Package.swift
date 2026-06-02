// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "sebsto-awssdk-nio",
    platforms: [.macOS(.v15)],
    products: [
        .executable(name: "bootstrap", targets: ["SwiftSebstoAWSSDKNIO"])
    ],
    dependencies: [
        .package(url: "https://github.com/awslabs/swift-aws-lambda-runtime.git", from: "2.0.0"),
        .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "1.7.0"),
        // SmithySwiftNIO is not re-exported by aws-sdk-swift, so we need an
        // explicit smithy-swift dependency. Pin the lower bound to the
        // version aws-sdk-swift 1.7.x declares (clientRuntimeVersion = 0.214.0).
        .package(url: "https://github.com/smithy-lang/smithy-swift.git", from: "0.214.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.77.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.0"),
    ],
    targets: [
        .executableTarget(
            name: "SwiftSebstoAWSSDKNIO",
            dependencies: [
                .product(name: "AWSLambdaRuntime", package: "swift-aws-lambda-runtime"),
                .product(name: "AWSS3", package: "aws-sdk-swift"),
                // The NIO HTTP engine for the official SDK (see
                // https://github.com/awslabs/aws-sdk-swift/discussions/2070).
                .product(name: "SmithySwiftNIO", package: "smithy-swift"),
                // Smithy.ByteStream is the request/response body type the
                // SDK exposes for S3 GET/PUT bodies.
                .product(name: "Smithy", package: "smithy-swift"),
                // ClientRuntime hosts HttpClientConfiguration, which is the
                // configuration object passed to SwiftNIOHTTPClient.
                .product(name: "ClientRuntime", package: "smithy-swift"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [
                .enableExperimentalFeature("StrictConcurrency"),
                .unsafeFlags(["-O"], .when(configuration: .release)),
            ]
        ),
    ]
)
