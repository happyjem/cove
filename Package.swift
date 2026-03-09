// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "Morfeo",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio", from: "1.21.0"),
    ],
    targets: [
        .executableTarget(
            name: "Morfeo",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ],
            path: "Morfeo",
            swiftSettings: [
                .swiftLanguageMode(.v6),
            ]
        ),
    ]
)
