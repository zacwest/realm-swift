// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let syncVersion = "5.0.23"
let baseUrl = "https://static.realm.io/downloads"

let package = Package(
    name: "RealmCocoa",
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "RealmCocoa",
            targets: ["RealmCocoa"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
    ],
    targets: [
        .systemLibrary(name: "zlib"),
        .binaryTarget(
            name: "RealmCore",
            url: "\(baseUrl)/sync/realm-sync-\(syncVersion).xcframework.zip",
            checksum: "5133eba05103cfb9277536986f6f0b67161a5ffdef86a679ac798924385aa30c"
        ),
        .target(
            name: "RealmCocoa",
            dependencies: ["RealmCore", "zlib"]
            //cxxSettings:
        ),
        .testTarget(
            name: "RealmCocoaTests",
            dependencies: ["RealmCocoa"]),
    ],
    cxxLanguageStandard: .cxx1z
)
