import SwiftUI
import RealmSwift

@main
struct RealmExamplesApp: SwiftUI.App {
    var body: some Scene {
        // Generate a random encryption key
        var key = Data(count: 64)
        _ = key.withUnsafeMutableBytes { (pointer: UnsafeMutableRawBufferPointer) in
            SecRandomCopyBytes(kSecRandomDefault, 64, pointer.baseAddress!) }

        Realm.Configuration.defaultConfiguration = .init(inMemoryIdentifier: UUID().uuidString, encryptionKey: key)
        _ = CandlestickPublisher.shared
        return WindowGroup {
            ContentView()
        }
    }
}
