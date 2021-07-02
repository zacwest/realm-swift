import SwiftUI
import RealmSwift

@main
struct RealmExamplesApp: SwiftUI.App {
    var body: some Scene {
        Realm.Configuration.defaultConfiguration = .init(inMemoryIdentifier: UUID().uuidString)
        _ = CandlestickPublisher.shared
        return WindowGroup {
            ContentView()
        }
    }
}
