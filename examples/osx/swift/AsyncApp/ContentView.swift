import SwiftUI
import RealmSwift
import Combine

@objcMembers class Candlestick: Object, ObjectKeyIdentifiable {
    dynamic var id: Int = 0
    dynamic var open: Double = 0.0
    dynamic var close: Double = 0.0
    dynamic var high: Double = 0.0
    dynamic var low: Double = 0.0
}



actor CandlestickPublisher {
    typealias Output = Candlestick
    typealias Failure = Never

    @RealmTaskLocal(Candlestick.self) static var candlesticks
    @RealmTaskLocal static var currentCandle: Candlestick?

    var subscribers: [AnySubscriber<Output, Failure>] = []

    static let shared = CandlestickPublisher()

    private init() {
        let realm = try! Realm()
        try! realm.write {
            let candlestick = Candlestick()
            candlestick.high = 100
            candlestick.low = 100
            candlestick.open = 100
            candlestick.close = 100
            realm.add(candlestick)
        }

        Timer.scheduledTimer(withTimeInterval: 0.1, repeats: true) { _ in
            detach {
                try await self.tickDecisecond()
            }
        }

        Timer.scheduledTimer(withTimeInterval: 60.0,
                             repeats: true) { _ in
            detach {
                let realm = try Realm()
                let candlestick = try await self.generateCandlestick()
                try realm.write {
                    realm.add(candlestick)
                }
            }
        }
    }

    func tickDecisecond() async throws {
        try await CandlestickPublisher.$currentCandle.withValue(CandlestickPublisher.candlesticks!.last!) {
            let (value, high, low) = await self.generateNewCandleValue()
            try CandlestickPublisher.currentCandle!.realm!.write {
                CandlestickPublisher.currentCandle!.close = value
                CandlestickPublisher.currentCandle!.high = high
                CandlestickPublisher.currentCandle!.low = low
            }
        }
    }

    func generateNewCandleValue() async -> (value: Double, high: Double, low: Double) {
        let newValue = Double.random(in: CandlestickPublisher.currentCandle!.low - 5 ..< CandlestickPublisher.currentCandle!.high + 5)
        return (newValue, Swift.max(CandlestickPublisher.currentCandle!.high, newValue), Swift.min(CandlestickPublisher.currentCandle!.low, newValue))
    }

    func generateCandlestick() async -> Candlestick {
        let candlestick = Candlestick()
        candlestick.id = CandlestickPublisher.currentCandle!.id + 1
        candlestick.open = CandlestickPublisher.currentCandle!.close
        candlestick.close = CandlestickPublisher.currentCandle!.close
        candlestick.high = CandlestickPublisher.currentCandle!.close
        candlestick.low = CandlestickPublisher.currentCandle!.close
        return candlestick
    }

//    func receive<S>(subscriber: S) where S : Subscriber, Never == S.Failure, Candlestick == S.Input {
//        subscribers.append( AnySubscriber(subscriber))
//    }
}

struct ContentView: View {
    let maxCandles = 50
    @ObservedResults(Candlestick.self) var candlesticks

    var body: some View {
        GeometryReader { proxy in
            HStack {
                ForEach(candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - 50)) { candlestick in
                    Path { path in
                        path.addRect(CGRect(origin: CGPoint(x: 0,y: 0), size: CGSize(width: proxy.size.width / 50, height: 10)))
                        path.addRect(CGRect.init(x: proxy.size.width / 50 / 2 - 1, y: -10, width: 2, height: 10))
                    }
                    .fill(Color.red)
                    .padding()
                }
            }
        }.background(Color.gray)
    }
}

struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        ContentView()
    }
}
