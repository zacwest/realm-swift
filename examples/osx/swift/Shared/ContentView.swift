import Foundation
import SwiftUI
import RealmSwift
import Combine

@objcMembers class Candlestick: Object, ObjectKeyIdentifiable {
    dynamic var id: TimeInterval = 0.0
    dynamic var open: Double = 0.0
    dynamic var close: Double = 0.0
    dynamic var high: Double = 0.0
    dynamic var low: Double = 0.0
    dynamic var exponentialMovingAverage12: Double = .nan
    dynamic var exponentialMovingAverage26: Double = .nan
    dynamic var MACDEMA9: Double = .nan

    static let EMA9multiplier = 2.0/(9 + 1)
    static let EMA12multiplier = 2.0/(12 + 1)
    static let EMA26multiplier = 2.0/(26 + 1)

    public var MACD: Double {
        exponentialMovingAverage12 - exponentialMovingAverage26
    }
}

@available(macOS 12.0, *)
class CandlestickSubject: Subscriber {
    typealias Input = RealmCollectionChange<Results<Candlestick>>
    typealias Failure = Never
    private var subscription: Subscription?


    func receive(subscription: Subscription) {
        self.subscription = subscription
    }

    func receive(_ input: RealmCollectionChange<Results<Candlestick>>) -> Subscribers.Demand {
        switch input {
        case .initial(_): break
        case .update(let results, _, insertions: let insertions, _):
            insertions.forEach { idx in
                // prepare to cook the previous candle
                let tsr = ThreadSafeReference(to: results[idx - 1])
                async {
                    try await Self.$candles.withResults {
                        try await CandlestickSubject.cookCandle()
                    }
                }
            }
        case .error(_): break
        }
        return .unlimited
    }

    func receive(completion: Subscribers.Completion<Never>) {
    }

    @RealmTaskLocal static var realm: Realm?
    @RealmTaskLocal static var rawCandle: Candlestick?
    @RealmTaskLocal static var rawCandle2: Candlestick?
    @RealmTaskLocal static var candles: Results<Candlestick>?

    class func cookCandle() async throws {
        try await $realm.write(($rawCandle, candles!.last!),
                               ($rawCandle2, candles!.last!)) {
            try await calculateEMA12()
            try await calculateEMA26()
            try await calculateMACDEMA9()
        }
    }

    class func calculateEMA12() async throws {
        print(MemoryLayout<String>.size)
        guard let candles = candles, candles.count >= 12,
            let candle = rawCandle else {
            return
        }
        assert(candle.realm?.isInWriteTransaction == true)
        let idx = candles.index(of: candle) ?? candles.endIndex
        // EMA = Closing price x multiplier + EMA (previous day) x (1-multiplier)
        if candles[candles.index(before: idx)].exponentialMovingAverage12.isNaN == false {
            let EMA = candles[candles.index(before: idx)].exponentialMovingAverage12
            candle.exponentialMovingAverage12 = (candle.close * Candlestick.EMA12multiplier) + (EMA * (1 - Candlestick.EMA12multiplier))
        } else {
            candle.exponentialMovingAverage12 = (candles + [candle]).reduce(into: candles.first!.close) { EMA, candle in
                EMA = (candle.close * Candlestick.EMA12multiplier) + (EMA * (1 - Candlestick.EMA12multiplier))
            }
        }
    }
    class func calculateMACDEMA9() async throws {
        guard let candles = candles, candles.count >= 37,
              let candle = rawCandle
        else {
            return
        }
        assert(candle.realm?.isInWriteTransaction == true)
        let idx = candles.index(of: candle) ?? candles.endIndex
        let last9Candles = (candles[idx - 8 ..< idx] + [candle])
        candle.MACDEMA9 = last9Candles.reduce(into: last9Candles.first!.MACD) { EMA, candle in
            EMA = (candle.MACD * Candlestick.EMA9multiplier) + (EMA * (1 - Candlestick.EMA9multiplier))
        }
    }
    class func calculateEMA26() async throws {
        guard let candles = candles, candles.count >= 26,
              let candle = rawCandle
        else {
            return
        }
        assert(candle.realm?.isInWriteTransaction == true)
        let idx = candles.index(of: candle) ?? candles.endIndex
        // EMA = Closing price x multiplier + EMA (previous day) x (1-multiplier)
        if candles[candles.index(before: idx)].exponentialMovingAverage26.isNaN == false {
            let EMA = candles[candles.index(before: idx)].exponentialMovingAverage26
            candle.exponentialMovingAverage26 = (candle.close * Candlestick.EMA26multiplier) + (EMA * (1 - Candlestick.EMA26multiplier))
        } else {
            candle.exponentialMovingAverage26 = (candles + [candle]).reduce(into: candles.first!.close) { EMA, candle in
                EMA = (candle.close * Candlestick.EMA26multiplier) + (EMA * (1 - Candlestick.EMA26multiplier))
            }
        }
    }
}

@available(macOS 12.0, *)
actor CandlestickPublisher {
    typealias Output = Candlestick
    typealias Failure = Never

    @RealmTaskLocal static var candlesticks: Results<Candlestick>?
    @RealmTaskLocal static var currentCandle: Candlestick?

    var subscribers: [AnySubscriber<Output, Failure>] = []

    static let shared = CandlestickPublisher()
    private let candlestickQueue = DispatchQueue(label: "candlestick-listener")

    init() {
        // seed one candle
        let realm = try! Realm()
        try! realm.write {
            let candlestick = Candlestick()
            candlestick.high = 100
            candlestick.low = 100
            candlestick.open = 100
            candlestick.close = 100
            realm.add(candlestick)
        }
    }

    @RealmTaskLocal static var realm: Realm?
    func seed(amount: Int = 1) async throws {
        for _ in 0..<amount - 1 {
            for _ in 0..<Int(5.0/0.1) {
                try await Self.tickDecisecond()
            }

//            try await CandlestickSubject.$realm.withValue(try Realm()) {
//
//            }
//            try await CandlestickPublisher.$candlesticks.withResults {
            try await Self.$candlesticks.withResults {
                try await CandlestickSubject.cookCandle()
            }
//            }
//            try await CandlestickPublisher.$candlesticks.withResults {
//                try await CandlestickPublisher.$currentCandle.withValue(CandlestickPublisher.candlesticks!.last!) {
//                    let realm = try Realm()
            try await Self.$realm.write {
                let candlestick = await self.generateCandlestick()
//                    try realm.write {
                Self.realm?.add(candlestick)
//                    }
            }
//                }
//            }
        }
    }

    var isInitialized = false
    fileprivate func initialize() async throws {
        guard !isInitialized else { return }
        try await seed(amount: 100)

        DispatchQueue.main.async {
            let realm = try! Realm()
            realm.objects(Candlestick.self).changesetPublisher
                .receive(on: self.candlestickQueue)
                .receive(subscriber: CandlestickSubject())

            Timer.scheduledTimer(withTimeInterval: 0.1, repeats: true) { _ in
                async {
                    try await Self.$candlesticks.withResults {
                        try await Self.tickDecisecond()
                    }
                }
            }

            Timer.scheduledTimer(withTimeInterval: 5,
                                 repeats: true) { _ in
                detach {
                    try await Self.$candlesticks.withResults {
                        let realm = try Realm()
                        let candlestick = await self.generateCandlestick()
                        try realm.write {
                            realm.add(candlestick)
                        }
                    }
                }
            }
        }
        isInitialized = true
    }

    class func tickDecisecond() async throws {
        guard let lastCandle = candlesticks?.last else {
            fatalError("No candles")
        }
//            try await $currentCandle.withValue(candlesticks!.last!) {
        let (value, high, low) = await Self.generateNewCandleValue(for: lastCandle)
        try await $realm.write(($currentCandle, lastCandle)) {
            currentCandle!.close = value
            currentCandle!.high = high
            currentCandle!.low = low
        }
//            }
    }

    class func generateNewCandleValue(for candle: Candlestick) async -> (value: Double, high: Double, low: Double) {
        let newValue = max(Double.random(in: candle.low - candle.low * 0.005 ..< candle.high + candle.high * 0.005), 0)
        return (newValue, Swift.max(candle.high, newValue), Swift.min(candle.low, newValue))
    }

    func generateCandlestick() -> Candlestick {
        guard let lastCandle = Self.candlesticks?.last else {
            fatalError("No candlesticks")
        }
        let candlestick = Candlestick()
        candlestick.id = lastCandle.id + 1
        candlestick.open = lastCandle.close
        candlestick.close = lastCandle.close
        candlestick.high = lastCandle.close
        candlestick.low = lastCandle.close
        return candlestick
    }
}

struct CandlestickShape: Shape {
    let candlestick: Candlestick
    let minY, maxY: Double

    func path(in proxy: CGRect) -> Path {
        var path = Path()
        let baseWidth = proxy.width
        let x1 = (baseWidth * 0.25)
        let x2 = (baseWidth * 0.5)
        let normalizedY1 = (candlestick.open - minY) / (maxY - minY)
        let y1 = (1 - normalizedY1) * proxy.maxY
        let normalizedY2 = (candlestick.close - minY) / (maxY - minY)
        let y2 = (1 - normalizedY2) * proxy.maxY
        let candleRect = CGRect(x: x1, y: y1, width: x2, height: (y2 - y1))
        path.addRoundedRect(in: candleRect, cornerSize: CGSize(width: 1, height: 1))
        let normalizedY3 = (candlestick.high - minY) / (maxY - minY)
        let y3 = (1 - normalizedY3) * proxy.maxY
        let height = -1 * (y3 - Swift.min(y1, y2))
        let highStick = CGRect.init(x: proxy.midX - 1,
                                    y: y3,
                                    width: 2,
                                    height: height)

        let normalizedY4 = (candlestick.low - minY) / (maxY - minY)
        let y4 = (1 - normalizedY4) * proxy.maxY
        let lowStick = CGRect.init(x: proxy.midX - 1,
                                   y: y4,
                                   width: 2,
                                   height: -1 * (y4 - Swift.max(y1, y2)))
        path.addRects([highStick, lowStick])
        return path
    }
}

// MARK: CandlestickView
struct CandlestickView: View {
    @ObservedResults(Candlestick.self) var candlesticks
    var interval: TimeInterval
    var minY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .min(ofProperty: "low")!
    }
    var maxY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .max(ofProperty: "high")!
    }
    var body: some View {
        HStack {
            ForEach(candlesticks
                        .filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
                        .sorted(byKeyPath: "id", ascending: true)) { candlestick in
                CandlestickShape(candlestick: candlestick, minY: minY, maxY: maxY)
                    .fill(candlestick.close > candlestick.open ? .green : .red,
                          strokeBorder: .white,
                          lineWidth: 1)
            }.animation(.spring(), value: candlesticks)
        }
    }
}
final class AnimatableCubicCurve : VectorArithmetic, Hashable, Identifiable {
    static func == (lhs: AnimatableCubicCurve, rhs: AnimatableCubicCurve) -> Bool {
        lhs.hashValue == rhs.hashValue
    }

    var start: CGPoint
    var end: CGPoint
    var control1: CGPoint
    var control2: CGPoint
    init(start: CGPoint, end: CGPoint, control1: CGPoint, control2: CGPoint) {
        self.start = start
        self.end = end; self.control1 = control1; self.control2 = control2;
    }
    var length: Double {
        return Double(((end.x - start.x) * (end.x - start.x)) +
                        ((end.y - start.y) * (end.y - start.y))).squareRoot()
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(start.x)
        hasher.combine(start.y)
        hasher.combine(end.x)
        hasher.combine(end.y)
        hasher.combine(control1.x)
        hasher.combine(control1.y)
        hasher.combine(control2.x)
        hasher.combine(control2.y)
    }
    var magnitudeSquared: Double {
        return length * length
    }

    func scale(by rhs: Double) {
        self.start.x.scale(by: rhs)
        self.start.y.scale(by: rhs)
        self.end.x.scale(by: rhs)
        self.end.y.scale(by: rhs)
        self.control1.x.scale(by: rhs)
        self.control1.y.scale(by: rhs)
        self.control2.x.scale(by: rhs)
        self.control2.y.scale(by: rhs)
    }

    static var zero: AnimatableCubicCurve {
        return AnimatableCubicCurve(start: CGPoint(x: 0.0, y: 0.0),
                                    end: CGPoint(x: 0.0, y: 0.0),
                                    control1: CGPoint(x: 0.0, y: 0.0),
                                    control2: CGPoint(x: 0.0, y: 0.0))
    }

    static func - (lhs: AnimatableCubicCurve, rhs: AnimatableCubicCurve) -> AnimatableCubicCurve {
        return AnimatableCubicCurve(
            start: CGPoint(
                x: lhs.start.x - rhs.start.x,
                y: lhs.start.y - rhs.start.y),
            end: CGPoint(
                x: lhs.end.x - rhs.end.x,
                y: lhs.end.y - rhs.end.y),
            control1: CGPoint(
                x: lhs.control1.x - rhs.control1.x,
                y: lhs.control1.y - rhs.control1.y),
            control2: CGPoint(
                x: lhs.control2.x - rhs.control2.x,
                y: lhs.control2.y - rhs.control2.y))
    }

    static func -= (lhs: inout AnimatableCubicCurve, rhs: AnimatableCubicCurve) {
        lhs = lhs - rhs
    }

    static func + (lhs: AnimatableCubicCurve, rhs: AnimatableCubicCurve) -> AnimatableCubicCurve {
        return AnimatableCubicCurve(
            start: CGPoint(
                x: lhs.start.x + rhs.start.x,
                y: lhs.start.y + rhs.start.y),
            end: CGPoint(
                x: lhs.end.x + rhs.end.x,
                y: lhs.end.y + rhs.end.y),
            control1: CGPoint(
                x: lhs.control1.x + rhs.control1.x,
                y: lhs.control1.y + rhs.control1.y),
            control2: CGPoint(
                x: lhs.control2.x + rhs.control2.x,
                y: lhs.control2.y + rhs.control2.y))
    }

    static func += (lhs: inout AnimatableCubicCurve, rhs: AnimatableCubicCurve) {
        lhs = lhs + rhs
    }

    static func * (lhs: AnimatableCubicCurve, rhs: Double) -> AnimatableCubicCurve {
        return AnimatableCubicCurve(
            start: CGPoint(
                x: lhs.start.x * CGFloat(rhs),
                y: lhs.start.y * CGFloat(rhs)),
            end: CGPoint(
                x: lhs.end.x * CGFloat(rhs),
                y: lhs.end.y * CGFloat(rhs)),
            control1: CGPoint(
                x: lhs.control1.x * CGFloat(rhs),
                y: lhs.control1.y * CGFloat(rhs)),
            control2: CGPoint(
                x: lhs.control2.x * CGFloat(rhs),
                y: lhs.control2.y * CGFloat(rhs)))
    }
}
// MARK: LineChartView
struct LineChartView<T: Object>: View where T: ObjectKeyIdentifiable {

    @State private var toggle = false

    static func points(for values: [Double]) -> [AnimatableCubicCurve] {
        guard let maxY = values.max(), let minY = values.min() else {
            return []
        }
        let points: [CGPoint] = values.indices.compactMap { idx in
            let candlestick = values[idx]
            let x = Double(idx)/Double(values.count)// * rect.size.width
            let normalizedMACD = (candlestick - minY) / (maxY - minY)
            let y = (1 - normalizedMACD)// * rect.maxY
            guard y.isNaN == false else {
                print(candlestick, normalizedMACD)
                return nil
            }
            return CGPoint(x: x, y: y)
        }
        let controlPoints = CubicCurveAlgorithm().controlPointsFromPoints(dataPoints: points)

        return points.indices.compactMap { i in
            let point = points[i];

            if i != 0 {
                let lastPoint = points[i - 1]
                let segment = controlPoints[i - 1]
                return AnimatableCubicCurve(start: lastPoint, end: point, control1: segment.controlPoint1, control2: segment.controlPoint2)
            } else {
                return nil
            }
        }
    }

    @Binding var interval: TimeInterval
    var intervalPath: KeyPath<T, TimeInterval>
    var valuePath: KeyPath<T, Double>
    let color: Color
    let lineWidth: CGFloat
    @ObservedResults(T.self) var candlesticks

    struct CubicCurveShape: Shape {
        var startPoint: CGPoint
        var endPoint: CGPoint
        var controlPoint1: CGPoint
        var controlPoint2: CGPoint

        private var animatableSegment: AnimatableCubicCurve

        var animatableData: AnimatableCubicCurve {
            get {
                AnimatableCubicCurve(start: startPoint, end: endPoint, control1: controlPoint1, control2: controlPoint2)
            }
            set {
                startPoint = newValue.start
                endPoint = newValue.end
                controlPoint1 = newValue.control1
                controlPoint2 = newValue.control2
            }
        }

        init(startPoint: CGPoint, endPoint: CGPoint, controlPoint1: CGPoint, controlPoint2: CGPoint) {
            self.startPoint = startPoint
            self.endPoint = endPoint
            self.controlPoint1 = controlPoint1
            self.controlPoint2 = controlPoint2
            self.animatableSegment = AnimatableCubicCurve(
                start: startPoint,
                end: endPoint,
                control1: controlPoint1,
                control2: controlPoint2)
        }

        init(curve: AnimatableCubicCurve) {
            self.init(startPoint: curve.start,
                      endPoint: curve.end,
                      controlPoint1: curve.control1,
                      controlPoint2: curve.control2)
        }

        func path(in rect: CGRect) -> Path {
            let start = CGPoint(x: startPoint.x * rect.size.width,
                                y: startPoint.y * rect.maxY)
            let end = CGPoint(x: endPoint.x * rect.size.width,
                              y: endPoint.y * rect.maxY)
            let control1 = CGPoint(x: controlPoint1.x * rect.size.width,
                                   y: controlPoint1.y * rect.maxY)
            let control2 = CGPoint(x: controlPoint2.x  * rect.size.width,
                                   y: controlPoint2.y * rect.maxY)

            var path = Path()
            path.move(to: start)
            path.addCurve(to: end, control1: control1, control2: control2)
            return path
        }
    }


    var points: [AnimatableCubicCurve] {
        let candlesticks = self.candlesticks.filter { (candlestick: T) -> Bool in
            let minimumInterval = (self.candlesticks.last?[keyPath: intervalPath] ?? 0) - interval
            return candlestick[keyPath: intervalPath] > minimumInterval
        }.sorted(by: { o1, o2 in
            o1[keyPath: intervalPath] < o2[keyPath: intervalPath]
        })
        let values = candlesticks.map { $0[keyPath: valuePath] }.filter { !$0.isNaN }
        return Self.points(for: values)
    }

    var body: some View {
        ZStack {
            ForEach(points, id: \.end.y) { curve in
                CubicCurveShape(curve: curve)
                    .stroke(color, lineWidth: lineWidth)
            }.animation(.spring(), value: points)
        }
    }
}
extension Shape {
    func fill<Fill: ShapeStyle, Stroke: ShapeStyle>(_ fillStyle: Fill, strokeBorder strokeStyle: Stroke, lineWidth: CGFloat = 1) -> some View {
        self
            .stroke(strokeStyle, lineWidth: lineWidth)
            .background(self.fill(fillStyle))
    }
}
var cancellables = [AnyCancellable]()

@available(macOS 12.0, *)
struct Axis: View {
    enum Kind {
        case x, y
    }
    @ObservedResults(Candlestick.self) var candlesticks
    var axis: Kind
    @Binding var interval: TimeInterval


    var minY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .min(ofProperty: "low") ?? 0
    }
    var maxY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .max(ofProperty: "high") ?? 0
    }
    var yAxis: [Double] {
        guard candlesticks.count > 1 else {
            return []
        }
        return stride(from: minY, through: maxY, by: (maxY-minY)/2).reversed()
    }

    private var view: some View {
        ForEach(yAxis, id: \.self) { value in
            Text(String(format: "%.2f", value)).background(Color.blue)
            if value != yAxis.last! {
                Spacer().background(Color.green)
            }
        }.background(Color.purple)
    }
    var body: some View {
        if axis == .y {
            VStack {
                view
            }.background(Color.pink)
                .padding(.top)
                .padding(.bottom)
        } else {
            HStack {
                view
            }.background(Color.pink)
                .padding(.top)
                .padding(.bottom)
        }
    }
}
@available(macOS 12.0, *)
struct ContentView: View {
    let maxCandles = 50

    @State var toggle = false
    var candlesticks: Results<Candlestick>
    //: Results<Candlestick>
    @State var interval = 24.0

    init() {
        self.candlesticks = try! Realm().objects(Candlestick.self)
    }

    var minY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .min(ofProperty: "low")!
    }
    var maxY: Double {
        return candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval)
            .max(ofProperty: "high")!
    }

    var yAxis: [Double] {
        guard candlesticks.count > 1 else { return [] }
        return stride(from: minY, through: maxY, by: (maxY-minY)/2).reversed()
    }
    var xAxis: [Int] {
        let candleIdsAtInterval = candlesticks.filter("id > %@", (candlesticks.last?.id ?? 0) - interval).map(\.id)
        guard candleIdsAtInterval.count > 5 else {
            return candleIdsAtInterval.map { Int($0) }
        }
        let max = candleIdsAtInterval.max() ?? 0
        let min = candleIdsAtInterval.min() ?? 0
        return stride(from: min, through: max, by: (max-min)/2).map { Int($0) }
    }
    var body: some View {
        VStack {
            HStack {
                Axis(axis: .y, interval: $interval)
                VStack(alignment: .center) {
                    ZStack {
                        CandlestickView(interval: interval)
                        LineChartView<Candlestick>(interval: $interval, intervalPath: \.id, valuePath: \.MACD, color: .cyan, lineWidth: 3)
                        LineChartView<Candlestick>(interval: $interval, intervalPath: \.id, valuePath: \.MACDEMA9, color: Color.init(red: 0.9804, green: 0.502, blue: 0.4471), lineWidth: 3)
                    }
                    HStack {
                        Button("24 ticks") {
                            interval = 24
                        }
                        Button("48 ticks") {
                            interval = 48
                        }
                        Button("96 ticks") {
                            interval = 96
                        }
                    }
                }.background(Color.primary).padding()
            }
            HStack(alignment: .center) {
                ForEach(xAxis, id: \.self) { value in
                    Text(String(value))
                    if value != xAxis.last! {
                        Spacer()
                    }
                }
            }
        }.background(Color.gray)
            .task {
                try! await CandlestickPublisher.shared.initialize()
            }
    }
}
@available(macOS 12.0, *)
struct ContentView_Previews: PreviewProvider {
    static var previews: some View {
        return ContentView()
            .previewInterfaceOrientation(.landscapeLeft)
    }
}
