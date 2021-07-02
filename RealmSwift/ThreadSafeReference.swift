////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

import Realm

/**
 Objects of types which conform to `ThreadConfined` can be managed by a Realm, which will make
 them bound to a thread-specific `Realm` instance. Managed objects must be explicitly exported
 and imported to be passed between threads.

 Managed instances of objects conforming to this protocol can be converted to a thread-safe
 reference for transport between threads by passing to the `ThreadSafeReference(to:)` constructor.

 Note that only types defined by Realm can meaningfully conform to this protocol, and defining new
 classes which attempt to conform to it will not make them work with `ThreadSafeReference`.
 */
public protocol ThreadConfined {
    // Must also conform to `AssistedObjectiveCBridgeable`

    /**
     The Realm which manages the object, or `nil` if the object is unmanaged.

     Unmanaged objects are not confined to a thread and cannot be passed to methods expecting a
     `ThreadConfined` object.
     */
    var realm: Realm? { get }

    /// Indicates if the object can no longer be accessed because it is now invalid.
    var isInvalidated: Bool { get }

    /**
    Indicates if the object is frozen.

    Frozen objects are not confined to their source thread. Forming a `ThreadSafeReference` to a
    frozen object is allowed, but is unlikely to be useful.
    */
    var isFrozen: Bool { get }

    /**
     Returns a frozen snapshot of this object.

     Unlike normal Realm live objects, the frozen copy can be read from any thread, and the values
     read will never update to reflect new writes to the Realm. Frozen collections can be queried
     like any other Realm collection. Frozen objects cannot be mutated, and cannot be observed for
     change notifications.

     Unmanaged Realm objects cannot be frozen.

     - warning: Holding onto a frozen object for an extended period while performing write
     transaction on the Realm may result in the Realm file growing to large sizes. See
     `Realm.Configuration.maximumNumberOfActiveVersions` for more information.
    */
    func freeze() -> Self

    /**
     Returns a live (mutable) reference of this object.
     Will return self if called on an already live object.
     */
    func thaw() -> Self?
}

/**
 An object intended to be passed between threads containing a thread-safe reference to its
 thread-confined object.

 To resolve a thread-safe reference on a target Realm on a different thread, pass to
 `Realm.resolve(_:)`.

 - warning: A `ThreadSafeReference` object must be resolved at most once.
            Failing to resolve a `ThreadSafeReference` will result in the source version of the
            Realm being pinned until the reference is deallocated.

 - note: Prefer short-lived `ThreadSafeReference`s as the data for the version of the source Realm
         will be retained until all references have been resolved or deallocated.

 - see: `ThreadConfined`
 - see: `Realm.resolve(_:)`
 */
@frozen public struct ThreadSafeReference<Confined: ThreadConfined> {
    fileprivate let swiftMetadata: Any?

    /**
     Indicates if the reference can no longer be resolved because an attempt to resolve it has
     already occurred. References can only be resolved once.
     */
    public var isInvalidated: Bool { return objectiveCReference.isInvalidated }

    fileprivate let objectiveCReference: RLMThreadSafeReference<RLMThreadConfined>

    /**
     Create a thread-safe reference to the thread-confined object.

     - parameter threadConfined: The thread-confined object to create a thread-safe reference to.

     - note: You may continue to use and access the thread-confined object after passing it to this
             constructor.
     */
    public init(to threadConfined: Confined) {
        let bridged = (threadConfined as! AssistedObjectiveCBridgeable).bridged
        swiftMetadata = bridged.metadata
        objectiveCReference = RLMThreadSafeReference(threadConfined: bridged.objectiveCValue as! RLMThreadConfined)
    }

    internal func resolve(in realm: Realm) -> Confined? {
        guard let objectiveCValue = realm.rlmRealm.__resolve(objectiveCReference) else { return nil }
        return ((Confined.self as! AssistedObjectiveCBridgeable.Type).bridging(from: objectiveCValue, with: swiftMetadata) as! Confined)
    }
}

extension Realm {
    // MARK: Thread Safe Reference

    /**
     Returns the same object as the one referenced when the `ThreadSafeReference` was first
     created, but resolved for the current Realm for this thread. Returns `nil` if this object was
     deleted after the reference was created.

     - parameter reference: The thread-safe reference to the thread-confined object to resolve in
                            this Realm.

     - warning: A `ThreadSafeReference` object must be resolved at most once.
                Failing to resolve a `ThreadSafeReference` will result in the source version of the
                Realm being pinned until the reference is deallocated.
                An exception will be thrown if a reference is resolved more than once.

     - warning: Cannot call within a write transaction.

     - note: Will refresh this Realm if the source Realm was at a later version than this one.

     - see: `ThreadSafeReference(to:)`
     */
    public func resolve<Confined>(_ reference: ThreadSafeReference<Confined>) -> Confined? {
        return reference.resolve(in: self)
    }
}

@available(iOSApplicationExtension 15.0, *)
@available(macOSApplicationExtension 12.0, *)
public class RealmTaskLocalBase {
    fileprivate class Context {
        private let configuration: Realm.Configuration
        private let threadSafeReference: AnyThreadSafeReference
        lazy var resolved = threadSafeReference.resolve(in: try! Realm(configuration: configuration))
        deinit {
            print("deinit")
        }
        init(configuration: Realm.Configuration, threadSafeReference: AnyThreadSafeReference) {
            self.configuration = configuration
            self.threadSafeReference = threadSafeReference
        }
    }

    fileprivate let defaultValue: ThreadConfined?
    fileprivate let taskLocal: TaskLocal<Context?>

    init(defaultValue: ThreadConfined?) {
        self.defaultValue = defaultValue
        if let defaultValue = defaultValue {
            self.taskLocal = TaskLocal(wrappedValue: Context(configuration: defaultValue.realm!.configuration,
                                                             threadSafeReference: AnyThreadSafeReference(to: defaultValue)))
        } else {
            self.taskLocal = TaskLocal(wrappedValue: nil)
        }
    }

    @discardableResult
    func withValue<R>(_ valueDuringOperation: ThreadConfined, operation: @escaping () async throws -> R) async throws -> R {
        guard let configuration = valueDuringOperation.realm?.configuration else {
            throwRealmException("Cannot use unmanaged objects as TaskLocal values")
        }

        let ctx = Context(configuration: configuration,
                          threadSafeReference: AnyThreadSafeReference(to: valueDuringOperation))
        return try await taskLocal.withValue(ctx,
                                             operation: operation)
    }
}

extension Realm {
    func resolve(_ reference: AnyThreadSafeReference) -> ThreadConfined? {
        return reference.resolve(in: self)
    }
}

final class AnyThreadSafeReference {
    private var _isInvalidated: () -> Bool
    public var isInvalidated: Bool { return _isInvalidated() }
    private let objectiveCReference: RLMThreadSafeReference<RLMThreadConfined>
    private let swiftMetadata: Any?
    private let assistedType: AssistedObjectiveCBridgeable.Type

    init<Confined: ThreadConfined>(_ threadSafeReference: ThreadSafeReference<Confined>) {
        self.objectiveCReference = threadSafeReference.objectiveCReference
        swiftMetadata = threadSafeReference.swiftMetadata
        self._isInvalidated = { threadSafeReference.isInvalidated }
        self.assistedType = Confined.self as! AssistedObjectiveCBridgeable.Type
    }
    init(to threadConfined: ThreadConfined) {
        let bridged = (threadConfined as! AssistedObjectiveCBridgeable).bridged
        assistedType = type(of: (threadConfined as! AssistedObjectiveCBridgeable))
        swiftMetadata = bridged.metadata
        objectiveCReference = RLMThreadSafeReference(threadConfined: bridged.objectiveCValue as! RLMThreadConfined)
        self._isInvalidated = { RLMThreadSafeReference(threadConfined: bridged.objectiveCValue as! RLMThreadConfined).isInvalidated }
//        self.assistedType =
    }

    internal func resolve(in realm: Realm) -> ThreadConfined? {
        guard let objectiveCValue = realm.rlmRealm.__resolve(objectiveCReference) else { return nil }
        return (assistedType.bridging(from: objectiveCValue, with: swiftMetadata) as! ThreadConfined)
    }
}

@available(iOSApplicationExtension 15.0, *)
@available(macOSApplicationExtension 12.0, *)
@dynamicCallable
@propertyWrapper public final class RealmTaskLocal<Value: ThreadConfined>: RealmTaskLocalBase, UnsafeSendable {
    private class Context {
        private let configuration: Realm.Configuration
        private let threadSafeReference: ThreadSafeReference<Value>
        lazy var resolved = threadSafeReference.resolve(in: try! Realm(configuration: configuration))

        deinit {
            print("deinit")
        }
        init(configuration: Realm.Configuration, threadSafeReference: ThreadSafeReference<Value>) {
            self.configuration = configuration
            self.threadSafeReference = threadSafeReference
        }
    }
    public init(wrappedValue defaultValue: Value?) where Value == Realm {
        super.init(defaultValue: defaultValue)
    }
    public init(wrappedValue defaultValue: Value?) where Value: Object {
        super.init(defaultValue: defaultValue)
    }
    public init<T: RealmSwiftObject>(wrappedValue defaultValue: Value?) where Value == Results<T> {
        super.init(defaultValue: try! Realm().objects(T.self))
    }
    public func dynamicallyCall(withArguments arguments: [ThreadConfined]) -> (RealmTaskLocalBase, ThreadConfined) {
        return (self, arguments.first!)
    }
//    public init<T: Object>(_ type: T.Type) where Value == Results<T> {
//        self.defaultValue = Results(RLMResults.emptyDetached())
//        self.taskLocal = TaskLocal(wrappedValue: nil)
//    }

    fileprivate var isInWriteTransaction = TaskLocal<Bool>(wrappedValue: false)

    /// Gets the value currently bound to this task-local from the current task.
    ///
    /// If no current task is available in the context where this call is made,
    /// or if the task-local has no value bound, this will return the `defaultValue`
    /// of the task local.
    public func get() -> Value? {
        let resolved = taskLocal.wrappedValue?.resolved
        if let resolved = resolved, let realm = resolved.realm, isInWriteTransaction.wrappedValue && !realm.isInWriteTransaction {
            realm.beginWrite()
        }
        return taskLocal.wrappedValue?.resolved as? Value
    }

    @discardableResult
    public func withResults<R, T: Object>(operation: @escaping () async throws -> R) async throws -> R where Value == Results<T> {
        return try await taskLocal.withValue(RealmTaskLocalBase.Context(configuration: Realm.Configuration.defaultConfiguration,
                                                     threadSafeReference: AnyThreadSafeReference(to: try Realm().objects(T.self))),
                                             operation: operation)
    }

    /// Binds the task-local to the specific value for the duration of the asynchronous operation.
    ///
    /// The value is available throughout the execution of the operation closure,
    /// including any `get` operations performed by child-tasks created during the
    /// execution of the operation closure.
    ///
    /// If the same task-local is bound multiple times, be it in the same task, or
    /// in specific child tasks, the more specific (i.e. "deeper") binding is
    /// returned when the value is read.
    ///
    /// If the value is a reference type, it will be retained for the duration of
    /// the operation closure.
    @discardableResult
    public func withValue<R>(_ valueDuringOperation: Value, operation: @escaping () async throws -> R) async throws -> R {
//        guard let configuration = valueDuringOperation.realm?.configuration else {
//            throwRealmException("Cannot use unmanaged objects as TaskLocal values")
//        }

        return try await super.withValue(valueDuringOperation, operation: operation)

//        return try await taskLocal.withValue(Context(configuration: configuration,
//                                                     threadSafeReference: ThreadSafeReference(to: valueDuringOperation)),
//                                             operation: operation)
    }

//    public func withValue<R>(_ valueDuringOperation: Value, operation: @escaping () throws -> R) rethrows -> R {
//        guard let configuration = valueDuringOperation.realm?.configuration else {
//            throwRealmException("Cannot use unmanaged objects as TaskLocal values")
//        }
//
//        self.taskLocal.withValue(Context(configuration: configuration,
//                                                   threadSafeReference: ThreadSafeReference(to: valueDuringOperation)),
//                                           operation: operation)
//    }

    public var projectedValue: RealmTaskLocal<Value> {
      get {
        self
      }

      @available(*, unavailable, message: "use '$myTaskLocal.withValue(_:do:)' instead")
      set {
        fatalError("Illegal attempt to set a \(Self.self) value, use `withValue(...) { ... }` instead.")
      }
    }

    // This subscript is used to enforce that the property wrapper may only be used
    // on static (or rather, "without enclosing instance") properties.
    // This is done by marking the `_enclosingInstance` as `Never` which informs
    // the type-checker that this property-wrapper never wants to have an enclosing
    // instance (it is impossible to declare a property wrapper inside the `Never`
    // type).
    public static subscript(
      _enclosingInstance object: Never,
      wrapped wrappedKeyPath: ReferenceWritableKeyPath<Never, Value>,
      storage storageKeyPath: ReferenceWritableKeyPath<Never, TaskLocal<Value>>
    ) -> Value {
      get {
        fatalError()
      }
    }

    public var wrappedValue: Value? {
      self.get()
    }
}

@available(macOSApplicationExtension 12.0, *)
@available(iOSApplicationExtension 15.0, *)
extension RealmTaskLocal where Value == Realm {
    @discardableResult
    public func write<Result>(_ taskLocals: (RealmTaskLocalBase, ThreadConfined)...,
                              withoutNotifying tokens: [NotificationToken] = [],
                              _ block: (@escaping @Sendable () async throws -> Result)) async throws -> Result {
//        beginWrite()
        var ret: Result!
        do {
            try await self.isInWriteTransaction.withValue(true) {
                var i = 0
                let curriedOperations = taskLocals.reduce(into: [() async throws -> ()]()) { partialResult, taskLocalPair in
                    partialResult.append({ [i, partialResult] in
                        if i != 0 {
                            try await taskLocalPair.0.withValue(taskLocalPair.1, operation: {
//                                guard let realm = taskLocalPair.0.taskLocal.get()?.resolved?.realm else {
//                                    fatalError()
//                                }
//                                if !realm.isInWriteTransaction {
//                                    realm.beginWrite()
//                                }
                                try await partialResult[i - 1]()
//                                if realm.isInWriteTransaction {
//                                    try realm.commitWrite()
//                                }
                            })
                        }
                        if i == 0 {
                            try await taskLocalPair.0.withValue(taskLocalPair.1, operation: {
                                guard let realm = taskLocalPair.0.taskLocal.get()?.resolved?.realm else {
                                    fatalError()
                                }
                                if !realm.isInWriteTransaction {
                                    realm.beginWrite()
                                }
                                ret = try await block()
                                try realm.commitWrite()
                            })
                        }
                    })
                    i += 1
                }
                if curriedOperations.count > 0 {
                    try await curriedOperations.last!()
                } else {
                    try await self.withValue(try Realm()) {
                        guard let realm = self.get() else {
                            fatalError()
                        }
                        if !realm.isInWriteTransaction {
                            realm.beginWrite()
                        }
                        ret = try await block()
                        try realm.commitWrite()
                    }
                }
            }
        } catch let error {
//            if isInWriteTransaction { cancelWrite() }
            throw error
        }
//        if isInWriteTransaction { try commitWrite(withoutNotifying: tokens) }
        return ret
    }
}
