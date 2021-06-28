////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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
import Realm.Private

// An opaque identifier for each property on a class. Happens to currently be
// the property's index in the object schema, but that's not something that any
// of the Swift code should rely on. In the future it may make sense to change
// this to the ColKey.
public typealias PropertyKey = UInt16

// A tag protocol used in schema discovery to find @Persisted properties
public protocol _DiscoverablePersistedProperty: _RealmSchemaDiscoverable {}

// A type which can be stored by the @Persisted property wrapper
public protocol _Persistable: _RealmSchemaDiscoverable {
    // Read a value of this type from the target object
    static func _rlmGetProperty(_ obj: ObjectBase, _ key: PropertyKey) -> Self
    // Read an optional value of this type from the target object
    static func _rlmGetPropertyOptional(_ obj: ObjectBase, _ key: PropertyKey) -> Self?
    // Set a value of this type on the target object
    static func _rlmSetProperty(_ obj: ObjectBase, _ key: PropertyKey, _ value: Self)
    // Get the zero/empty/nil value for this type. Used to supply a default
    // when the user does not declare one in their model.
    static func _rlmDefaultValue() -> Self
    // Set the swiftAccessor for this type if the default PersistedPropertyAccessor
    // is not suitable.
    static func _rlmSetAccessor(_ prop: RLMProperty)
    // Do the values of this type need to be cached on the Persisted?
    static var _rlmRequiresCaching: Bool { get }
}
extension _Persistable {
    public static var _rlmRequiresCaching: Bool {
        false
    }
}

// A tag protocol for persistable types which can appear inside Optional
public protocol _OptionalPersistable: _Persistable {
}

extension _OptionalPersistable {
    public static func _rlmSetAccessor(_ prop: RLMProperty) {
        if prop.optional {
            prop.swiftAccessor = PersistedPropertyAccessor<Optional<Self>>.self
        } else {
            prop.swiftAccessor = PersistedPropertyAccessor<Self>.self
        }
    }
}

// Default definition of _rlmDefaultValue used by everything exception for
// Optional, which requires doing Optional<T>.none rather than Optional<T>().
public protocol _DefaultConstructible {
    init()
}
extension _Persistable where Self: _DefaultConstructible {
    public static func _rlmDefaultValue() -> Self { .init() }
}