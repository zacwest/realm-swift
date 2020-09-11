////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#if canImport(SwiftUI)
import SwiftUI

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, tvOS 13.0, *)
@propertyWrapper
public struct FetchObjects<O: Object> {
    @Environment(\.realmConfiguration) var configuaration: Realm.Configuration

    private let frozen: Bool
    private let predicate: NSPredicate?

    public init(frozen: Bool=false, predicate: NSPredicate?=nil) {
        self.frozen = frozen
        self.predicate = predicate
    }

    public var wrappedValue: Results<O> {
        get {
            do {
                let realm = try Realm(configuration: configuaration)
                if frozen && (predicate != nil) {
                    return realm.objects(O.self).filter(predicate!).freeze()
                } else if predicate != nil {
                    return realm.objects(O.self).filter(predicate!)
                } else if frozen {
                    return realm.objects(O.self).freeze()
                } else {
                    return realm.objects(O.self)
                }
            } catch {
                fatalError()
            }
        }
    }
}

// There is a limitation working with @Environment because our property wrappers are outside the views hirearcy.
// Therefore SwiftUI's Environment Key Value store is empty when we try to access it. To get around this we store a instance of the realm configuration and
// create the realm instance where needed.
@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, tvOS 13.0, *)
extension EnvironmentValues {
    private static var _realmConfiguration: Realm.Configuration?
    public var realmConfiguration: Realm.Configuration {
        get {
            Self._realmConfiguration ?? .defaultConfiguration
        }
        set {
            Self._realmConfiguration = newValue
        }
    }
}

#endif // canImport(SwiftUI)
