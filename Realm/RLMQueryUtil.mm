////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
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

#import "RLMQueryUtil.hpp"

#import "RLMDecimal128_Private.hpp"
#import "RLMObjectId_Private.hpp"
#import "RLMObjectSchema_Private.h"
#import "RLMObject_Private.hpp"
#import "RLMPredicateUtil.hpp"
#import "RLMQueryBuilder.hpp"
#import "RLMUtil.hpp"

#include <realm/query.hpp>

using namespace realm;

namespace {

QueryMixed toMixed(id value) {
    if (!value || value == NSNull.null) {
        return {};
    }
    if (auto decimal = RLMDynamicCast<RLMDecimal128>(value)) {
        return realm::Decimal128(decimal.decimal128Value);
    }
    if (auto string = RLMDynamicCast<NSString>(value)) {
        return RLMStringDataWithNSString(string);
    }
    if (auto data = RLMDynamicCast<NSData>(value)) {
        return RLMBinaryDataForNSData(data);
    }
    if (auto number = RLMDynamicCast<NSDecimalNumber>(value)) {
        return realm::Decimal128(number.stringValue.UTF8String);
    }
    if (auto number = RLMDynamicCast<NSNumber>(value)) {
        switch (*number.objCType) {
            case 'f':
                return number.floatValue;
            case 'd':
                return number.doubleValue;
            case 'c':
            case 'B':
                return number.boolValue;
            default:
                return number.longLongValue;
        }
    }
    if (auto date = RLMDynamicCast<NSDate>(value)) {
        return RLMTimestampForNSDate(date);
    }
    if (auto obj = RLMDynamicCast<RLMObjectBase>(value)) {
        return {obj->_objectSchema.objectName.UTF8String, obj->_row.get_key()};
    }
    if (auto objId = RLMDynamicCast<RLMObjectId>(value)) {
        return objId.value;
    }
    @throw RLMException(@"Unsupported value '%@' for query", value);
}

inline realm::QueryMixed evaluateExpression(id value) {
    while ([value isKindOfClass:[NSExpression class]]) {
        if ([value expressionType] == NSConstantValueExpressionType) {
            value = [value constantValue];
        }
        else {
            @throw RLMException(@"Expected constant value but got expression '%@'", value);
        }
    }
    if (auto enumerable = RLMAsFastEnumeration(value)) {
        std::vector<realm::QueryMixed> evaluated;
        for (id value in enumerable) {
            evaluated.push_back(evaluateExpression(value));
        }
        return evaluated;
    }
    return toMixed(value);
}

static bool is_self_value_for_key_path_function_expression(NSExpression *expression)
{
    return expression.expressionType == NSFunctionExpressionType
        && expression.operand.expressionType == NSEvaluatedObjectExpressionType
        && [expression.function isEqualToString:@"valueForKeyPath:"];
}

PredicateExpression convertExpression(NSExpression *e);
Predicate convertPredicate(NSPredicate *p) {
    using Type = Predicate::Kind;

    if (auto compound = RLMDynamicCast<NSCompoundPredicate>(p)) {
        std::vector<Predicate> subpredicates;
        for (NSPredicate *sub in compound.subpredicates) {
            subpredicates.push_back(convertPredicate(sub));
        }
        return Predicate(static_cast<CompoundPredicateType>(compound.compoundPredicateType),
                         std::move(subpredicates));
    }
    if (auto comparison = RLMDynamicCast<NSComparisonPredicate>(p)) {
        return Predicate(convertExpression(comparison.leftExpression),
                         convertExpression(comparison.rightExpression),
                         static_cast<Predicate::Modifier>(comparison.comparisonPredicateModifier),
                         static_cast<OperatorType>(comparison.predicateOperatorType),
                         static_cast<ComparisonOptions>(comparison.options));
    }
    if ([p isEqual:[NSPredicate predicateWithValue:YES]]) {
        return Predicate(Type::True);
    }
    if ([p isEqual:[NSPredicate predicateWithValue:NO]]) {
        return Predicate(Type::False);
    }
    @throw RLMException(@"Unsupported predicate [%@] %@", [p class], p);
}

PredicateExpression convertExpression(NSExpression *e) {
    using Type = PredicateExpression::Type;

    auto type = static_cast<Type>(e.expressionType);
    // 10 is an undocumented expression type that seems to be equivalent to a keypath
    if (type == static_cast<Type>(10)) {
        type = Type::KeyPath;
    }
    switch (type) {
        case Type::ConstantValue:
        case Type::Aggregate:
            return {type, evaluateExpression(e.constantValue)};
        case Type::KeyPath:
            return {type, StringData(e.keyPath.UTF8String)};
        case Type::Function: {
            std::unique_ptr<PredicateExpression> argument, operand;
            if (auto a = e.arguments.firstObject)
                argument = std::make_unique<PredicateExpression>(convertExpression(a));
            if (auto o = e.operand)
                operand = std::make_unique<PredicateExpression>(convertExpression(o));
            return {type, std::move(operand), e.function.UTF8String, std::move(argument)};
        }
        case Type::Subquery: {
            // Eliminate references to the iteration variable in the subquery.
            NSPredicate *subqueryPredicate = [e.predicate predicateWithSubstitutionVariables:@{e.variable: [NSExpression expressionForEvaluatedObject]}];
            subqueryPredicate = transformPredicate(subqueryPredicate, [](NSExpression *expression) {
                if (is_self_value_for_key_path_function_expression(expression)) {
                    if (NSString *keyPath = [expression.arguments.firstObject keyPath]) {
                        return [NSExpression expressionForKeyPath:keyPath];
                    }
                }
                return expression;
            });
            return {type, [e.collection keyPath].UTF8String, std::make_unique<Predicate>(convertPredicate(subqueryPredicate))};
        }
        default:
            @throw RLMException(@"Unsupported predicate expression [%@] %@", [e class], e);
    }
}
}

// return the property for a validated column name
// FIXME: used elsewhere, but not in this file
RLMProperty *RLMValidatedProperty(RLMObjectSchema *desc, NSString *columnName) {
    RLMProperty *prop = desc[columnName];
    if (!prop) {
        @throw RLMException(@"Property '%@' not found in object of type '%@'", columnName, desc.className);
    }
    return prop;
}

realm::Query RLMPredicateToQuery(NSPredicate *predicate, RLMObjectSchema *objectSchema,
                                 RLMSchema *schema, Group &group)
{
    // passing a nil predicate is a no-op
    if (!predicate) {
        return realm::Query();
//        return get_table(group, objectSchema).where();
    }
    @autoreleasepool {
        try {
            return RLMPredicateToQuery(convertPredicate(predicate), objectSchema, schema, group);
        }
        catch (std::exception const& e) {
            @throw RLMException(e);
        }
    }
}
