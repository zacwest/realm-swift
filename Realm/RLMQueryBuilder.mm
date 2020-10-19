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

#import "RLMQueryBuilder.hpp"

#import "RLMSchema_Private.hpp"
#import "RLMObjectSchema_Private.hpp"
#import "RLMProperty_Private.hpp"
#import "RLMUtil.hpp"

#import "object_store.hpp"
#import "results.hpp"

#include <realm/query_engine.hpp>
#include <realm/query_expression.hpp>
#include <realm/util/cf_ptr.hpp>
#include <realm/util/overload.hpp>

using namespace realm;

namespace {
REALM_NORETURN REALM_NOINLINE REALM_COLD static void precondition_failure(StringData msg)
{
    throw std::logic_error(msg);
}
REALM_NORETURN REALM_NOINLINE REALM_COLD static void precondition_failure(const char* msg)
{
    throw std::logic_error(msg);
}
template<typename... Args>
REALM_NORETURN REALM_NOINLINE REALM_COLD static void precondition_failure(const char* format, Args&&... args)
{
    precondition_failure(util::format(format, static_cast<Args&&>(args)...));
}

// check a precondition and throw an exception if it is not met
// this should be used iff the condition being false indicates a bug in the caller
// of the function checking its preconditions
template<typename... Args>
REALM_FORCEINLINE static void precondition(bool condition, const char* format, Args&&... args)
{
    if (__builtin_expect(condition, 1)) {
        return;
    }
    precondition_failure(format, static_cast<Args&&>(args)...);
}

bool type_is_numeric(DataType type) {
    switch (type) {
        case type_Int:
        case type_Float:
        case type_Double:
        case type_Decimal:
            return true;
        default:
            return false;
    }
}

bool validate_value(QueryMixed const& value, bool array, ColKey col, StringData object_type) {
    if (!col) {
        return true;
    }
    if (col.is_nullable() && value.is_null()) {
        return true;
    }
    if (array) {
        if (value.get_type() == type_LinkList) {
            auto& array = value.get_array();
            return std::all_of(array.begin(), array.end(), [&](auto& value) {
                return validate_value(value, false, col, object_type);
            });
        }
    }
    if (value.is_null()) {
        return false;
    }

    switch (col.get_type()) {
        case col_type_String:
            return value.get_type() == type_String;
        case col_type_Bool:
            return value.get_type() == type_Bool
                || (value.get_type() == type_Int && (value.get<Int>() == 0 || value.get<Int>() == 1));
        case col_type_Timestamp:
            return value.get_type() == type_Timestamp;
        case col_type_Int:
            return value.get_type() == type_Int;
        case col_type_Float:
            if (value.get_type() == type_Double) {
                double v = value.get<double>();
                return isnan(v) || (v >= std::numeric_limits<float>::lowest() && v <= std::numeric_limits<float>::max());
            }
            return value.get_type() == type_Float || value.get_type() == type_Int;
        case col_type_Double:
            return value.get_type() == type_Float || value.get_type() == type_Double || value.get_type() == type_Int;
        case col_type_Binary:
            return value.get_type() == type_Binary;
        case col_type_OldMixed:
            return false;
        case col_type_BackLink:
            return true;
        case col_type_LinkList:
        case col_type_Link: {
            if (value.get_type() != type_Link)
                return false;
            return value.get<std::pair<StringData, ObjKey>>().first == object_type;
        }
        case col_type_ObjectId:
            return value.get_type() == type_ObjectId;
        case col_type_Decimal:
            return type_is_numeric(value.get_type())
                || (value.get_type() == type_String && Decimal128::is_valid_str(value.get<StringData>().data()));
    }
    precondition_failure("Invalid PropertyType");
}

BOOL property_type_is_numeric(PropertyType propertyType) {
    switch (propertyType) {
        case PropertyType::Int:
        case PropertyType::Float:
        case PropertyType::Double:
        case PropertyType::Decimal:
            return YES;
        default:
            return NO;
    }
}


// Equal and ContainsSubstring are used by QueryBuilder::add_string_constraint as the comparator
// for performing diacritic-insensitive comparisons.

bool equal(CFStringCompareFlags options, StringData v1, StringData v2)
{
    if (v1.is_null() || v2.is_null()) {
        return v1.is_null() == v2.is_null();
    }

    auto s1 = util::adoptCF(CFStringCreateWithBytesNoCopy(kCFAllocatorSystemDefault, (const UInt8*)v1.data(), v1.size(),
                                                          kCFStringEncodingUTF8, false, kCFAllocatorNull));
    auto s2 = util::adoptCF(CFStringCreateWithBytesNoCopy(kCFAllocatorSystemDefault, (const UInt8*)v2.data(), v2.size(),
                                                          kCFStringEncodingUTF8, false, kCFAllocatorNull));

    return CFStringCompare(s1.get(), s2.get(), options) == kCFCompareEqualTo;
}

template <CFStringCompareFlags options>
struct Equal {
    using CaseSensitive = Equal<options & ~kCFCompareCaseInsensitive>;
    using CaseInsensitive = Equal<options | kCFCompareCaseInsensitive>;

    bool operator()(StringData v1, StringData v2, bool v1_null, bool v2_null) const
    {
        REALM_ASSERT_DEBUG(v1_null == v1.is_null());
        REALM_ASSERT_DEBUG(v2_null == v2.is_null());

        return equal(options, v1, v2);
    }

    static const char* description() { return options & kCFCompareCaseInsensitive ? "==[cd]" : "==[d]"; }
};

bool contains_substring(CFStringCompareFlags options, StringData v1, StringData v2)
{
    if (v2.is_null()) {
        // Everything contains NULL
        return true;
    }

    if (v1.is_null()) {
        // NULL contains nothing (except NULL, handled above)
        return false;
    }

    if (v2.size() == 0) {
        // Everything (except NULL, handled above) contains the empty string
        return true;
    }

    auto s1 = util::adoptCF(CFStringCreateWithBytesNoCopy(kCFAllocatorSystemDefault, (const UInt8*)v1.data(), v1.size(),
                                                          kCFStringEncodingUTF8, false, kCFAllocatorNull));
    auto s2 = util::adoptCF(CFStringCreateWithBytesNoCopy(kCFAllocatorSystemDefault, (const UInt8*)v2.data(), v2.size(),
                                                          kCFStringEncodingUTF8, false, kCFAllocatorNull));

    return CFStringFind(s1.get(), s2.get(), options).location != kCFNotFound;
}

template <CFStringCompareFlags options>
struct ContainsSubstring {
    using CaseSensitive = ContainsSubstring<options & ~kCFCompareCaseInsensitive>;
    using CaseInsensitive = ContainsSubstring<options | kCFCompareCaseInsensitive>;

    bool operator()(StringData v1, StringData v2, bool v1_null, bool v2_null) const
    {
        REALM_ASSERT_DEBUG(v1_null == v1.is_null());
        REALM_ASSERT_DEBUG(v2_null == v2.is_null());

        return contains_substring(options, v1, v2);
    }

    static const char* description() { return options & kCFCompareCaseInsensitive ? "CONTAINS[cd]" : "CONTAINS[d]"; }
};


const char *operatorName(OperatorType operatorType)
{
    switch (operatorType) {
        case OperatorType::LessThan: return "<";
        case OperatorType::LessThanOrEqual: return "<=";
        case OperatorType::GreaterThan: return ">";
        case OperatorType::GreaterThanOrEqual: return ">=";
        case OperatorType::Equal: return "==";
        case OperatorType::NotEqual: return "!=";
        case OperatorType::Matches: return "MATCHES";
        case OperatorType::Like: return "LIKE";
        case OperatorType::BeginsWith: return "BEGINSWITH";
        case OperatorType::EndsWith: return "ENDSWITH";
        case OperatorType::In: return "IN";
        case OperatorType::Contains: return "CONTAINS";
        case OperatorType::Between: return "BETWEEN";
        default: return "unknown operator";
    }
}

Table& get_table(Group& group, RLMObjectSchema *objectSchema)
{
    return *ObjectStore::table_for_object_type(group, objectSchema.objectName.UTF8String);
}

// A reference to a column within a query. Can be resolved to a Columns<T> for use in query expressions.
class ColumnReference {
public:
    ColumnReference(Query& query, Group& group, RLMSchema *schema, RLMProperty* property,
                    const std::vector<RLMProperty*>& links = {})
    : m_links(links), m_property(property), m_schema(schema), m_group(&group), m_query(&query), m_table(query.get_table())
    {
        auto& table = walk_link_chain([](Table const&, ColKey, PropertyType) { });
        m_col = table.get_column_key(m_property.columnName.UTF8String);
    }

    template <typename T, typename... SubQuery>
    auto resolve(SubQuery&&... subquery) const
    {
        static_assert(sizeof...(SubQuery) < 2, "resolve() takes at most one subquery");
        LinkChain lc(m_table);
        walk_link_chain([&](Table const& link_origin, ColKey col, PropertyType type) {
            if (type != PropertyType::LinkingObjects) {
                lc.link(col);
            }
            else {
                lc.backlink(link_origin, col);
            }
        });

        if (type() != PropertyType::LinkingObjects) {
            return lc.column<T>(column(), std::forward<SubQuery>(subquery)...);
        }

        if constexpr (std::is_same_v<T, Link>) {
            return with_link_origin(m_property, [&](Table& table, ColKey col) {
                return lc.column<T>(table, col, std::forward<SubQuery>(subquery)...);
            });
        }

        REALM_TERMINATE("LinkingObjects property did not have column type Link");
    }

    RLMProperty *property() const { return m_property; }
    ColKey column() const { return m_col; }
    PropertyType type() const { return static_cast<realm::PropertyType>(property().type); }
    Group& group() const { return *m_group; }

    RLMObjectSchema *link_target_object_schema() const
    {
        switch (type()) {
            case PropertyType::Object:
            case PropertyType::LinkingObjects:
                return m_schema[property().objectClassName];
            default:
                REALM_UNREACHABLE();
        }
    }

    bool has_links() const { return m_links.size(); }

    bool has_any_to_many_links() const {
        return std::any_of(begin(m_links), end(m_links),
                           [](RLMProperty *property) { return property.array; });
    }

    ColumnReference last_link_column() const {
        REALM_ASSERT(!m_links.empty());
        return {*m_query, *m_group, m_schema, m_links.back(), {m_links.begin(), m_links.end() - 1}};
    }

    ColumnReference column_ignoring_links(Query& query) const {
        return {query, *m_group, m_schema, m_property};
    }

private:
    template<typename Func>
    Table const& walk_link_chain(Func&& func) const
    {
        auto table = m_query->get_table().unchecked_ptr();
        for (const auto& link : m_links) {
            if (link.type != RLMPropertyTypeLinkingObjects) {
                auto index = table->get_column_key(link.columnName.UTF8String);
                func(*table, index, static_cast<PropertyType>(link.type));
                table = table->get_link_target(index).unchecked_ptr();
            }
            else {
                with_link_origin(link, [&](Table& link_origin_table, ColKey link_origin_column) {
                    func(link_origin_table, link_origin_column, static_cast<PropertyType>(link.type));
                    table = &link_origin_table;
                });
            }
        }
        return *table;
    }

    template<typename Func>
    auto with_link_origin(RLMProperty *prop, Func&& func) const
    {
        RLMObjectSchema *link_origin_schema = m_schema[prop.objectClassName];
        Table& link_origin_table = get_table(*m_group, link_origin_schema);
        NSString *column_name = link_origin_schema[prop.linkOriginPropertyName].columnName;
        auto link_origin_column = link_origin_table.get_column_key(column_name.UTF8String);
        return func(link_origin_table, link_origin_column);
    }

    std::vector<RLMProperty*> m_links;
    RLMProperty *m_property;
    RLMSchema *m_schema;
    Group *m_group;
    Query *m_query;
    ConstTableRef m_table;
    ColKey m_col;
};

bool is_valid_for_column(QueryMixed const& value, ColumnReference const& column) {
    return validate_value(value, column.column().is_list(), column.column(),
                          column.property().objectClassName.UTF8String);
}

class CollectionOperation {
public:
    enum Type {
        Count,
        Minimum,
        Maximum,
        Sum,
        Average,
    };

    CollectionOperation(Type type, ColumnReference link_column, util::Optional<ColumnReference> column)
        : m_type(type)
        , m_link_column(std::move(link_column))
        , m_column(std::move(column))
    {
        precondition(m_link_column.property().array,
                     "Collection operations can only be applied to a list properties.");

        switch (m_type) {
            case Count:
                precondition(!m_column, "Result of @count does not have any properties.");
                break;
            case Minimum:
            case Maximum:
            case Sum:
            case Average:
                precondition(m_column && property_type_is_numeric(m_column->type()),
                             "%1 can only be applied to a numeric property.", name_for_type(m_type));
                break;
        }
    }

    CollectionOperation(StringData operationName,
                        ColumnReference link_column,
                        util::Optional<ColumnReference> column = util::none)
    : CollectionOperation(type_for_name(operationName), std::move(link_column), std::move(column))
    {
    }

    Type type() const { return m_type; }
    const ColumnReference& link_column() const { return m_link_column; }
    const ColumnReference& column() const { return *m_column; }

    void validate_comparison(QueryMixed value) const {
        switch (m_type) {
            case Count:
            case Average:
                precondition(type_is_numeric(value.get_type()),
                             "%1 can only be compared with a numeric value.", name_for_type(m_type));
                break;
            case Minimum:
            case Maximum:
            case Sum:
                precondition(is_valid_for_column(value, *m_column),
                             "%1 on a property of type %2 cannot be compared with '%3'",
                             name_for_type(m_type), string_for_property_type(m_column->type()), value.description());
                break;
        }
    }

    void validate_comparison(const ColumnReference& column) const {
        switch (m_type) {
            case Count:
                precondition(property_type_is_numeric(column.type()),
                             "%1 can only be compared with a numeric value.", name_for_type(m_type));
                break;
            case Average:
            case Minimum:
            case Maximum:
            case Sum:
                precondition(property_type_is_numeric(column.type()),
                             "%1 on a property of type '%2' cannot be compared with property of type '%3'",
                                name_for_type(m_type), string_for_property_type(m_column->type()), string_for_property_type(column.type()));
                break;
        }
    }

private:
    static Type type_for_name(StringData name) {
        if (name == "@count")
            return Count;
        if (name == "@min")
            return Minimum;
        if (name == "@max")
            return Maximum;
        if (name == "@sum")
            return Sum;
        if (name == "@avg")
            return Average;
        precondition_failure("Unsupported collection operation '%1'", name);
    }

    static const char *name_for_type(Type type) {
        switch (type) {
            case Count: return "@count";
            case Minimum: return "@min";
            case Maximum: return "@max";
            case Sum: return "@sum";
            case Average: return "@avg";
        }
    }

    Type m_type;
    ColumnReference m_link_column;
    util::Optional<ColumnReference> m_column;
};

class QueryBuilder {
public:
    QueryBuilder(Query& query, Group& group, RLMSchema *schema)
    : m_query(query), m_group(group), m_schema(schema) { }

    void apply_predicate(Predicate predicate, RLMObjectSchema *objectSchema);


    void apply_collection_operator_expression(RLMObjectSchema *desc, StringData keyPath, QueryMixed value, Predicate);
    void apply_value_expression(RLMObjectSchema *desc, StringData keyPath, QueryMixed value, Predicate);
    void apply_column_expression(RLMObjectSchema *desc, StringData leftKeyPath, StringData rightKeyPath, Predicate);
    void apply_subquery_count_expression(RLMObjectSchema *objectSchema, PredicateExpression subqueryExpression,
                                         OperatorType operatorType, PredicateExpression right);
    void apply_function_subquery_expression(RLMObjectSchema *objectSchema, PredicateExpression functionExpression,
                                            OperatorType operatorType, PredicateExpression right);
    void apply_function_expression(RLMObjectSchema *objectSchema, PredicateExpression functionExpression,
                                   OperatorType operatorType, PredicateExpression right);


    template <typename A, typename B>
    void add_numeric_constraint(PropertyType datatype,
                                OperatorType operatorType,
                                A&& lhs, B&& rhs);

    template <typename A, typename B>
    void add_bool_constraint(PropertyType, OperatorType operatorType, A&& lhs, B&& rhs);

    void add_substring_constraint(null, Query condition);
    template<typename T>
    void add_substring_constraint(const T& value, Query condition);
    template<typename T>
    void add_substring_constraint(const Columns<T>& value, Query condition);

    template <typename T>
    void add_string_constraint(OperatorType operatorType,
                               ComparisonOptions predicateOptions,
                               Columns<String> &&column,
                               T value);

    void add_string_constraint(OperatorType operatorType,
                               ComparisonOptions predicateOptions,
                               StringData value,
                               Columns<String>&& column);

    template <typename L, typename R>
    void add_constraint(PropertyType type,
                        OperatorType operatorType,
                        ComparisonOptions predicateOptions,
                        L const& lhs, R const& rhs);
    template <typename... T>
    void do_add_constraint(PropertyType type, OperatorType operatorType,
                           ComparisonOptions predicateOptions, T&&... values);
    void do_add_constraint(PropertyType, OperatorType, ComparisonOptions, QueryMixed, realm::null);

    void add_between_constraint(const ColumnReference& column, QueryMixed value);

    void add_binary_constraint(OperatorType operatorType, const ColumnReference& column, BinaryData value);
    void add_binary_constraint(OperatorType operatorType, const ColumnReference& column, QueryMixed value);
    void add_binary_constraint(OperatorType operatorType, const ColumnReference& column, null);
    void add_binary_constraint(OperatorType operatorType, QueryMixed value, const ColumnReference& column);
    void add_binary_constraint(OperatorType, const ColumnReference&, const ColumnReference&);

    void add_link_constraint(OperatorType operatorType, const ColumnReference& column, QueryMixed const& obj);
    void add_link_constraint(OperatorType operatorType, const ColumnReference& column, realm::null);
    template<typename T>
    void add_link_constraint(OperatorType operatorType, T obj, const ColumnReference& column);
    void add_link_constraint(OperatorType, const ColumnReference&, const ColumnReference&);

    template <CollectionOperation::Type Operation, typename... T>
    void add_collection_operation_constraint(PropertyType propertyType, OperatorType operatorType, T... values);
    template <typename... T>
    void add_collection_operation_constraint(OperatorType operatorType,
                                             CollectionOperation collectionOperation, T... values);


    CollectionOperation collection_operation_from_key_path(RLMObjectSchema *desc, StringData keyPath);
    ColumnReference column_reference_from_key_path(RLMObjectSchema *objectSchema, StringData keyPath, bool isAggregate);

private:
    Query& m_query;
    Group& m_group;
    RLMSchema *m_schema;
};

// add a clause for numeric constraints based on operator type
template <typename A, typename B>
void QueryBuilder::add_numeric_constraint(PropertyType datatype,
                                          OperatorType operatorType,
                                          A&& lhs, B&& rhs)
{
    switch (operatorType) {
        case OperatorType::LessThan:
            m_query.and_query(lhs < rhs);
            break;
        case OperatorType::LessThanOrEqual:
            m_query.and_query(lhs <= rhs);
            break;
        case OperatorType::GreaterThan:
            m_query.and_query(lhs > rhs);
            break;
        case OperatorType::GreaterThanOrEqual:
            m_query.and_query(lhs >= rhs);
            break;
        case OperatorType::Equal:
            m_query.and_query(lhs == rhs);
            break;
        case OperatorType::NotEqual:
            m_query.and_query(lhs != rhs);
            break;
        default:
            precondition_failure("Operator '%1' not supported for type '%2'",
                                 operatorName(operatorType), string_for_property_type(datatype));
    }
}

template <typename A, typename B>
void QueryBuilder::add_bool_constraint(PropertyType datatype,
                                       OperatorType operatorType,
                                       A&& lhs, B&& rhs) {
    switch (operatorType) {
        case OperatorType::Equal:
            m_query.and_query(lhs == rhs);
            break;
        case OperatorType::NotEqual:
            m_query.and_query(lhs != rhs);
            break;
        default:
            precondition_failure("Operator '%1' not supported for type '%2'",
                                 operatorName(operatorType), string_for_property_type(datatype));
    }
}

void QueryBuilder::add_substring_constraint(null, Query) {
    // Foundation always returns false for substring operations with a RHS of null or "".
    m_query.and_query(std::unique_ptr<Expression>(new FalseExpression));
}

template<typename T>
void QueryBuilder::add_substring_constraint(const T& value, Query condition) {
    // Foundation always returns false for substring operations with a RHS of null or "".
    m_query.and_query(value.size()
                      ? std::move(condition)
                      : std::unique_ptr<Expression>(new FalseExpression));
}

template<typename T>
void QueryBuilder::add_substring_constraint(const Columns<T>& value, Query condition) {
    // Foundation always returns false for substring operations with a RHS of null or "".
    // We don't need to concern ourselves with the possibility of value traversing a link list
    // and producing multiple values per row as such expressions will have been rejected.
    m_query.and_query(const_cast<Columns<String>&>(value).size() != 0 && std::move(condition));
}

template <typename T>
void QueryBuilder::add_string_constraint(OperatorType operatorType,
                                         ComparisonOptions predicateOptions,
                                         Columns<String> &&column,
                                         T value) {
    bool caseSensitive = !is_set(predicateOptions, ComparisonOptions::CaseInsensitive);
    bool diacriticSensitive = !is_set(predicateOptions, ComparisonOptions::DiacriticInsensitive);

    if (diacriticSensitive) {
        switch (operatorType) {
            case OperatorType::BeginsWith:
                add_substring_constraint(value, column.begins_with(value, caseSensitive));
                break;
            case OperatorType::EndsWith:
                add_substring_constraint(value, column.ends_with(value, caseSensitive));
                break;
            case OperatorType::Contains:
                add_substring_constraint(value, column.contains(value, caseSensitive));
                break;
            case OperatorType::Equal:
                m_query.and_query(column.equal(value, caseSensitive));
                break;
            case OperatorType::NotEqual:
                m_query.and_query(column.not_equal(value, caseSensitive));
                break;
            case OperatorType::Like:
                m_query.and_query(column.like(value, caseSensitive));
                break;
            default:
                precondition_failure("Operator '%1' not supported for string type",
                                     operatorName(operatorType));
        }
        return;
    }

    auto as_subexpr = util::overload([](StringData value) { return make_subexpr<ConstantStringValue>(value); },
                                     [](const Columns<String>& c) { return c.clone(); });
    auto left = as_subexpr(column);
    auto right = as_subexpr(value);

    auto make_constraint = [&](auto comparator) {
        using Comparator = decltype(comparator);
        using CompareCS = Compare<typename Comparator::CaseSensitive, StringData>;
        using CompareCI = Compare<typename Comparator::CaseInsensitive, StringData>;
        if (caseSensitive) {
            return make_expression<CompareCS>(std::move(left), std::move(right));
        }
        else {
            return make_expression<CompareCI>(std::move(left), std::move(right));
        }
    };

    switch (operatorType) {
        case OperatorType::BeginsWith: {
            using C = ContainsSubstring<kCFCompareDiacriticInsensitive | kCFCompareAnchored>;
            add_substring_constraint(value, make_constraint(C{}));
            break;
        }
        case OperatorType::EndsWith: {
            using C = ContainsSubstring<kCFCompareDiacriticInsensitive | kCFCompareAnchored | kCFCompareBackwards>;
            add_substring_constraint(value, make_constraint(C{}));
            break;
        }
        case OperatorType::Contains: {
            using C = ContainsSubstring<kCFCompareDiacriticInsensitive>;
            add_substring_constraint(value, make_constraint(C{}));
            break;
        }
        case OperatorType::NotEqual:
            m_query.Not();
            REALM_FALLTHROUGH;
        case OperatorType::Equal:
            m_query.and_query(make_constraint(Equal<kCFCompareDiacriticInsensitive>{}));
            break;
        case OperatorType::Like:
            precondition_failure("Operator 'LIKE' not supported with diacritic-insensitive modifier.");
        default:
            precondition_failure("Operator '%1' not supported for string type", operatorName(operatorType));
    }
}

void QueryBuilder::add_string_constraint(OperatorType operatorType,
                                         ComparisonOptions predicateOptions,
                                         StringData value,
                                         Columns<String>&& column) {
    switch (operatorType) {
        case OperatorType::Equal:
        case OperatorType::NotEqual:
            add_string_constraint(operatorType, predicateOptions, std::move(column), value);
            break;
        default:
            precondition_failure("Operator '%1' is not supported for string type with key path on right side of operator",
                                 operatorName(operatorType));
    }
}

void QueryBuilder::add_between_constraint(const ColumnReference& column, QueryMixed value) {
    if (column.has_any_to_many_links()) {
        auto link_column = column.last_link_column();
        Query subquery = get_table(m_group, link_column.link_target_object_schema()).where();
        QueryBuilder(subquery, m_group, m_schema).add_between_constraint(column.column_ignoring_links(subquery), value);

        m_query.and_query(link_column.resolve<Link>(std::move(subquery)).count() > 0);
        return;
    }

    precondition(value.get_type() == type_LinkList, "BETWEEN operation requires an array with exactly two values");
    auto& array = value.get_array();
    precondition(array.size() == 2, "BETWEEN operation requires an array with exactly two values");

    auto prop = column.property();
    precondition(is_valid_for_column(array[0], column) && is_valid_for_column(array[1], column),
                 "array objects must be of type '%1' for BETWEEN operations",
                 string_for_property_type(static_cast<PropertyType>(prop.type)));

    PropertyType type = column.type();

    m_query.group();
    add_constraint(type, OperatorType::GreaterThanOrEqual, ComparisonOptions::None, column, array[0]);
    add_constraint(type, OperatorType::LessThanOrEqual, ComparisonOptions::None, column, array[1]);
    m_query.end_group();
}

void QueryBuilder::add_binary_constraint(OperatorType operatorType,
                                         const ColumnReference& column,
                                         BinaryData value) {
    precondition(!column.has_links(), "data properties cannot be queried over an object link.");

    auto index = column.column();
    Query query = m_query.get_table()->where();

    switch (operatorType) {
        case OperatorType::BeginsWith:
            add_substring_constraint(value, query.begins_with(index, value));
            break;
        case OperatorType::EndsWith:
            add_substring_constraint(value, query.ends_with(index, value));
            break;
        case OperatorType::Contains:
            add_substring_constraint(value, query.contains(index, value));
            break;
        case OperatorType::Equal:
            m_query.equal(index, value);
            break;
        case OperatorType::NotEqual:
            m_query.not_equal(index, value);
            break;
        default:
            precondition_failure("Operator '%1' not supported for binary type", operatorName(operatorType));
    }
}

void QueryBuilder::add_binary_constraint(OperatorType operatorType, const ColumnReference& column, QueryMixed value) {
    add_binary_constraint(operatorType, column, value.get<BinaryData>());
}

void QueryBuilder::add_binary_constraint(OperatorType operatorType, const ColumnReference& column, null) {
    add_binary_constraint(operatorType, column, BinaryData());
}

void QueryBuilder::add_binary_constraint(OperatorType operatorType, QueryMixed value, const ColumnReference& column) {
    precondition(operatorType == OperatorType::Equal || operatorType == OperatorType::NotEqual,
                 "Operator '%1' is not supported for binary type with key path on right side of operator",
                 operatorName(operatorType));
    add_binary_constraint(operatorType, column, value);
}

void QueryBuilder::add_binary_constraint(OperatorType, const ColumnReference&, const ColumnReference&) {
    precondition_failure("Comparisons between two data properties are not supported");
}

void QueryBuilder::add_link_constraint(OperatorType operatorType,
                                       const ColumnReference& column, QueryMixed const& value) {
    auto [object_type, obj_key] = value.get<std::pair<StringData, ObjKey>>();
    auto table = column.group().get_table(std::string("class_") + object_type.data());
    if (!table->is_valid(obj_key)) {
        // Unmanaged or deleted objects are not equal to any managed objects.
        // For arrays this effectively checks if there are any objects in the
        // array, while for links it's just always constant true or false
        // (for != and = respectively).
        if (column.property().array) {
            add_bool_constraint(PropertyType::Object, operatorType, column.resolve<Link>(), null());
        }
        else if (operatorType == OperatorType::Equal) {
            m_query.and_query(std::unique_ptr<Expression>(new FalseExpression));
        }
        else {
            m_query.and_query(std::unique_ptr<Expression>(new TrueExpression));
        }
    }
    else {
        add_bool_constraint(PropertyType::Object, operatorType, column.resolve<Link>(), table->get_object(obj_key));
    }
}

void QueryBuilder::add_link_constraint(OperatorType operatorType,
                                       const ColumnReference& column,
                                       realm::null) {
    add_bool_constraint(PropertyType::Object, operatorType, column.resolve<Link>(), null());
}

template<typename T>
void QueryBuilder::add_link_constraint(OperatorType operatorType, T obj, const ColumnReference& column) {
    // Link constraints only support the equal-to and not-equal-to operators. The order of operands
    // is not important for those comparisons so we can delegate to the other implementation.
    add_link_constraint(operatorType, column, obj);
}

void QueryBuilder::add_link_constraint(OperatorType, const ColumnReference&, const ColumnReference&) {
    // This is not actually reachable as this case is caught earlier, but this
    // overload is needed for the code to compile
    precondition_failure("Comparisons between two list properties are not supported");
}

class QueryArgumentValue {
public:
    template <typename RequestedType>
    RequestedType get();
};

template <typename RequestedType>
RequestedType convert(QueryMixed value) {
    return value.get<RequestedType>();
}

template <>
ObjectId convert<ObjectId>(QueryMixed value) {
    switch (value.get_type()) {
        case type_ObjectId:
            return value.get<ObjectId>();
        case type_String:
            return ObjectId(value.get<String>().data());
        default:
            precondition_failure("Cannot convert value '%1' of type '%2' to object id", "foo", "bar");
    }
}

template <typename>
realm::null value_of_type(realm::null) {
    return realm::null();
}

template <typename RequestedType>
auto value_of_type(QueryMixed value) {
    return ::convert<RequestedType>(value);
}

template <typename RequestedType>
auto value_of_type(const ColumnReference& column) {
    return column.resolve<RequestedType>();
}


template <typename... T>
void QueryBuilder::do_add_constraint(PropertyType type, OperatorType operatorType,
                                     ComparisonOptions predicateOptions, T&&... values)
{
    static_assert(sizeof...(T) == 2, "do_add_constraint accepts only two values as arguments");

    switch (type) {
        case PropertyType::Bool:
            return add_bool_constraint(type, operatorType, value_of_type<bool>(values)...);
        case PropertyType::ObjectId:
            return add_bool_constraint(type, operatorType, value_of_type<ObjectId>(values)...);
        case PropertyType::Date:
            return add_numeric_constraint(type, operatorType, value_of_type<Timestamp>(values)...);
        case PropertyType::Double:
            return add_numeric_constraint(type, operatorType, value_of_type<Double>(values)...);
        case PropertyType::Float:
            return add_numeric_constraint(type, operatorType, value_of_type<Float>(values)...);
        case PropertyType::Int:
            return add_numeric_constraint(type, operatorType, value_of_type<Int>(values)...);
        case PropertyType::Decimal:
            return add_numeric_constraint(type, operatorType, value_of_type<Decimal128>(values)...);
        case PropertyType::String:
            return add_string_constraint(operatorType, predicateOptions, value_of_type<String>(values)...);
        case PropertyType::Data:
            return add_binary_constraint(operatorType, values...);
        case PropertyType::Object:
        case PropertyType::LinkingObjects:
            return add_link_constraint(operatorType, values...);
        default:
            precondition_failure("Object type %1 not supported", string_for_property_type(type));
    }
}

void QueryBuilder::do_add_constraint(PropertyType, OperatorType, ComparisonOptions, QueryMixed, realm::null)
{
    // This is not actually reachable as this case is caught earlier, but this
    // overload is needed for the code to compile
    precondition_failure("Predicate expressions must compare a keypath and another keypath or a constant value");
}

bool is_nsnull(QueryMixed const& v) {
    return v.is_null();
}

template<typename T>
bool is_nsnull(T) {
    return false;
}

template <typename L, typename R>
void QueryBuilder::add_constraint(PropertyType type, OperatorType operatorType,
                                  ComparisonOptions predicateOptions, L const& lhs, R const& rhs)
{
    // The expression operators are only overloaded for realm::null on the rhs
    precondition(!is_nsnull(lhs), "Nil is only supported on the right side of operators");

    if (is_nsnull(rhs)) {
        do_add_constraint(type, operatorType, predicateOptions, lhs, realm::null());
    }
    else {
        do_add_constraint(type, operatorType, predicateOptions, lhs, rhs);
    }
}

struct KeyPath {
    std::vector<RLMProperty *> links;
    RLMProperty *property;
    bool containsToManyRelationship;
};

KeyPath key_path_from_string(RLMSchema *schema, RLMObjectSchema *objectSchema, StringData keyPath)
{
    RLMProperty *property;
    std::vector<RLMProperty *> links;

    bool keyPathContainsToManyRelationship = false;

    auto start = keyPath.data(), end = keyPath.data() + keyPath.size();
    while (start != end) {
        auto dot = std::find(start, end, '.');
        auto propertyName = [[NSString alloc] initWithBytes:start length:dot - start
                                                   encoding:NSUTF8StringEncoding];
        property = objectSchema[propertyName];
        precondition(property, "Property '%1' not found in object of type '%2'",
                     propertyName.UTF8String, objectSchema.className.UTF8String);

        if (property.array)
            keyPathContainsToManyRelationship = true;

        if (dot != end) {
            precondition(property.type == RLMPropertyTypeObject || property.type == RLMPropertyTypeLinkingObjects,
                            "Property '%1' is not a link in object of type '%2'",
                            propertyName.UTF8String, objectSchema.className.UTF8String);

            links.push_back(property);
            REALM_ASSERT(property.objectClassName);
            objectSchema = schema[property.objectClassName];
        }

        start = dot == end ? end : dot + 1;
    }

    return {std::move(links), property, keyPathContainsToManyRelationship};
}

ColumnReference QueryBuilder::column_reference_from_key_path(RLMObjectSchema *objectSchema,
                                                             StringData keyPathString, bool isAggregate)
{
    auto keyPath = key_path_from_string(m_schema, objectSchema, keyPathString);

    if (isAggregate && !keyPath.containsToManyRelationship) {
        precondition_failure("Aggregate operations can only be used on key paths that include an array property");
    } else if (!isAggregate && keyPath.containsToManyRelationship) {
        precondition_failure("Key paths that include an array property must use aggregate operations");
    }

    return ColumnReference(m_query, m_group, m_schema, keyPath.property, std::move(keyPath.links));
}

void validate_property_value(const ColumnReference& column,
                             QueryMixed const& value,
                             const char *err,
                             __unsafe_unretained RLMObjectSchema *const objectSchema,
                             StringData keyPath) {
    RLMProperty *prop = column.property();
    if (prop.type == RLMPropertyTypeLinkingObjects) {
        precondition(!value.is_null() && value.get_type() == type_Link && value.get<std::pair<StringData, ObjKey>>().first == prop.objectClassName.UTF8String,
                     err, prop.objectClassName.UTF8String,
                     keyPath, objectSchema.className.UTF8String, value.description());
    }
    precondition(is_valid_for_column(value, column),
                 err, string_for_property_type(static_cast<PropertyType>(prop.type)),
                 keyPath, objectSchema.className.UTF8String, value.description());
//    if (RLMObjectBase *obj = RLMDynamicCast<RLMObjectBase>(value)) {
//        precondition(!obj->_row.is_valid() || &column.group() == &obj->_realm.group,
//                     "Object must be from the Realm being queried");
//    }
}

template <typename RequestedType, CollectionOperation::Type OperationType>
struct ValueOfTypeWithCollectionOperationHelper;

template <>
struct ValueOfTypeWithCollectionOperationHelper<Int, CollectionOperation::Count> {
    static auto convert(const CollectionOperation& operation)
    {
        assert(operation.type() == CollectionOperation::Count);
        return operation.link_column().resolve<Link>().count();
    }
};

#define VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER(OperationType, function) \
template <typename T> \
struct ValueOfTypeWithCollectionOperationHelper<T, OperationType> { \
    static auto convert(const CollectionOperation& operation) \
    { \
        REALM_ASSERT(operation.type() == OperationType); \
        auto targetColumn = operation.link_column().resolve<Link>().template column<T>(operation.column().column()); \
        return targetColumn.function(); \
    } \
} \

VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER(CollectionOperation::Minimum, min);
VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER(CollectionOperation::Maximum, max);
VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER(CollectionOperation::Sum, sum);
VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER(CollectionOperation::Average, average);
#undef VALUE_OF_TYPE_WITH_COLLECTION_OPERATOR_HELPER

template <typename Requested, CollectionOperation::Type OperationType, typename T>
auto value_of_type_with_collection_operation(T&& value) {
    return value_of_type<Requested>(std::forward<T>(value));
}

template <typename Requested, CollectionOperation::Type OperationType>
auto value_of_type_with_collection_operation(CollectionOperation operation) {
    using helper = ValueOfTypeWithCollectionOperationHelper<Requested, OperationType>;
    return helper::convert(operation);
}

template <CollectionOperation::Type Operation, typename... T>
void QueryBuilder::add_collection_operation_constraint(PropertyType propertyType, OperatorType operatorType, T... values)
{
    switch (propertyType) {
        case PropertyType::Int:
            add_numeric_constraint(propertyType, operatorType, value_of_type_with_collection_operation<Int, Operation>(values)...);
            break;
        case PropertyType::Float:
            add_numeric_constraint(propertyType, operatorType, value_of_type_with_collection_operation<Float, Operation>(values)...);
            break;
        case PropertyType::Double:
            add_numeric_constraint(propertyType, operatorType, value_of_type_with_collection_operation<Double, Operation>(values)...);
            break;
        case PropertyType::Decimal:
            add_numeric_constraint(propertyType, operatorType, value_of_type_with_collection_operation<Decimal128, Operation>(values)...);
            break;
        default:
            REALM_ASSERT(false && "Only numeric property types should hit this path.");
    }
}

template <typename... T>
void QueryBuilder::add_collection_operation_constraint(OperatorType operatorType,
                                                  CollectionOperation collectionOperation, T... values)
{
    static_assert(sizeof...(T) == 2, "add_collection_operation_constraint accepts only two values as arguments");

    switch (collectionOperation.type()) {
        case CollectionOperation::Count:
            add_numeric_constraint(PropertyType::Int, operatorType,
                                   value_of_type_with_collection_operation<Int, CollectionOperation::Count>(values)...);
            break;
        case CollectionOperation::Minimum:
            add_collection_operation_constraint<CollectionOperation::Minimum>(collectionOperation.column().type(), operatorType, values...);
            break;
        case CollectionOperation::Maximum:
            add_collection_operation_constraint<CollectionOperation::Maximum>(collectionOperation.column().type(), operatorType, values...);
            break;
        case CollectionOperation::Sum:
            add_collection_operation_constraint<CollectionOperation::Sum>(collectionOperation.column().type(), operatorType, values...);
            break;
        case CollectionOperation::Average:
            add_collection_operation_constraint<CollectionOperation::Average>(collectionOperation.column().type(), operatorType, values...);
            break;
    }
}

bool key_path_contains_collection_operator(StringData keyPath) {
    return keyPath.contains("@");
}

StringData get_collection_operation_name_from_key_path(StringData keyPath, StringData *leadingKeyPath,
                                                       StringData *trailingKey) {
    auto start = keyPath.data();
    auto end = keyPath.data() + keyPath.size();
    auto at = std::find(start, end, '@');
    precondition(at != start && at != end && at + 1 != end && *(at - 1) == '.',
                 "Invalid key path '%1'", keyPath);

    *leadingKeyPath = keyPath.substr(0, at - start - 1);

    auto dot = std::find(at + 1, end, '.');
    if (dot == end) {
        *trailingKey = nullptr;
        return keyPath.substr(at - start);
    }
    *trailingKey = keyPath.substr(dot + 1 - start);
    return keyPath.substr(at - start, dot - at);
}

CollectionOperation QueryBuilder::collection_operation_from_key_path(RLMObjectSchema *desc, StringData keyPath) {
    StringData leadingKeyPath;
    StringData trailingKey;
    StringData collectionOperationName = get_collection_operation_name_from_key_path(keyPath, &leadingKeyPath, &trailingKey);

    ColumnReference linkColumn = column_reference_from_key_path(desc, leadingKeyPath, true);
    util::Optional<ColumnReference> column;
    if (trailingKey) {
        precondition(!trailingKey.contains("."),
                     "Right side of collection operator may only have a single level key");
        column = column_reference_from_key_path(desc, util::format("%1.%2", leadingKeyPath, trailingKey), true);
    }

    return {collectionOperationName, std::move(linkColumn), std::move(column)};
}

void QueryBuilder::apply_collection_operator_expression(RLMObjectSchema *desc,
                                                        StringData keyPath, QueryMixed value,
                                                        Predicate pred) {
    CollectionOperation operation = collection_operation_from_key_path(desc, keyPath);
    operation.validate_comparison(value);

    if (pred.left().type() == PredicateExpression::Type::KeyPath) {
        add_collection_operation_constraint(pred.operator_type(), operation, operation, value);
    } else {
        add_collection_operation_constraint(pred.operator_type(), operation, value, operation);
    }
}

void QueryBuilder::apply_value_expression(RLMObjectSchema *desc,
                                          StringData keyPath, realm::QueryMixed value,
                                          Predicate pred)
{
    if (key_path_contains_collection_operator(keyPath)) {
        apply_collection_operator_expression(desc, keyPath, value, pred);
        return;
    }

    bool isAny = pred.modifier() == Predicate::Modifier::Any;
    ColumnReference column = column_reference_from_key_path(desc, keyPath, isAny);

    // check to see if this is a between query
    if (pred.operator_type() == OperatorType::Between) {
        add_between_constraint(std::move(column), value);
        return;
    }

    // turn "key.path IN collection" into ored together ==. "collection IN key.path" is handled elsewhere.
    if (pred.operator_type() == OperatorType::In) {
        precondition(value.get_type() == type_LinkList, "IN clause requires an array of items");

        m_query.group();
        bool first = true;
        for (auto& item : value.get_array()) {
            if (!first) {
                m_query.Or();
            }
            first = false;

            validate_property_value(column, item,
                                    "Expected object of type '%1' in IN clause for property '%3.%2', but received: %4", desc, keyPath);
            add_constraint(column.type(), OperatorType::Equal, pred.options(), column, item);
        }

        if (first) {
            // Queries can't be empty, so if there's zero things in the OR group
            // validation will fail. Work around this by adding an expression which
            // will never find any rows in a table.
            m_query.and_query(std::unique_ptr<Expression>(new FalseExpression));
        }

        m_query.end_group();
        return;
    }

    validate_property_value(column, value,
                            "Expected object of type '%1' for property '%3.%2', but received: %4", desc, keyPath);
    if (pred.left().type() == PredicateExpression::Type::KeyPath) {
        add_constraint(column.type(), pred.operator_type(), pred.options(), std::move(column), value);
    } else {
        add_constraint(column.type(), pred.operator_type(), pred.options(), value, std::move(column));
    }
}

void QueryBuilder::apply_column_expression(RLMObjectSchema *desc,
                                           StringData leftKeyPath, StringData rightKeyPath,
                                           Predicate predicate)
{
    bool left_key_path_contains_collection_operator = key_path_contains_collection_operator(leftKeyPath);
    bool right_key_path_contains_collection_operator = key_path_contains_collection_operator(rightKeyPath);
    precondition(!left_key_path_contains_collection_operator || !right_key_path_contains_collection_operator,
                 "Key paths including aggregate operations cannot be compared with other aggregate operations.");

    if (left_key_path_contains_collection_operator) {
        CollectionOperation left = collection_operation_from_key_path(desc, leftKeyPath);
        ColumnReference right = column_reference_from_key_path(desc, rightKeyPath, false);
        left.validate_comparison(right);
        add_collection_operation_constraint(predicate.operator_type(), left, left, std::move(right));
        return;
    }
    if (right_key_path_contains_collection_operator) {
        ColumnReference left = column_reference_from_key_path(desc, leftKeyPath, false);
        CollectionOperation right = collection_operation_from_key_path(desc, rightKeyPath);
        right.validate_comparison(left);
        add_collection_operation_constraint(predicate.operator_type(), right, std::move(left), right);
        return;
    }

    bool isAny = false;
    ColumnReference left = column_reference_from_key_path(desc, leftKeyPath, isAny);
    ColumnReference right = column_reference_from_key_path(desc, rightKeyPath, isAny);

    // NOTE: It's assumed that column type must match and no automatic type conversion is supported.
    precondition(left.type() == right.type(),
                 "Comparison between '%1' and '%2' properties is not supported",
                 string_for_property_type(left.type()),
                 string_for_property_type(right.type()));

    // TODO: Should we handle special case where left row is the same as right row (tautology)
    add_constraint(left.type(), predicate.operator_type(), predicate.options(),
                   std::move(left), std::move(right));
}

void QueryBuilder::apply_subquery_count_expression(RLMObjectSchema *objectSchema,
                                                   PredicateExpression subqueryExpression,
                                                   OperatorType operatorType,
                                                   PredicateExpression right) {
    precondition(right.type() == PredicateExpression::Type::ConstantValue && right.value().get_type() == type_Int,
                 "SUBQUERY(...).@count is only supported when compared with a constant integer.");
    int64_t value = right.value().get<int64_t>();

    ColumnReference collectionColumn = column_reference_from_key_path(objectSchema, subqueryExpression.key_path(), true);
    RLMObjectSchema *collectionMemberObjectSchema = m_schema[collectionColumn.property().objectClassName];

    auto subquery = get_table(m_group, collectionMemberObjectSchema).where();
    @autoreleasepool {
        QueryBuilder(subquery, m_group, m_schema).apply_predicate(subqueryExpression.predicate(), collectionMemberObjectSchema);
    }
    std::string validateMessage = subquery.validate();
    precondition(validateMessage.empty(), "%1", validateMessage);

    add_numeric_constraint(PropertyType::Int, operatorType,
                           collectionColumn.resolve<Link>(std::move(subquery)).count(), value);
}

void QueryBuilder::apply_function_subquery_expression(RLMObjectSchema *objectSchema, PredicateExpression functionExpression,
                                                      OperatorType operatorType, PredicateExpression right) {
    precondition(functionExpression.function_name() == "valueForKeyPath:" /*&& functionExpression.arguments.count != 1*/,
                 "The '%1' function is not supported on the result of a SUBQUERY.", functionExpression.function_name());

    auto keyPathExpression = functionExpression.argument();
    precondition(keyPathExpression.key_path() == "@count",
                 "SUBQUERY is only supported when immediately followed by .@count that is compared with a constant number.");
    apply_subquery_count_expression(objectSchema, functionExpression.operand(), operatorType, right);
}

void QueryBuilder::apply_function_expression(RLMObjectSchema *objectSchema, PredicateExpression functionExpression,
                                             OperatorType operatorType, PredicateExpression right) {
    precondition(functionExpression.operand().type() == PredicateExpression::Type::Subquery,
                 "The '%1' function is not supported.", functionExpression.function_name());
    apply_function_subquery_expression(objectSchema, functionExpression, operatorType, right);
}

void QueryBuilder::apply_predicate(Predicate predicate, RLMObjectSchema *objectSchema)
{
    switch (predicate.predicate_type()) {
        case Predicate::Kind::Compound: {
            switch (predicate.compoundType()) {
                case CompoundPredicateType::And: {
                    auto subpredicates = predicate.subpredicates();
                    if (subpredicates.empty()) {
                        // Empty AND group is true
                        m_query.and_query(std::unique_ptr<Expression>(new TrueExpression));
                    }
                    else {
                        // Add all of the subpredicates.
                        m_query.group();
                        for (auto& sub : subpredicates)
                            apply_predicate(sub, objectSchema);
                        m_query.end_group();
                    }
                    return;
                }

                case CompoundPredicateType::Or: {
                    auto subpredicates = predicate.subpredicates();
                    if (subpredicates.empty()) {
                        // Empty OR group is false
                        m_query.and_query(std::unique_ptr<Expression>(new FalseExpression));
                    }
                    else {
                        // Add all of the subpredicates.
                        m_query.group();
                        bool first = true;
                        for (auto& sub : subpredicates) {
                            if (!first) {
                                m_query.Or();
                            }
                            first = false;
                            apply_predicate(sub, objectSchema);
                        }
                        m_query.end_group();
                    }
                    return;
                }

                case CompoundPredicateType::Not:
                    m_query.Not();
                    apply_predicate(predicate.subpredicates().front(), objectSchema);
                    return;

                default:
                    precondition_failure("Only the AND, OR and NOT compound predicate types are supported");
            }
            break;
        }
        case Predicate::Kind::Comparison: {
            precondition(predicate.modifier() != Predicate::Modifier::All,
                         "The ALL modifier is not supported");

            auto type = predicate.operator_type();
            auto exp1Type = predicate.left().type();
            auto exp2Type = predicate.right().type();

            if (predicate.modifier() == Predicate::Modifier::Any) {
                precondition(exp1Type == PredicateExpression::Type::KeyPath && exp2Type == PredicateExpression::Type::ConstantValue,
                             "Predicate with ANY modifier must compare a KeyPath with RLMArray with a value");
            }

            if (type == OperatorType::Between || type == OperatorType::In) {
                // Inserting an array via %@ gives Expression::Type::ConstantValue, but including it directly gives NSAggregateExpressionType
                if (exp1Type == PredicateExpression::Type::KeyPath && (exp2Type == PredicateExpression::Type::Aggregate || exp2Type == PredicateExpression::Type::ConstantValue)) {
                    // "key.path IN %@", "key.path IN {…}", "key.path BETWEEN %@", or "key.path BETWEEN {…}".
                    exp2Type = PredicateExpression::Type::ConstantValue;
                }
                else if (type == OperatorType::In && exp1Type == PredicateExpression::Type::ConstantValue && exp2Type == PredicateExpression::Type::KeyPath) {
                    // "%@ IN key.path" is equivalent to "ANY key.path IN %@". Rewrite the former into the latter.
                    predicate = Predicate(predicate.right(), predicate.left(),
                                          Predicate::Modifier::Any, OperatorType::Equal, ComparisonOptions::None);
                    exp1Type = PredicateExpression::Type::KeyPath;
                    exp2Type = PredicateExpression::Type::ConstantValue;
                }
                else {
                    precondition(type != OperatorType::Between,
                                 "Predicate with BETWEEN operator must compare a KeyPath with an aggregate with two values");
                    precondition(type != OperatorType::In,
                                 "Predicate with IN operator must compare a KeyPath with an aggregate");
                }
            }

            if (exp1Type == PredicateExpression::Type::KeyPath && exp2Type == PredicateExpression::Type::KeyPath) {
                // both expression are KeyPaths
                apply_column_expression(objectSchema, predicate.left().key_path(), predicate.right().key_path(), predicate);
            }
            else if (exp1Type == PredicateExpression::Type::KeyPath && exp2Type == PredicateExpression::Type::ConstantValue) {
                // comparing keypath to value
                apply_value_expression(objectSchema, predicate.left().key_path(), predicate.right().value(), predicate);
            }
            else if (exp1Type == PredicateExpression::Type::ConstantValue && exp2Type == PredicateExpression::Type::KeyPath) {
                // comparing value to keypath
                apply_value_expression(objectSchema, predicate.right().key_path(), predicate.left().value(), predicate);
            }
            else if (exp1Type == PredicateExpression::Type::Function) {
                apply_function_expression(objectSchema, predicate.left(), type, predicate.right());
            }
            else {
                // The subquery expressions that we support are handled by the NSFunctionExpressionType case above.
                precondition(exp1Type != PredicateExpression::Type::Subquery,
                             "SUBQUERY is only supported when immediately followed by .@count");
                precondition_failure("Predicate expressions must compare a keypath and another keypath or a constant value");
            }
            break;
        }
        case Predicate::Kind::True:
            m_query.and_query(std::unique_ptr<Expression>(new TrueExpression));
            break;
        case Predicate::Kind::False:
            m_query.and_query(std::unique_ptr<Expression>(new FalseExpression));
            break;
        default:
            precondition_failure("Only support compound, comparison, and constant predicates");
            break;
    }
}
} // namespace

realm::Query RLMPredicateToQuery(Predicate predicate, RLMObjectSchema *objectSchema,
                                 RLMSchema *schema, Group &group)
{
    auto query = get_table(group, objectSchema).where();
    QueryBuilder(query, group, schema).apply_predicate(predicate, objectSchema);
    std::string validateMessage = query.validate();
    precondition(validateMessage.empty(), "%1", validateMessage);
    return query;
}

