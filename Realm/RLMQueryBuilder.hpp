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

#import <Foundation/Foundation.h>

#import <vector>
#import <realm/mixed.hpp>
#import <realm/string_data.hpp>
#import <realm/util/overload.hpp>

namespace realm {
class Group;
class Query;
class SortDescriptor;
class ObjectSchema;
struct Property;
}

namespace realm {
class QueryMixed {
public:
    QueryMixed() noexcept {}
    QueryMixed(util::None) noexcept : QueryMixed() { }

    QueryMixed(int v) noexcept : m_type(type_Int), int_val(v) {}
    QueryMixed(int64_t v) noexcept : m_type(type_Int), int_val(v) {}
    QueryMixed(bool v) noexcept : m_type(type_Bool), bool_val(v) {}
    QueryMixed(float v) noexcept : m_type(type_Float), float_val(v) {}
    QueryMixed(double v) noexcept : m_type(type_Double), double_val(v) {}
    QueryMixed(StringData v) noexcept;
    QueryMixed(BinaryData v) noexcept;
    QueryMixed(Timestamp v) noexcept;
    QueryMixed(Decimal128 v) noexcept;
    QueryMixed(ObjectId v) noexcept : m_type(type_ObjectId), id_val(v) {}
    QueryMixed(StringData t, ObjKey o) noexcept : m_type(type_Link), obj_val({t, o}) {}
    QueryMixed(std::vector<QueryMixed> v) noexcept : m_type(type_LinkList), array_val(std::move(v)) {}

    // These are shortcuts for QueryMixed(StringData(c_str)), and are
    // needed to avoid unwanted implicit conversion of char* to bool.
    QueryMixed(char* c_str) noexcept
    : QueryMixed(StringData(c_str))
    {
    }
    QueryMixed(const char* c_str) noexcept
    : QueryMixed(StringData(c_str))
    {
    }
    QueryMixed(const std::string& s) noexcept
    : QueryMixed(StringData(s))
    {
    }

    ~QueryMixed() noexcept
    {
        destroy();
    }

    QueryMixed(QueryMixed const&);
    QueryMixed& operator=(QueryMixed const&);
    QueryMixed(QueryMixed&&) noexcept;
    QueryMixed& operator=(QueryMixed&&) noexcept;

    DataType get_type() const noexcept
    {
        REALM_ASSERT(m_type != type_Null);
        return DataType(m_type);
    }

    template <class T>
    T get() const noexcept;

    std::vector<QueryMixed> const& get_array() const noexcept;

    bool is_null() const noexcept { return m_type == type_Null; }
    std::string description() const;

private:
    enum {
        type_Null = uint32_t(-1)
    };
    union {
        int64_t int_val;
        bool bool_val;
        float float_val;
        double double_val;
        StringData string_val;
        BinaryData binary_val;
        Timestamp date_val;
        ObjectId id_val;
        Decimal128 decimal_val;
        std::pair<StringData, ObjKey> obj_val;
        std::vector<QueryMixed> array_val;
    };
    uint32_t m_type = type_Null;

    template<typename Func>
    auto switch_on_type(Func&& func) const
    {
        switch (m_type) {
            case type_Null: return func(util::none);
            case type_Int: return func(&QueryMixed::int_val);
            case type_Float: return func(&QueryMixed::float_val);
            case type_Double: return func(&QueryMixed::double_val);
            case type_String: return func(&QueryMixed::string_val);
            case type_Binary: return func(&QueryMixed::binary_val);
            case type_Timestamp: return func(&QueryMixed::date_val);
            case type_ObjectId: return func(&QueryMixed::id_val);
            case type_Decimal: return func(&QueryMixed::decimal_val);
            case type_Link: return func(&QueryMixed::obj_val);
            case type_LinkList: return func(&QueryMixed::array_val);
        }
        REALM_COMPILER_HINT_UNREACHABLE();
    }

    template<typename Right>
    void assign(Right&& r, bool move)
    {
        if (&r == this) {
            return;
        }
        m_type = r.m_type;
        switch_on_type(util::overload([](util::None) {}, [&](auto ptr) {
            using T = std::remove_reference_t<decltype(this->*ptr)>;
            if (move)
                new(&(this->*ptr)) T(std::move(r.*ptr));
            else
                new(&(this->*ptr)) T(r.*ptr);
        }));
    }

    void destroy() {
        if (m_type == type_LinkList) {
            array_val.~vector<QueryMixed>();
        }
    }
};

inline std::string QueryMixed::description() const
{
    return switch_on_type(util::overload([](util::None) -> std::string {
        return "<null>";
    }, [this](BinaryData QueryMixed::*) -> std::string {
        return "binary";
    }, [this](Timestamp QueryMixed::*) -> std::string {
        return "timestamp";
    }, [this](ObjectId QueryMixed::*) -> std::string {
        return id_val.to_string();
    }, [this](Decimal128 QueryMixed::*) -> std::string {
        return id_val.to_string();
    }, [this](std::pair<StringData, ObjKey> QueryMixed::*) -> std::string {
        return obj_val.first;
    }, [this](std::vector<QueryMixed> QueryMixed::*) -> std::string {
        return "array";
    }, [this](auto ptr) {
        return util::to_string(this->*ptr);
    }));
}

inline QueryMixed::QueryMixed(QueryMixed const& q)
{
    assign(q, false);
}
inline QueryMixed::QueryMixed(QueryMixed&& q) noexcept
{
    assign(q, true);
}
inline QueryMixed& QueryMixed::operator=(QueryMixed const& q)
{
    assign(q, false);
    return *this;
}
inline QueryMixed& QueryMixed::operator=(QueryMixed&& q) noexcept
{
    assign(q, true);
    return *this;
}

inline QueryMixed::QueryMixed(StringData v) noexcept
{
    if (!v.is_null()) {
        m_type = type_String;
        string_val = v;
    }
}

inline QueryMixed::QueryMixed(BinaryData v) noexcept
{
    if (!v.is_null()) {
        m_type = type_Binary;
        binary_val = v;
    }
}

inline QueryMixed::QueryMixed(Timestamp v) noexcept
{
    if (!v.is_null()) {
        m_type = type_Timestamp;
        date_val = v;
    }
}

inline QueryMixed::QueryMixed(Decimal128 v) noexcept
{
    if (!v.is_null()) {
        m_type = type_Decimal;
        decimal_val = v;
    }
}

template <>
inline int64_t QueryMixed::get<int64_t>() const noexcept
{
    switch (m_type) {
        case type_Bool: return bool_val;
        case type_Int: return int_val;
        case type_Float: return float_val;
        case type_Double: return double_val;
    }
    REALM_UNREACHABLE();
}

template <>
inline bool QueryMixed::get<bool>() const noexcept
{
    switch (m_type) {
        case type_Bool: return bool_val;
        case type_Int: return int_val != 0;
    }
    REALM_UNREACHABLE();
}

template <>
inline float QueryMixed::get<float>() const noexcept
{
    switch (m_type) {
        case type_Bool: return bool_val;
        case type_Int: return int_val;
        case type_Float: return float_val;
        case type_Double: return double_val;
    }
    REALM_UNREACHABLE();
}

template <>
inline double QueryMixed::get<double>() const noexcept
{
    switch (m_type) {
        case type_Bool: return bool_val;
        case type_Int: return int_val;
        case type_Float: return float_val;
        case type_Double: return double_val;
    }
    REALM_UNREACHABLE();
}

template <>
inline StringData QueryMixed::get<StringData>() const noexcept
{
    REALM_ASSERT(get_type() == type_String);
    return string_val;
}

template <>
inline BinaryData QueryMixed::get<BinaryData>() const noexcept
{
    REALM_ASSERT(get_type() == type_Binary);
    return binary_val;
}

template <>
inline Timestamp QueryMixed::get<Timestamp>() const noexcept
{
    REALM_ASSERT(get_type() == type_Timestamp);
    return date_val;
}

template <>
inline Decimal128 QueryMixed::get<Decimal128>() const noexcept
{
    switch (m_type) {
        case type_Bool: return Decimal128(bool_val);
        case type_Int: return Decimal128(int_val);
        case type_Float: return Decimal128(float_val);
        case type_Double: return Decimal128(double_val);
        case type_String: return Decimal128(string_val);
        case type_Decimal: return decimal_val;
    }
    REALM_UNREACHABLE();
}

template <>
inline ObjectId QueryMixed::get<ObjectId>() const noexcept
{
    REALM_ASSERT(get_type() == type_ObjectId);
    return id_val;
}

template <>
inline std::pair<StringData, ObjKey> QueryMixed::get<std::pair<StringData, ObjKey>>() const noexcept
{
    REALM_ASSERT(get_type() == type_Link);
    return obj_val;
}

inline std::vector<QueryMixed> const& QueryMixed::get_array() const noexcept
{
    REALM_ASSERT(get_type() == type_LinkList);
    return array_val;
}
}

template<typename T>
class CopyPtr : private std::unique_ptr<T> {
public:
    using std::unique_ptr<T>::unique_ptr;
    using std::unique_ptr<T>::get;
    using std::unique_ptr<T>::release;
    using std::unique_ptr<T>::reset;
    using std::unique_ptr<T>::operator bool;
    using std::unique_ptr<T>::operator*;
    using std::unique_ptr<T>::operator->;
    using std::unique_ptr<T>::operator=;

    CopyPtr(CopyPtr const& rhs) noexcept : std::unique_ptr<T>(rhs.clone()) { }
    CopyPtr& operator=(CopyPtr const& rhs) noexcept
    {
        if (this != &rhs) {
            *this = rhs.clone();
        }
        return *this;
    }
private:
    std::unique_ptr<T> clone() const {
        if (*this) {
            return std::make_unique<T>(**this);
        }
        return nullptr;
    }
};

enum class OperatorType {
    Equal = NSEqualToPredicateOperatorType,
    NotEqual = NSNotEqualToPredicateOperatorType,
    LessThan = NSLessThanPredicateOperatorType,
    LessThanOrEqual = NSLessThanOrEqualToPredicateOperatorType,
    GreaterThan = NSGreaterThanPredicateOperatorType,
    GreaterThanOrEqual = NSGreaterThanOrEqualToPredicateOperatorType,
    BeginsWith = NSBeginsWithPredicateOperatorType,
    EndsWith = NSEndsWithPredicateOperatorType,
    Contains = NSContainsPredicateOperatorType,
    Like = NSLikePredicateOperatorType,
    In = NSInPredicateOperatorType,
    Matches = NSMatchesPredicateOperatorType,
    Between = NSBetweenPredicateOperatorType
};

enum class ComparisonOptions : unsigned char {
    None = 0,
    CaseInsensitive = 1,
    DiacriticInsensitive = 2,
    Normalized = 4,
    LocaleSensitive = 8,
};
inline bool is_set(ComparisonOptions o, ComparisonOptions opt)
{
    auto e = static_cast<unsigned char>(opt);
    return (static_cast<unsigned char>(o) & e) == e;
}

class Predicate;
class PredicateExpression {
public:
    enum class Type {
        ConstantValue = NSConstantValueExpressionType,
        EvaluatedObject = NSEvaluatedObjectExpressionType,
        Variable = NSVariableExpressionType,
        KeyPath = NSKeyPathExpressionType,
        Function = NSFunctionExpressionType,
        UnionSet = NSUnionSetExpressionType,
        IntersectSet = NSIntersectSetExpressionType,
        MinusSet = NSMinusSetExpressionType,
        Subquery = NSSubqueryExpressionType,
        Aggregate = NSAggregateExpressionType,
        AnyKey = NSAnyKeyExpressionType,
        Block = NSBlockExpressionType,
        Conditional = NSConditionalExpressionType
    };

    PredicateExpression() = default;
    PredicateExpression(Type type, realm::QueryMixed value)
    : m_type(type), m_value(value) {}
    PredicateExpression(Type type, realm::StringData key_path)
    : m_type(type), m_key_path(key_path) {}
    PredicateExpression(Type type, std::unique_ptr<PredicateExpression> operand, realm::StringData function,
               std::unique_ptr<PredicateExpression> argument)
    : m_type(type)
    , m_operand(operand.release())
    , m_function_name(function)
    , m_argument(argument.release())
    {}
    PredicateExpression(Type type, realm::StringData key_path, std::unique_ptr<Predicate> subpredicate)
    : m_type(type), m_key_path(key_path), m_predicate(subpredicate.release()) {}

    PredicateExpression(PredicateExpression&&) = default;
    PredicateExpression& operator=(PredicateExpression&&) = default;
    PredicateExpression(PredicateExpression const& e) = default;
    PredicateExpression& operator=(PredicateExpression const& e) = default;

    Type type() const noexcept { return m_type; }
    realm::StringData key_path() const noexcept { return m_key_path; }
    realm::QueryMixed value() const noexcept { return m_value; }
    realm::StringData function_name() const noexcept { return m_function_name; }
    PredicateExpression argument() const noexcept { return *m_argument; }
    PredicateExpression operand() const noexcept { return *m_operand; }
    Predicate const& predicate() const noexcept { return *m_predicate; }

private:
    Type m_type;

    // FIXME: union or something
    std::string m_key_path;
    realm::QueryMixed m_value;
    std::string m_function_name;
    CopyPtr<PredicateExpression> m_operand;
    CopyPtr<PredicateExpression> m_argument;
    CopyPtr<Predicate> m_predicate;
};

enum class CompoundPredicateType {
    And = NSAndPredicateType,
    Or = NSOrPredicateType,
    Not = NSNotPredicateType
};

class Predicate {
public:
    enum class Modifier {
        Direct = NSDirectPredicateModifier,
        All = NSAllPredicateModifier,
        Any = NSAnyPredicateModifier,
    };

    enum class Kind {
        Base,
        Compound,
        Comparison,
        True,
        False
    };

    Predicate(PredicateExpression left, PredicateExpression right,
              Modifier modifier, OperatorType op, ComparisonOptions options)
    : m_kind(Kind::Comparison)
    , m_left(left)
    , m_right(right)
    , m_modifier(modifier)
    , m_operator(op)
    , m_options(options)
    {
    }

    Predicate(Kind kind) : m_kind(kind) {}
    Predicate(CompoundPredicateType compound_type, std::vector<Predicate> subpredicates)
    : m_kind(Kind::Compound), m_compound_type(compound_type), m_subpredicates(std::move(subpredicates)) {}

    Kind predicate_type() const noexcept { return m_kind; }

    // Compound
    CompoundPredicateType compoundType() const noexcept { return m_compound_type; }
    std::vector<Predicate> subpredicates() const noexcept { return m_subpredicates; }

    // Expression
    Modifier modifier() const noexcept { return m_modifier; }
    OperatorType operator_type() const noexcept { return m_operator; }
    ComparisonOptions options() const noexcept { return m_options; }

    PredicateExpression left() const noexcept { return m_left; }
    PredicateExpression right() const noexcept { return m_right; }

private:
    CompoundPredicateType m_compound_type;
    std::vector<Predicate> m_subpredicates;

    Modifier m_modifier = Modifier::Direct;
    OperatorType m_operator;
    ComparisonOptions m_options = ComparisonOptions::None;

    PredicateExpression m_left;
    PredicateExpression m_right;

    Kind m_kind = Kind::Base;
};

@class RLMObjectSchema, RLMSchema;

realm::Query RLMPredicateToQuery(Predicate predicate, RLMObjectSchema *objectSchema,
                                 RLMSchema *schema, realm::Group &group);

