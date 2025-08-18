package rules

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
)

var logger zerolog.Logger

func init() {
	logger = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	}).With().Timestamp().Logger()
}

// NewTransformRule creates a new transform rule based on the provided parameters
func NewTransformRule(table, column string, params map[string]interface{}) (Rule, error) {
	transformType, ok := params["type"].(string)
	if !ok {
		return nil, fmt.Errorf("transform type is required for TransformRule")
	}

	operations, _ := params["operations"].([]utils.OperationType)
	allowEmptyDeletes, _ := params["allow_empty_deletes"].(bool)

	switch transformType {
	case "regex":
		rule, err := NewRegexTransformRule(table, column, params)
		if err != nil {
			return nil, err
		}
		rule.Operations = operations
		rule.AllowEmptyDeletes = allowEmptyDeletes
		if len(rule.Operations) == 0 {
			rule.Operations = []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete}
		}
		return rule, nil
	case "mask":
		rule, err := NewMaskTransformRule(table, column, params)
		if err != nil {
			return nil, err
		}
		rule.Operations = operations
		rule.AllowEmptyDeletes = allowEmptyDeletes
		if len(rule.Operations) == 0 {
			rule.Operations = []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete}
		}
		return rule, nil
	default:
		return nil, fmt.Errorf("unsupported transform type: %s", transformType)
	}
}

// NewRegexTransformRule creates a new regex transform rule
func NewRegexTransformRule(table, column string, params map[string]interface{}) (*TransformRule, error) {
	pattern, ok := params["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("pattern parameter is required for regex transform")
	}
	replace, ok := params["replace"].(string)
	if !ok {
		return nil, fmt.Errorf("replace parameter is required for regex transform")
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	transform := func(m *utils.CDCMessage) (*utils.CDCMessage, error) {
		value, err := m.GetColumnValue(column, false)
		if err != nil {
			return m, nil
		}
		str, ok := value.(string)
		if !ok {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Str("type", fmt.Sprintf("%T", value)).
				Msg("Regex transform skipped: only works on string values")
			return m, nil
		}
		transformed := regex.ReplaceAllString(str, replace)
		err = m.SetColumnValue(column, transformed)
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	return &TransformRule{TableName: table, ColumnName: column, Transform: transform}, nil
}

// NewMaskTransformRule creates a new mask transform rule
func NewMaskTransformRule(table, column string, params map[string]interface{}) (*TransformRule, error) {
	maskChar, ok := params["mask_char"].(string)
	if !ok {
		return nil, fmt.Errorf("mask_char parameter is required for mask transform")
	}

	transform := func(m *utils.CDCMessage) (*utils.CDCMessage, error) {
		useOldValues := m.Type == utils.OperationDelete
		value, err := m.GetColumnValue(column, useOldValues)
		if err != nil {
			return m, nil
		}
		str, ok := value.(string)
		if !ok {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Str("type", fmt.Sprintf("%T", value)).
				Msg("Mask transform skipped: only works on string values")
			return m, nil
		}
		if len(str) <= 2 {
			return m, nil
		}
		masked := string(str[0]) + strings.Repeat(maskChar, len(str)-2) + string(str[len(str)-1])
		err = m.SetColumnValue(column, masked)
		if err != nil {
			return nil, err
		}
		return m, nil
	}

	return &TransformRule{TableName: table, ColumnName: column, Transform: transform}, nil
}

// NewFilterRule creates a new filter rule based on the provided parameters
func NewFilterRule(table, column string, params map[string]interface{}) (Rule, error) {
	operator, ok := params["operator"].(string)
	if !ok {
		return nil, fmt.Errorf("operator parameter is required for FilterRule")
	}
	value, ok := params["value"]
	if !ok {
		return nil, fmt.Errorf("value parameter is required for FilterRule")
	}

	operations, _ := params["operations"].([]utils.OperationType)
	allowEmptyDeletes, _ := params["allow_empty_deletes"].(bool)

	var condition func(*utils.CDCMessage) bool

	switch operator {
	case "eq", "ne", "gt", "lt", "gte", "lte":
		condition = NewComparisonCondition(column, operator, value)
	case "contains":
		condition = NewContainsCondition(column, value)
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	rule := &FilterRule{
		TableName:         table,
		ColumnName:        column,
		Condition:         condition,
		Operations:        operations,
		AllowEmptyDeletes: allowEmptyDeletes,
	}

	if len(rule.Operations) == 0 {
		rule.Operations = []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete}
	}

	return rule, nil
}

func NewExcludeColumnRule(table, column string) (Rule, error) {
	rule := &ExcludeColumnRule{
		TableName:  table,
		ColumnName: column,
	}
	return rule, nil
}

// NewComparisonCondition creates a new comparison condition function
func NewComparisonCondition(column, operator string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		useOldValues := m.Type == utils.OperationDelete
		columnValue, err := m.GetColumnValue(column, useOldValues)
		if err != nil {
			return false
		}

		colIndex := m.GetColumnIndex(column)
		if colIndex == -1 {
			return false
		}

		// Unified comparison - work directly with pgx native types
		return compareAnyValues(columnValue, value, operator)
	}
}

// NewContainsCondition creates a new contains condition function
func NewContainsCondition(column string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		useOldValues := m.Type == utils.OperationDelete
		columnValue, err := m.GetColumnValue(column, useOldValues)
		if err != nil {
			return false
		}
		str, ok := columnValue.(string)
		if !ok {
			return false
		}
		searchStr, ok := value.(string)
		if !ok {
			return false
		}
		return strings.Contains(str, searchStr)
	}
}

// compareAnyValues is a unified comparison function that works with any type
func compareAnyValues(a, b interface{}, operator string) bool {
	// Handle nil values
	if a == nil || b == nil {
		switch operator {
		case "eq":
			return a == nil && b == nil
		case "ne":
			return !(a == nil && b == nil)
		default:
			return false
		}
	}

	// Handle time.Time with timezone awareness
	if aTime, ok := a.(time.Time); ok {
		if bTime, ok := b.(time.Time); ok {
			return compareTime(aTime, bTime, operator)
		}
		// Try to parse string as time
		if bStr := fmt.Sprintf("%v", b); bStr != "" {
			if bTime, err := utils.ParseTimestamp(bStr); err == nil {
				return compareTime(aTime, bTime, operator)
			}
		}
		return false
	}

	// Handle pgtype.Numeric with precision
	if aNum, ok := a.(pgtype.Numeric); ok {
		if !aNum.Valid {
			return false
		}
		aDec := decimal.NewFromBigInt(aNum.Int, aNum.Exp)
		bDec, err := decimal.NewFromString(fmt.Sprintf("%v", b))
		if err != nil {
			return false
		}
		return compareDecimalValues(aDec, bDec, operator)
	}

	// Handle numeric mixing (int vs float) - ONLY if both are actually numeric
	if isNumeric(a) && isNumeric(b) {
		aFloat, aOk := convertToFloat64(a)
		bFloat, bOk := convertToFloat64(b)
		if aOk && bOk {
			return compareFloat64(aFloat, bFloat, operator)
		}
	}

	// Try to convert rule value (b) to match column type (a) for user-friendly rules
	if convertedB, ok := tryConvertRuleValue(b, a); ok {
		// Use the converted value for comparison
		switch operator {
		case "eq":
			return reflect.DeepEqual(a, convertedB)
		case "ne":
			return !reflect.DeepEqual(a, convertedB)
		default:
			// For ordering, ensure both are the same type now
			return compareConvertedValues(a, convertedB, operator)
		}
	}

	// If conversion failed, check strict type compatibility
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)
	if !areTypesCompatible(aType, bType) {
		return false // Incompatible types fail comparison
	}

	// Direct comparison for compatible types
	switch operator {
	case "eq":
		return reflect.DeepEqual(a, b)
	case "ne":
		return !reflect.DeepEqual(a, b)
	default:
		// For ordering on compatible types, use string comparison as fallback
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		return compareStringValues(aStr, bStr, operator)
	}
}

// Helper functions for unified comparison system

// tryConvertRuleValue attempts to convert rule value to match column type
func tryConvertRuleValue(ruleValue, columnValue interface{}) (interface{}, bool) {
	ruleStr := fmt.Sprintf("%v", ruleValue)

	switch columnValue.(type) {
	case int32:
		if val, ok := utils.ToInt64(ruleValue); ok {
			return int32(val), true
		}
		if val, ok := utils.ToInt64(ruleStr); ok {
			return int32(val), true
		}
	case int64:
		if val, ok := utils.ToInt64(ruleValue); ok {
			return val, true
		}
		if val, ok := utils.ToInt64(ruleStr); ok {
			return val, true
		}
	case float64:
		if val, ok := utils.ToFloat64(ruleValue); ok {
			return val, true
		}
		if val, ok := utils.ToFloat64(ruleStr); ok {
			return val, true
		}
	case bool:
		if val, ok := utils.ToBool(ruleValue); ok {
			return val, true
		}
		if val, ok := utils.ToBool(ruleStr); ok {
			return val, true
		}
	}

	return nil, false
}

// compareConvertedValues compares values of the same type
func compareConvertedValues(a, b interface{}, operator string) bool {
	switch aVal := a.(type) {
	case int32:
		bVal := b.(int32)
		return compareInt32(aVal, bVal, operator)
	case int64:
		bVal := b.(int64)
		return compareInt64(aVal, bVal, operator)
	case float64:
		bVal := b.(float64)
		return compareFloat64(aVal, bVal, operator)
	case bool:
		bVal := b.(bool)
		return compareBool(aVal, bVal, operator)
	default:
		return false
	}
}

// areTypesCompatible checks if two types can be meaningfully compared
func areTypesCompatible(aType, bType reflect.Type) bool {
	if aType == bType {
		return true // Same types are always compatible
	}

	// Allow numeric type mixing (int, float)
	if isNumericType(aType) && isNumericType(bType) {
		return true
	}

	// Allow string comparisons with string-like types
	if isStringType(aType) && isStringType(bType) {
		return true
	}

	// All other combinations are incompatible
	return false
}

// isNumericType checks if a reflect.Type represents a numeric type
func isNumericType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// isStringType checks if a reflect.Type represents a string-like type
func isStringType(t reflect.Type) bool {
	if t == nil {
		return false
	}
	return t.Kind() == reflect.String
}

// isNumeric checks if a value is a numeric type
func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

// convertToFloat64 safely converts various numeric types to float64
func convertToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}

// compareFloat64 compares two float64 values
func compareFloat64(a, b float64, operator string) bool {
	switch operator {
	case "eq":
		return a == b
	case "ne":
		return a != b
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	default:
		return false
	}
}

// compareTime compares two time.Time values
func compareTime(a, b time.Time, operator string) bool {
	switch operator {
	case "eq":
		return a.Equal(b)
	case "ne":
		return !a.Equal(b)
	case "gt":
		return a.After(b)
	case "lt":
		return a.Before(b)
	case "gte":
		return a.After(b) || a.Equal(b)
	case "lte":
		return a.Before(b) || a.Equal(b)
	default:
		return false
	}
}

// compareInt32 compares two int32 values
func compareInt32(a, b int32, operator string) bool {
	switch operator {
	case "eq":
		return a == b
	case "ne":
		return a != b
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	default:
		return false
	}
}

// compareInt64 compares two int64 values
func compareInt64(a, b int64, operator string) bool {
	switch operator {
	case "eq":
		return a == b
	case "ne":
		return a != b
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	default:
		return false
	}
}

// compareBool compares two boolean values
func compareBool(a, b bool, operator string) bool {
	switch operator {
	case "eq":
		return a == b
	case "ne":
		return a != b
	default:
		return false // Boolean values don't have ordering
	}
}

// compareStringValues compares two string values
func compareStringValues(a, b string, operator string) bool {
	switch operator {
	case "eq":
		return a == b
	case "ne":
		return a != b
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	default:
		return false
	}
}

// compareDecimalValues compares two decimal values directly - no string conversion
func compareDecimalValues(a, b decimal.Decimal, operator string) bool {
	switch operator {
	case "eq":
		return a.Equal(b)
	case "ne":
		return !a.Equal(b)
	case "gt":
		return a.GreaterThan(b)
	case "lt":
		return a.LessThan(b)
	case "gte":
		return a.GreaterThanOrEqual(b)
	case "lte":
		return a.LessThanOrEqual(b)
	default:
		return false
	}
}

// Apply applies the transform rule to the provided data
func (r *TransformRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	if !containsOperation(r.Operations, message.Type) {
		return message, nil
	}

	// Don't apply rule if asked not to
	if message.Type == utils.OperationDelete && r.AllowEmptyDeletes {
		return message, nil
	}

	return r.Transform(message)
}

// Apply applies the filter rule to the provided data
func (r *FilterRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	if !containsOperation(r.Operations, message.Type) {
		return message, nil
	}

	// Don't apply rule if asked not to
	if message.Type == utils.OperationDelete && r.AllowEmptyDeletes {
		return message, nil
	}

	passes := r.Condition(message)

	logger.Debug().
		Str("column", r.ColumnName).
		Any("operation", message.Type).
		Bool("passes", passes).
		Bool("allowEmptyDeletes", r.AllowEmptyDeletes).
		Msg("Filter condition result")

	if !passes {
		return nil, nil // filter out
	}

	return message, nil
}

// Apply applies the exclude_column rule to the provided data
func (r *ExcludeColumnRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	err := message.RemoveColumn(r.ColumnName)
	if err != nil {
		if _, ok := err.(utils.ColumnNotFoundError); !ok {
			// make this a warning since some columns are produced in copy but not in logs
			logger.Warn().Str("column", r.ColumnName).Err(err).Msg("Failed to exclude column")
		}
	}
	if len(message.Columns) == 0 {
		return nil, fmt.Errorf("exclude_column removed the last column from table: %s", r.TableName)
	}
	return message, nil
}

// containsOperation checks if the given operation is in the list of operations
func containsOperation(operations []utils.OperationType, operation utils.OperationType) bool {
	for _, op := range operations {
		if op == operation {
			return true
		}
	}
	return false
}
