package rules

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
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

	var rule *TransformRule
	var err error

	switch transformType {
	case "regex":
		rule, err = NewRegexTransformRule(table, column, params)
	case "mask":
		rule, err = NewMaskTransformRule(table, column, params)
	default:
		return nil, fmt.Errorf("unsupported transform type: %s", transformType)
	}

	if err != nil {
		return nil, err
	}

	return setupRuleOperations(rule, params), nil
}

// setupRuleOperations consolidates common operation and allowEmptyDeletes setup
func setupRuleOperations(rule *TransformRule, params map[string]interface{}) *TransformRule {
	operations, _ := params["operations"].([]utils.OperationType)
	allowEmptyDeletes, _ := params["allow_empty_deletes"].(bool)

	rule.Operations = operations
	rule.AllowEmptyDeletes = allowEmptyDeletes
	if len(rule.Operations) == 0 {
		rule.Operations = []utils.OperationType{utils.OperationInsert, utils.OperationUpdate, utils.OperationDelete}
	}
	return rule
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
		rawData, colIndex := getRawColumnData(m, column)
		if rawData == nil || colIndex == -1 {
			return m, nil
		}

		// Only apply regex to text-like data
		dataType := m.Columns[colIndex].DataType
		if !isTextType(dataType) {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Uint32("dataType", dataType).
				Msg("Regex transform skipped: only works on text types")
			return m, nil
		}

		// Transform at text level - preserves PostgreSQL semantics
		original := string(rawData)
		transformed := regex.ReplaceAllString(original, replace)

		// Set directly back - no encoding needed
		setRawColumnData(m, colIndex, []byte(transformed))
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
		rawData, colIndex := getRawColumnData(m, column)
		if rawData == nil || colIndex == -1 {
			return m, nil
		}

		// Only apply mask to text-like data
		dataType := m.Columns[colIndex].DataType
		if !isTextType(dataType) {
			logger.Warn().
				Str("table", table).
				Str("column", column).
				Uint32("dataType", dataType).
				Msg("Mask transform skipped: only works on text types")
			return m, nil
		}

		// Transform at text level
		original := string(rawData)
		if len(original) <= 2 {
			return m, nil
		}

		masked := string(original[0]) + strings.Repeat(maskChar, len(original)-2) + string(original[len(original)-1])
		setRawColumnData(m, colIndex, []byte(masked))
		return m, nil
	}

	return &TransformRule{TableName: table, ColumnName: column, Transform: transform}, nil
}

func getRawColumnData(m *utils.CDCMessage, column string) ([]byte, int) {
	colIndex := m.GetColumnIndex(column)
	if colIndex == -1 {
		return nil, -1
	}

	var rawData []byte
	useOldValues := m.Type == utils.OperationDelete
	if useOldValues && m.OldTuple != nil {
		rawData = m.OldTuple.Columns[colIndex].Data
	} else if m.NewTuple != nil {
		rawData = m.NewTuple.Columns[colIndex].Data
	} else if m.CopyData != nil {
		rawData = m.CopyData[colIndex]
	}

	return rawData, colIndex
}

func setRawColumnData(m *utils.CDCMessage, colIndex int, data []byte) {
	useOldValues := m.Type == utils.OperationDelete
	if useOldValues && m.OldTuple != nil {
		m.OldTuple.Columns[colIndex].Data = data
	} else if m.NewTuple != nil {
		m.NewTuple.Columns[colIndex].Data = data
	} else if m.CopyData != nil {
		m.CopyData[colIndex] = data
	}
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

// NewComparisonCondition creates a new comparison condition function using PostgreSQL semantics
func NewComparisonCondition(column, operator string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		rawData, colIndex := getRawColumnData(m, column)
		if rawData == nil || colIndex == -1 {
			return false
		}

		columnValue := string(rawData)
		ruleValue := fmt.Sprintf("%v", value)
		dataType := m.Columns[colIndex].DataType

		return comparePostgreSQLValues(columnValue, ruleValue, operator, dataType)
	}
}

// NewContainsCondition creates a new contains condition function
func NewContainsCondition(column string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		rawData, colIndex := getRawColumnData(m, column)
		if rawData == nil || colIndex == -1 {
			return false
		}

		columnValue := string(rawData)
		searchStr := fmt.Sprintf("%v", value)
		return strings.Contains(columnValue, searchStr)
	}
}

// comparePostgreSQLValues uses PostgreSQL's text comparison semantics - much simpler and more reliable
func comparePostgreSQLValues(a, b, operator string, dataType uint32) bool {
	// Check for strict type compatibility based on the rule value type
	if !isRuleValueCompatibleWithColumn(b, dataType) {
		return false // Incompatible types fail comparison
	}

	// For booleans, handle PostgreSQL's 't'/'f' representation
	if isBoolType(dataType) {
		return compareBooleanStrings(a, b, operator)
	}

	switch operator {
	case "eq":
		// For numeric types, normalize values for comparison
		if isNumericType(dataType) {
			return compareNumericStrings(a, b, "eq")
		}
		// For timestamps, handle timezone equivalence
		if isTimeType(dataType) {
			return compareTimeStrings(a, b, "eq")
		}
		// Default string comparison
		return a == b
	case "ne":
		// Inverse of eq logic
		if isNumericType(dataType) {
			return !compareNumericStrings(a, b, "eq")
		}
		if isTimeType(dataType) {
			return !compareTimeStrings(a, b, "eq")
		}
		return a != b
	case "gt", "lt", "gte", "lte":
		// For numeric types, do numeric comparison
		if isNumericType(dataType) {
			return compareNumericStrings(a, b, operator)
		}
		// For timestamps, do time comparison
		if isTimeType(dataType) {
			return compareTimeStrings(a, b, operator)
		}
		// For booleans, ordering doesn't make sense
		if isBoolType(dataType) {
			return false
		}
		// Default: lexicographic (works for most types like text, char, etc.)
		return compareLexicographic(a, b, operator)
	}
	return false
}

func isTextType(dataType uint32) bool {
	return dataType == pgtype.TextOID || dataType == pgtype.VarcharOID || dataType == pgtype.BPCharOID
}

func isNumericType(dataType uint32) bool {
	return dataType == pgtype.Int2OID || dataType == pgtype.Int4OID || dataType == pgtype.Int8OID ||
		dataType == pgtype.Float4OID || dataType == pgtype.Float8OID || dataType == pgtype.NumericOID
}

func isTimeType(dataType uint32) bool {
	return dataType == pgtype.TimestampOID || dataType == pgtype.TimestamptzOID || dataType == pgtype.DateOID
}

func isBoolType(dataType uint32) bool {
	return dataType == pgtype.BoolOID
}

// isRuleValueCompatibleWithColumn checks if a rule value is compatible with a column type
func isRuleValueCompatibleWithColumn(ruleValue string, columnDataType uint32) bool {
	// Detect the rule value type based on content
	if isBooleanRuleValue(ruleValue) {
		return isBoolType(columnDataType) // Strict: boolean rules only work with boolean columns
	}
	if isNumericRuleValue(ruleValue) {
		return isNumericType(columnDataType) || isTextType(columnDataType) // Allow numeric rules with text containing numbers
	}
	if isTimeRuleValue(ruleValue) {
		return isTimeType(columnDataType) || isTextType(columnDataType) // Allow time rules with text containing timestamps
	}
	// String values are compatible with most types (except boolean, which is strict)
	return !isBoolType(columnDataType)
}

func isBooleanRuleValue(s string) bool {
	switch strings.ToLower(s) {
	case "true", "false":
		return true
	default:
		return false
	}
}

func isNumericRuleValue(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isTimeRuleValue(s string) bool {
	_, err := parseBasicTime(s)
	return err == nil
}

func compareBooleanStrings(a, b, operator string) bool {
	aBool := postgresStringToBool(a)
	bBool := postgresStringToBool(b)

	switch operator {
	case "eq":
		return aBool == bBool
	case "ne":
		return aBool != bBool
	default:
		return false // Boolean values don't have ordering
	}
}

func postgresStringToBool(s string) bool {
	switch strings.ToLower(s) {
	case "t", "true", "1", "y", "yes", "on":
		return true
	case "f", "false", "0", "n", "no", "off":
		return false
	default:
		return false
	}
}

func compareNumericStrings(a, b, operator string) bool {
	aFloat, aErr := strconv.ParseFloat(a, 64)
	bFloat, bErr := strconv.ParseFloat(b, 64)

	// If parsing fails, fall back to string comparison
	if aErr != nil || bErr != nil {
		if operator == "eq" {
			return a == b
		}
		return compareLexicographic(a, b, operator)
	}

	switch operator {
	case "eq":
		return aFloat == bFloat
	case "gt":
		return aFloat > bFloat
	case "lt":
		return aFloat < bFloat
	case "gte":
		return aFloat >= bFloat
	case "lte":
		return aFloat <= bFloat
	}
	return false
}

func compareTimeStrings(a, b, operator string) bool {
	aTime, aErr := parseBasicTime(a)
	bTime, bErr := parseBasicTime(b)

	if aErr == nil && bErr == nil {
		switch operator {
		case "eq":
			return aTime.Equal(bTime)
		case "gt":
			return aTime.After(bTime)
		case "lt":
			return aTime.Before(bTime)
		case "gte":
			return aTime.After(bTime) || aTime.Equal(bTime)
		case "lte":
			return aTime.Before(bTime) || aTime.Equal(bTime)
		}
	}

	return compareLexicographic(a, b, operator)
}

func parseBasicTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05-07:00", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("not a basic time format: %s", s)
}

func compareLexicographic(a, b, operator string) bool {
	switch operator {
	case "gt":
		return a > b
	case "lt":
		return a < b
	case "gte":
		return a >= b
	case "lte":
		return a <= b
	}
	return false
}

func shouldApplyRule(operations []utils.OperationType, allowEmptyDeletes bool, messageType utils.OperationType) bool {
	if !containsOperation(operations, messageType) {
		return false
	}
	return !(messageType == utils.OperationDelete && allowEmptyDeletes)
}

// Apply applies the transform rule to the provided data
func (r *TransformRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	if !shouldApplyRule(r.Operations, r.AllowEmptyDeletes, message.Type) {
		return message, nil
	}
	return r.Transform(message)
}

// Apply applies the filter rule to the provided data
func (r *FilterRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	if !shouldApplyRule(r.Operations, r.AllowEmptyDeletes, message.Type) {
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
