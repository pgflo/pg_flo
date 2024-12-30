package rules

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"os"

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
		useOldValues := m.Operation == utils.OperationDelete
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

// NewComparisonCondition creates a new comparison condition function
func NewComparisonCondition(column, operator string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		useOldValues := m.Operation == utils.OperationDelete
		columnValue, err := m.GetColumnValue(column, useOldValues)
		if err != nil {
			return false
		}

		var colVal, compareVal decimal.Decimal

		// Handle column value from JSON
		switch v := columnValue.(type) {
		case float64:
			colVal = decimal.NewFromFloat(v)
			logger.Debug().
				Float64("value", v).
				Str("column", column).
				Msg("Converting float64")
		case json.Number:
			var err error
			colVal, err = decimal.NewFromString(v.String())
			if err != nil {
				logger.Debug().
					Err(err).
					Str("value", v.String()).
					Str("column", column).
					Msg("Failed to parse json.Number")
				return false
			}
			logger.Debug().
				Str("value", v.String()).
				Str("column", column).
				Msg("Converting json.Number")
		case int64:
			colVal = decimal.NewFromInt(v)
			logger.Debug().
				Int64("value", v).
				Str("column", column).
				Msg("Converting int64")
		case int:
			colVal = decimal.NewFromInt(int64(v))
			logger.Debug().
				Int("value", v).
				Str("column", column).
				Msg("Converting int")
		case string:
			// Handle numeric strings (sometimes wal2json sends these)
			var err error
			colVal, err = decimal.NewFromString(v)
			if err != nil {
				logger.Debug().
					Err(err).
					Str("value", v).
					Str("column", column).
					Msg("Failed to parse numeric string")
				return false
			}
			logger.Debug().
				Str("value", v).
				Str("column", column).
				Msg("Converting numeric string")
		default:
			logger.Debug().
				Str("type", fmt.Sprintf("%T", v)).
				Any("value", v).
				Str("column", column).
				Msg("Unsupported numeric type")
			return false
		}

		// Handle comparison value from YAML
		switch v := value.(type) {
		case float64:
			compareVal = decimal.NewFromFloat(v)
		case json.Number:
			var err error
			compareVal, err = decimal.NewFromString(v.String())
			if err != nil {
				return false
			}
		case int:
			compareVal = decimal.NewFromInt(int64(v))
		default:
			return false
		}

		switch operator {
		case "eq":
			return colVal.Equal(compareVal)
		case "ne":
			return !colVal.Equal(compareVal)
		case "gt":
			return colVal.GreaterThan(compareVal)
		case "lt":
			return colVal.LessThan(compareVal)
		case "gte":
			return colVal.GreaterThanOrEqual(compareVal)
		case "lte":
			return colVal.LessThanOrEqual(compareVal)
		default:
			return false
		}
	}
}

// NewContainsCondition creates a new contains condition function
func NewContainsCondition(column string, value interface{}) func(*utils.CDCMessage) bool {
	return func(m *utils.CDCMessage) bool {
		useOldValues := m.Operation == utils.OperationDelete
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

// Apply applies the transform rule to the provided data
func (r *TransformRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	if !containsOperation(r.Operations, message.Operation) {
		return message, nil
	}

	// Don't apply rule if asked not to
	if message.Operation == utils.OperationDelete && r.AllowEmptyDeletes {
		return message, nil
	}

	return r.Transform(message)
}

// Apply applies the filter rule to the provided data
func (r *FilterRule) Apply(message *utils.CDCMessage) (*utils.CDCMessage, error) {

	if !containsOperation(r.Operations, message.Operation) {
		return message, nil
	}

	// Don't apply rule if asked not to
	if message.Operation == utils.OperationDelete && r.AllowEmptyDeletes {
		return message, nil
	}

	passes := r.Condition(message)

	logger.Debug().
		Str("column", r.ColumnName).
		Any("operation", message.Operation).
		Bool("passes", passes).
		Bool("allowEmptyDeletes", r.AllowEmptyDeletes).
		Msg("Filter condition result")

	if !passes {
		return nil, nil // filter out
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
