package rules_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/pgflo/pg_flo/pkg/rules"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestTransformRules(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
	}{
		{
			name: "Regex Transform - Email Domain",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":    "regex",
				"pattern": "@example\\.com$",
				"replace": "@masked.com",
			}),
			input:          createCDCMessage(utils.OperationInsert, "email", pgtype.TextOID, "user@example.com"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "email", pgtype.TextOID, "user@masked.com"),
		},
		{
			name: "Mask Transform - Credit Card",
			rule: createRule(t, "transform", "payments", "credit_card", map[string]interface{}{
				"type":      "mask",
				"mask_char": "*",
			}),
			input:          createCDCMessage(utils.OperationInsert, "credit_card", pgtype.TextOID, "1234567890123456"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "credit_card", pgtype.TextOID, "1**************6"),
		},
		{
			name: "Regex Transform - Phone Number",
			rule: createRule(t, "transform", "contacts", "phone", map[string]interface{}{
				"type":    "regex",
				"pattern": "(\\d{3})(\\d{3})(\\d{4})",
				"replace": "($1) $2-$3",
			}),
			input:          createCDCMessage(utils.OperationUpdate, "phone", pgtype.TextOID, "1234567890"),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "phone", pgtype.TextOID, "(123) 456-7890"),
		},
		{
			name: "Regex Transform - No Match",
			rule: createRule(t, "transform", "users", "email", map[string]interface{}{
				"type":    "regex",
				"pattern": "@example\\.com$",
				"replace": "@masked.com",
			}),
			input:          createCDCMessage(utils.OperationInsert, "email", pgtype.TextOID, "user@otherdomain.com"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "email", pgtype.TextOID, "user@otherdomain.com"),
		},
		{
			name: "Mask Transform - Short String",
			rule: createRule(t, "transform", "payments", "credit_card", map[string]interface{}{
				"type":      "mask",
				"mask_char": "*",
			}),
			input:          createCDCMessage(utils.OperationInsert, "credit_card", pgtype.TextOID, "12"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "credit_card", pgtype.TextOID, "12"),
		},
		{
			name: "Regex Transform - Non-String Input",
			rule: createRule(t, "transform", "products", "price", map[string]interface{}{
				"type":    "regex",
				"pattern": "^\\d+\\.\\d{2}$",
				"replace": "$.$$",
			}),
			input:          createCDCMessage(utils.OperationUpdate, "price", pgtype.Float8OID, 99.99),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "price", pgtype.Float8OID, 99.99),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestFilterRules(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
	}{
		{
			name: "Equal Filter (Text) - Pass",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator": "eq",
				"value":    "completed",
			}),
			input:          createCDCMessage(utils.OperationInsert, "status", pgtype.TextOID, "completed"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "status", pgtype.TextOID, "completed"),
		},
		{
			name: "Equal Filter (Text) - Fail",
			rule: createRule(t, "filter", "orders", "status", map[string]interface{}{
				"operator": "eq",
				"value":    "completed",
			}),
			input:          createCDCMessage(utils.OperationInsert, "status", pgtype.TextOID, "pending"),
			expectedOutput: nil,
		},
		{
			name: "Greater Than Filter (Integer) - Pass",
			rule: createRule(t, "filter", "products", "stock", map[string]interface{}{
				"operator": "gt",
				"value":    10,
			}),
			input:          createCDCMessage(utils.OperationUpdate, "stock", pgtype.Int4OID, 15),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "stock", pgtype.Int4OID, 15),
		},
		{
			name: "Less Than Filter (Float) - Pass",
			rule: createRule(t, "filter", "prices", "amount", map[string]interface{}{
				"operator": "lt",
				"value":    100.0,
			}),
			input:          createCDCMessage(utils.OperationInsert, "amount", pgtype.Float8OID, 99.99),
			expectedOutput: createCDCMessage(utils.OperationInsert, "amount", pgtype.Float8OID, 99.99),
		},
		{
			name: "Contains Filter (Text) - Pass",
			rule: createRule(t, "filter", "products", "name", map[string]interface{}{
				"operator": "contains",
				"value":    "Premium",
			}),
			input:          createCDCMessage(utils.OperationInsert, "name", pgtype.TextOID, "Premium Widget"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "name", pgtype.TextOID, "Premium Widget"),
		},
		{
			name: "Equal Filter (Case Insensitive) - Pass",
			rule: createRule(t, "filter", "products", "category", map[string]interface{}{
				"operator": "eq",
				"value":    "Electronics",
			}),
			input:          createCDCMessage(utils.OperationInsert, "category", pgtype.TextOID, "electronics"),
			expectedOutput: nil,
		},
		{
			name: "Greater Than Filter (String vs Integer) - Pass",
			rule: createRule(t, "filter", "orders", "quantity", map[string]interface{}{
				"operator": "gt",
				"value":    "5",
			}),
			input:          createCDCMessage(utils.OperationUpdate, "quantity", pgtype.Int4OID, 10),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "quantity", pgtype.Int4OID, 10),
		},
		{
			name: "Less Than Filter (Float vs Integer) - Pass",
			rule: createRule(t, "filter", "products", "price", map[string]interface{}{
				"operator": "lt",
				"value":    100,
			}),
			input:          createCDCMessage(utils.OperationInsert, "price", pgtype.Float8OID, 99.99),
			expectedOutput: createCDCMessage(utils.OperationInsert, "price", pgtype.Float8OID, 99.99),
		},
		{
			name: "Contains Filter (Case Sensitive) - Fail",
			rule: createRule(t, "filter", "users", "name", map[string]interface{}{
				"operator": "contains",
				"value":    "John",
			}),
			input:          createCDCMessage(utils.OperationInsert, "name", pgtype.TextOID, "john doe"),
			expectedOutput: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestDateTimeFilters(t *testing.T) {
	now := time.Now()
	pastDate := now.Add(-24 * time.Hour)

	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
	}{
		{
			name: "Greater Than Date Filter - Pass",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "gt",
				"value":    pastDate.Format(time.RFC3339),
			}),
			input:          createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, now),
			expectedOutput: createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, now),
		},
		{
			name: "Less Than or Equal Date Filter - Pass",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "lte",
				"value":    now.Format(time.RFC3339),
			}),
			input:          createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, pastDate),
			expectedOutput: createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, pastDate),
		},
		{
			name: "Equal Date Filter (Different Timezone) - Pass",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "eq",
				"value":    now.UTC().Truncate(time.Second).Format(time.RFC3339),
			}),
			input:          createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, now.Truncate(time.Second).In(time.FixedZone("EST", -5*60*60))),
			expectedOutput: createCDCMessage(utils.OperationInsert, "date", pgtype.TimestamptzOID, now.Truncate(time.Second).In(time.FixedZone("EST", -5*60*60))),
		},
		{
			name: "Greater Than Date Filter (String Input) - Fail",
			rule: createRule(t, "filter", "events", "date", map[string]interface{}{
				"operator": "gt",
				"value":    pastDate.Format(time.RFC3339),
			}),
			input:          createCDCMessage(utils.OperationInsert, "date", pgtype.TextOID, now.Format(time.RFC3339)),
			expectedOutput: createCDCMessage(utils.OperationInsert, "date", pgtype.TextOID, now.Format(time.RFC3339)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestBooleanFilters(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
	}{
		{
			name: "Equal Boolean Filter - Pass",
			rule: createRule(t, "filter", "users", "is_active", map[string]interface{}{
				"operator": "eq",
				"value":    true,
			}),
			input:          createCDCMessage(utils.OperationInsert, "is_active", pgtype.BoolOID, true),
			expectedOutput: createCDCMessage(utils.OperationInsert, "is_active", pgtype.BoolOID, true),
		},
		{
			name: "Not Equal Boolean Filter - Pass",
			rule: createRule(t, "filter", "users", "is_deleted", map[string]interface{}{
				"operator": "ne",
				"value":    true,
			}),
			input:          createCDCMessage(utils.OperationUpdate, "is_deleted", pgtype.BoolOID, false),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "is_deleted", pgtype.BoolOID, false),
		},
		{
			name: "Equal Boolean Filter (String Input) - Pass",
			rule: createRule(t, "filter", "users", "is_active", map[string]interface{}{
				"operator": "eq",
				"value":    true,
			}),
			input:          createCDCMessage(utils.OperationInsert, "is_active", pgtype.TextOID, "true"),
			expectedOutput: nil,
		},
		{
			name: "Not Equal Boolean Filter (Integer Input) - Fail",
			rule: createRule(t, "filter", "users", "is_deleted", map[string]interface{}{
				"operator": "ne",
				"value":    false,
			}),
			input:          createCDCMessage(utils.OperationUpdate, "is_deleted", pgtype.Int4OID, 0),
			expectedOutput: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestNumericFilters(t *testing.T) {
	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
	}{
		{
			name: "Greater Than or Equal Numeric Filter - Pass",
			rule: createRule(t, "filter", "products", "price", map[string]interface{}{
				"operator": "gte",
				"value":    "99.99",
			}),
			input:          createCDCMessage(utils.OperationInsert, "price", pgtype.NumericOID, "100.00"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "price", pgtype.NumericOID, "100.00"),
		},
		{
			name: "Less Than Numeric Filter - Pass",
			rule: createRule(t, "filter", "orders", "total", map[string]interface{}{
				"operator": "lt",
				"value":    "1000.00",
			}),
			input:          createCDCMessage(utils.OperationUpdate, "total", pgtype.NumericOID, "999.99"),
			expectedOutput: createCDCMessage(utils.OperationUpdate, "total", pgtype.NumericOID, "999.99"),
		},
		{
			name: "Less Than Numeric Filter (String Input) - Fail",
			rule: createRule(t, "filter", "orders", "total", map[string]interface{}{
				"operator": "lt",
				"value":    1000.00,
			}),
			input:          createCDCMessage(utils.OperationUpdate, "total", pgtype.TextOID, "999.99"),
			expectedOutput: nil,
		},
		{
			name: "Equal Numeric Filter (Exact Match) - Pass",
			rule: createRule(t, "filter", "products", "weight", map[string]interface{}{
				"operator": "eq",
				"value":    "1.24",
			}),
			input:          createCDCMessage(utils.OperationInsert, "weight", pgtype.NumericOID, "1.24"),
			expectedOutput: createCDCMessage(utils.OperationInsert, "weight", pgtype.NumericOID, "1.24"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestExcludeColumnRule(t *testing.T) {
	createWithA := func(opType utils.OperationType, columnName string, dataType uint32, value interface{}) *utils.CDCMessage {
		return &utils.CDCMessage{
			Type: opType,
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "a", DataType: dataType},
				{Name: columnName, DataType: dataType},
			},
			NewTuple: &pglogrepl.TupleData{
				ColumnNum: 2,
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: encodeValue(1)}, // a
					{Data: encodeValue(value)},
				},
			},
		}
	}
	createWithC := func(opType utils.OperationType, columnName string, dataType uint32, value interface{}) *utils.CDCMessage {
		return &utils.CDCMessage{
			Type: opType,
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "a", DataType: dataType},
				{Name: columnName, DataType: dataType},
				{Name: "c", DataType: dataType},
			},
			NewTuple: &pglogrepl.TupleData{
				ColumnNum: 3,
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: encodeValue(1)}, // a
					{Data: encodeValue(value)},
					{Data: encodeValue(3)}, // c
				},
			},
		}
	}

	tests := []struct {
		name           string
		rule           rules.Rule
		input          *utils.CDCMessage
		expectedOutput *utils.CDCMessage
		wantErr        bool
	}{
		{
			name:           "Simple exclude column - Pass",
			rule:           createRule(t, "exclude_column", "products", "a", nil),
			input:          createWithA(utils.OperationInsert, "price", pgtype.Int4OID, 2),
			expectedOutput: createCDCMessage(utils.OperationInsert, "price", pgtype.Int4OID, 2),
		},
		{
			name:           "Exclude column c - Pass",
			rule:           createRule(t, "exclude_column", "products", "c", nil),
			input:          createWithC(utils.OperationInsert, "price", pgtype.Int4OID, 2),
			expectedOutput: createWithA(utils.OperationInsert, "price", pgtype.Int4OID, 2),
		},
		{
			name:           "Exclude all columns - Error",
			rule:           createRule(t, "exclude_column", "products", "price", nil),
			input:          createCDCMessage(utils.OperationInsert, "price", pgtype.Int4OID, 2),
			expectedOutput: nil,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := tt.rule.Apply(tt.input)
			if !tt.wantErr {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func createRule(t *testing.T, ruleType, table, column string, params map[string]interface{}) rules.Rule {
	var rule rules.Rule
	var err error

	switch ruleType {
	case "transform":
		rule, err = rules.NewTransformRule(table, column, params)
	case "filter":
		rule, err = rules.NewFilterRule(table, column, params)
	case "exclude_column":
		rule, err = rules.NewExcludeColumnRule(table, column)
	default:
		t.Fatalf("Unknown rule type: %s", ruleType)
	}

	if err != nil {
		t.Fatalf("Failed to create rule: %v", err)
	}

	return rule
}

func createCDCMessage(opType utils.OperationType, columnName string, dataType uint32, value interface{}) *utils.CDCMessage {
	return &utils.CDCMessage{
		Type: opType,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: columnName, DataType: dataType},
		},
		NewTuple: &pglogrepl.TupleData{
			ColumnNum: 1,
			Columns: []*pglogrepl.TupleDataColumn{
				{Data: encodeValue(value)},
			},
		},
	}
}

func encodeValue(value interface{}) []byte {
	// Use our proper PostgreSQL encoding for consistent test data
	data, err := utils.GlobalPostgreSQLTypeConverter.EncodePostgreSQLValue(value, getOIDForValue(value))
	if err != nil {
		// Fallback for unknown types
		return []byte(fmt.Sprintf("%v", value))
	}
	return data
}

func getOIDForValue(value interface{}) uint32 {
	switch value.(type) {
	case string:
		return pgtype.TextOID
	case int, int32:
		return pgtype.Int4OID
	case int64:
		return pgtype.Int8OID
	case float64:
		return pgtype.Float8OID
	case bool:
		return pgtype.BoolOID
	case time.Time:
		return pgtype.TimestamptzOID
	default:
		return pgtype.TextOID
	}
}
