package rules

import (
	"fmt"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// AddRule adds a new rule for the specified table
func (re *RuleEngine) AddRule(tableName string, rule Rule) {
	re.mutex.Lock()
	defer re.mutex.Unlock()
	re.Rules[tableName] = append(re.Rules[tableName], rule)
}

// ApplyRules applies all rules for the specified table to the given data
func (re *RuleEngine) ApplyRules(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	re.mutex.RLock()
	defer re.mutex.RUnlock()

	rules, exists := re.Rules[message.Table]
	if !exists {
		return message, nil // No rules for this table
	}

	logger.Info().
		Str("table", message.Table).
		Str("operation", string(message.Operation)).
		Int("ruleCount", len(rules)).
		Msg("Applying rules")

	var err error
	for _, rule := range rules {
		message, err = rule.Apply(message)
		if err != nil {
			return nil, err
		}
		if message == nil {
			// Message filtered out
			return nil, nil
		}
	}
	return message, nil
}

// LoadRules loads rules from the provided configuration
func (re *RuleEngine) LoadRules(config Config) error {
	for tableName, ruleConfigs := range config.Tables {
		logger.Info().Str("table", tableName).Msg("Loading rules for table")
		for i, ruleConfig := range ruleConfigs {
			rule, err := createRule(tableName, ruleConfig)
			if err != nil {
				return fmt.Errorf("error creating rule for table %s: %w", tableName, err)
			}
			logger.Info().
				Str("table", tableName).
				Int("ruleIndex", i+1).
				Str("ruleType", fmt.Sprintf("%T", rule)).
				Msg("Created rule")
			re.AddRule(tableName, rule)
		}
	}
	return nil
}

// createRule creates a new rule based on the provided configuration
func createRule(tableName string, config RuleConfig) (Rule, error) {
	logger.Info().
		Str("table", tableName).
		Str("ruleType", config.Type).
		Msg("Creating rule")
	switch config.Type {
	case "transform":
		return NewTransformRule(tableName, config.Column, config.Parameters)
	case "filter":
		return NewFilterRule(tableName, config.Column, config.Parameters)
	default:
		return nil, fmt.Errorf("unknown rule type: %s", config.Type)
	}
}
