package rules

import (
	"sync"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// Rule interface defines the methods that all rules must implement
type Rule interface {
	Apply(message *utils.CDCMessage) (*utils.CDCMessage, error)
}

// RuleConfig represents the configuration for a single rule
type RuleConfig struct {
	Type              string                 `yaml:"type"`
	Column            string                 `yaml:"column"`
	Parameters        map[string]interface{} `yaml:"parameters"`
	Operations        []utils.OperationType  `yaml:"operations,omitempty"`
	AllowEmptyDeletes bool                   `yaml:"allow_empty_deletes,omitempty"`
}

// Config represents the overall configuration for rules
type Config struct {
	Tables map[string][]RuleConfig `yaml:"tables"`
}

// TransformRule represents a rule that transforms data
type TransformRule struct {
	TableName         string
	ColumnName        string
	Transform         func(*utils.CDCMessage) (*utils.CDCMessage, error)
	Operations        []utils.OperationType
	AllowEmptyDeletes bool
}

// FilterRule represents a rule that filters data
type FilterRule struct {
	TableName         string
	ColumnName        string
	Condition         func(*utils.CDCMessage) bool
	Operations        []utils.OperationType
	AllowEmptyDeletes bool
}

// ExcludeColumnRule removes specified columns from CDC messages
type ExcludeColumnRule struct {
	TableName  string
	ColumnName string
}

// RuleEngine manages and applies rules to data
type RuleEngine struct {
	Rules map[string][]Rule // map of table name to slice of rules
	mutex sync.RWMutex
}

// NewRuleEngine creates a new RuleEngine instance
func NewRuleEngine() *RuleEngine {
	return &RuleEngine{
		Rules: make(map[string][]Rule),
	}
}
