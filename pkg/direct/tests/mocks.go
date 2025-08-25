// Package tests provides test utilities for the direct package
package tests

import (
	"github.com/pgflo/pg_flo/pkg/utils"
)

//nolint:revive // Test mock doesn't need comments
type MockLogger struct{}

//nolint:revive // Test mock methods don't need comments
func (m *MockLogger) Debug() utils.LogEvent { return &MockLogEvent{} }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogger) Info() utils.LogEvent { return &MockLogEvent{} }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogger) Warn() utils.LogEvent { return &MockLogEvent{} }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogger) Error() utils.LogEvent { return &MockLogEvent{} }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogger) Err(_ error) utils.LogEvent { return &MockLogEvent{} }

//nolint:revive // Test mock doesn't need comments
type MockLogEvent struct{}

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Str(_, _ string) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Int(_ string, _ int) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Int64(_ string, _ int64) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Uint8(_ string, _ uint8) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Uint32(_ string, _ uint32) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Uint64(_ string, _ uint64) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Float64(_ string, _ float64) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Interface(_ string, _ interface{}) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Err(_ error) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Strs(_ string, _ []string) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Any(_ string, _ interface{}) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Type(_ string, _ interface{}) utils.LogEvent { return m }

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Msg(_ string) {}

//nolint:revive // Test mock methods don't need comments
func (m *MockLogEvent) Msgf(_ string, _ ...interface{}) {}
