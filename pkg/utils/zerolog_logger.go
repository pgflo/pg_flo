// Package utils provides common utilities and data structures for pg_flo.
package utils //nolint:revive // utils is a standard package name

import (
	"github.com/rs/zerolog"
)

// ZerologLogger implements the Logger interface using zerolog
type ZerologLogger struct {
	logger zerolog.Logger
}

// NewZerologLogger creates a new ZerologLogger instance
func NewZerologLogger(logger zerolog.Logger) Logger {
	return &ZerologLogger{logger: logger}
}

// ZerologLogEvent implements the LogEvent interface using zerolog
type ZerologLogEvent struct {
	event *zerolog.Event
}

// Debug creates a debug-level log event
func (z *ZerologLogger) Debug() LogEvent {
	return &ZerologLogEvent{event: z.logger.Debug()}
}

// Info creates an info-level log event
func (z *ZerologLogger) Info() LogEvent {
	return &ZerologLogEvent{event: z.logger.Info()}
}

// Warn creates a warning-level log event
func (z *ZerologLogger) Warn() LogEvent {
	return &ZerologLogEvent{event: z.logger.Warn()}
}

func (z *ZerologLogger) Error() LogEvent {
	return &ZerologLogEvent{event: z.logger.Error()}
}

// Err creates an error-level log event with the given error
func (z *ZerologLogger) Err(err error) LogEvent {
	return &ZerologLogEvent{event: z.logger.Err(err)}
}

// Str adds a string field to the log event
func (e *ZerologLogEvent) Str(key, val string) LogEvent {
	e.event = e.event.Str(key, val)
	return e
}

// Int adds an integer field to the log event
func (e *ZerologLogEvent) Int(key string, val int) LogEvent {
	e.event = e.event.Int(key, val)
	return e
}

// Int64 adds a 64-bit integer field to the log event
func (e *ZerologLogEvent) Int64(key string, val int64) LogEvent {
	e.event = e.event.Int64(key, val)
	return e
}

// Uint32 adds a 32-bit unsigned integer field to the log event
func (e *ZerologLogEvent) Uint32(key string, val uint32) LogEvent {
	e.event = e.event.Uint32(key, val)
	return e
}

// Uint64 adds a 64-bit unsigned integer field to the log event
func (e *ZerologLogEvent) Uint64(key string, val uint64) LogEvent {
	e.event = e.event.Uint64(key, val)
	return e
}

// Float64 adds a 64-bit float field to the log event
func (e *ZerologLogEvent) Float64(key string, val float64) LogEvent {
	e.event = e.event.Float64(key, val)
	return e
}

// Interface adds an interface field to the log event
func (e *ZerologLogEvent) Interface(key string, val interface{}) LogEvent {
	e.event = e.event.Interface(key, val)
	return e
}

// Err adds an error field to the log event
func (e *ZerologLogEvent) Err(err error) LogEvent {
	e.event = e.event.Err(err)
	return e
}

// Msg sets the message for the log event and emits it
func (e *ZerologLogEvent) Msg(msg string) {
	e.event.Msg(msg)
}

// Msgf sets a formatted message for the log event and emits it
func (e *ZerologLogEvent) Msgf(format string, v ...interface{}) {
	e.event.Msgf(format, v...)
}

// Strs adds a string slice field to the log event
func (e *ZerologLogEvent) Strs(key string, vals []string) LogEvent {
	e.event = e.event.Strs(key, vals)
	return e
}

// Any adds an arbitrary field to the log event
func (e *ZerologLogEvent) Any(key string, val interface{}) LogEvent {
	e.event = e.event.Interface(key, val)
	return e
}

// Uint8 adds an 8-bit unsigned integer field to the log event
func (e *ZerologLogEvent) Uint8(key string, val uint8) LogEvent {
	e.event = e.event.Uint8(key, val)
	return e
}

// Type adds a type field to the log event
func (e *ZerologLogEvent) Type(key string, val interface{}) LogEvent {
	e.event = e.event.Type(key, val)
	return e
}
