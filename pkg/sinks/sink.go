package sinks

import (
	"github.com/pgflo/pg_flo/pkg/utils"
)

// Sink defines the interface for message output destinations
type Sink interface {
	WriteBatch(data []*utils.CDCMessage) error
}
