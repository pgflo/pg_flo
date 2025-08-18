package sinks

import "github.com/jackc/pglogrepl"

// Status represents the status of a sink operation
type Status struct {
	LastLSN pglogrepl.LSN `json:"last_lsn"`
}
