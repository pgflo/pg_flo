package routing

import (
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ColumnMapping struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
}

type TableRoute struct {
	SourceTable      string                `yaml:"source_table"`
	DestinationTable string                `yaml:"destination_table"`
	ColumnMappings   []ColumnMapping       `yaml:"column_mappings"`
	Operations       []utils.OperationType `yaml:"operations"`
}

type Router struct {
	Routes map[string]TableRoute
	mutex  sync.RWMutex
	logger zerolog.Logger
}

func NewRouter() *Router {
	return &Router{
		Routes: make(map[string]TableRoute),
		logger: log.With().Str("component", "router").Logger(),
	}
}

func (r *Router) AddRoute(route TableRoute) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.Routes[route.SourceTable] = route
}

func (r *Router) ApplyRouting(message *utils.CDCMessage) (*utils.CDCMessage, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	route, exists := r.Routes[message.Table]
	if !exists {
		return message, nil
	}

	if !ContainsOperation(route.Operations, message.Type) {
		return nil, nil
	}

	routedMessage := *message
	routedMessage.Table = route.DestinationTable

	if len(route.ColumnMappings) > 0 {
		// Build mapping lookup once for efficiency
		columnMap := make(map[string]string, len(route.ColumnMappings))
		for _, mapping := range route.ColumnMappings {
			columnMap[mapping.Source] = mapping.Destination
		}

		// Only copy and modify if mappings actually apply
		hasChanges := false
		for _, col := range message.Columns {
			if _, exists := columnMap[col.Name]; exists {
				hasChanges = true
				break
			}
		}

		if hasChanges {
			newColumns := make([]*pglogrepl.RelationMessageColumn, len(message.Columns))
			for i, col := range message.Columns {
				if mappedName, exists := columnMap[col.Name]; exists {
					newCol := *col
					newCol.Name = mappedName
					newColumns[i] = &newCol
				} else {
					newColumns[i] = col
				}
			}
			routedMessage.Columns = newColumns

			// Update replication key columns
			if routedMessage.ReplicationKey.Type != utils.ReplicationKeyFull {
				for i, keyCol := range routedMessage.ReplicationKey.Columns {
					if mappedName, exists := columnMap[keyCol]; exists {
						routedMessage.ReplicationKey.Columns[i] = mappedName
					}
				}
			}
		}
	}

	return &routedMessage, nil
}

// ContainsOperation checks if the given operation is in the list of operations
func ContainsOperation(operations []utils.OperationType, operation utils.OperationType) bool {
	for _, op := range operations {
		if op == operation {
			return true
		}
	}
	return false
}

// GetMappedColumnName returns the destination column name for a given source column name
func GetMappedColumnName(mappings []ColumnMapping, sourceName string) string {
	for _, mapping := range mappings {
		if mapping.Source == sourceName {
			return mapping.Destination
		}
	}
	return ""
}

// LoadRoutes loads routes from the provided configuration
func (r *Router) LoadRoutes(config map[string]TableRoute) error {
	for sourceName, route := range config {
		r.logger.Info().
			Str("source_table", sourceName).
			Str("destination_table", route.DestinationTable).
			Any("operations", route.Operations).
			Any("column_mappings", route.ColumnMappings).
			Msg("Loading route")

		route.SourceTable = sourceName
		if route.DestinationTable == "" {
			route.DestinationTable = sourceName
		}
		r.AddRoute(route)
	}
	return nil
}
