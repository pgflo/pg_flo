package routing

import (
	"sync"

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

	if !ContainsOperation(route.Operations, message.Operation) {
		return nil, nil
	}

	routedMessage := *message
	routedMessage.Table = route.DestinationTable

	if len(route.ColumnMappings) > 0 {
		newColumns := make([]utils.Column, len(message.Columns))
		for i, col := range message.Columns {
			newCol := col
			mappedName := GetMappedColumnName(route.ColumnMappings, col.Name)
			if mappedName != "" {
				newCol.Name = mappedName
			}
			newColumns[i] = newCol
		}
		routedMessage.Columns = newColumns

		newData := make(map[string]interface{})
		newOldData := make(map[string]interface{})
		newColumnTypes := make(map[string]string)

		for oldName, value := range message.Data {
			newName := GetMappedColumnName(route.ColumnMappings, oldName)
			if newName == "" {
				newName = oldName
			}
			newData[newName] = value
			if typeVal, exists := message.ColumnTypes[oldName]; exists {
				newColumnTypes[newName] = typeVal
			}
		}

		for oldName, value := range message.OldData {
			newName := GetMappedColumnName(route.ColumnMappings, oldName)
			if newName == "" {
				newName = oldName
			}
			newOldData[newName] = value
		}

		routedMessage.Data = newData
		routedMessage.OldData = newOldData
		routedMessage.ColumnTypes = newColumnTypes

		if routedMessage.ReplicationKey != nil && routedMessage.ReplicationKey.Type != utils.ReplicationKeyFull {
			mappedColumns := make([]string, len(routedMessage.ReplicationKey.Columns))
			for i, keyCol := range routedMessage.ReplicationKey.Columns {
				mappedName := GetMappedColumnName(route.ColumnMappings, keyCol)
				if mappedName != "" {
					mappedColumns[i] = mappedName
				} else {
					mappedColumns[i] = keyCol
				}
			}
			routedMessage.ReplicationKey.Columns = mappedColumns
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
