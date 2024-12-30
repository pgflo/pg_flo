package utils

import "github.com/jackc/pgx/v5/pgtype"

var typeMap = pgtype.NewMap()

// GetOIDFromTypeName converts a PostgreSQL type name to its OID
func GetOIDFromTypeName(typeName string) uint32 {
	dt, ok := typeMap.TypeForName(typeName)
	if !ok {
		return pgtype.TextOID
	}
	return dt.OID
}
