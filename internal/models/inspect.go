package models

// TableInfo holds basic information about a warehouse table.
type TableInfo struct {
	Schema   string
	Name     string
	RowCount int64
}

// ColumnInfo holds information about a table column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Position int
}

// TableSchema holds the full schema of a table.
type TableSchema struct {
	Schema  string
	Name    string
	Columns []ColumnInfo
}
