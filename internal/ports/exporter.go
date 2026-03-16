package ports

import "context"

// Exporter defines the contract for exporting data to a file format.
type Exporter interface {
	// Export writes the given table data to the specified output path.
	Export(ctx context.Context, table string, path string) error

	// FileType returns the file type this exporter handles (e.g., "parquet", "csv").
	FileType() string
}
