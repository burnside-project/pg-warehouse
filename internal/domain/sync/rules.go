package sync

import "fmt"

// DetermineMode decides whether to do a full or incremental sync for a table.
// If there is no previous watermark or no watermark column configured, it falls back to full.
func DetermineMode(watermarkColumn string, lastWatermark string) SyncMode {
	if watermarkColumn == "" || lastWatermark == "" {
		return SyncModeFull
	}
	return SyncModeIncremental
}

// ValidateTableName checks that a table name is non-empty.
func ValidateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name must not be empty")
	}
	return nil
}

// ValidatePrimaryKeys checks that at least one primary key is configured for incremental merge.
func ValidatePrimaryKeys(keys []string) error {
	if len(keys) == 0 {
		return fmt.Errorf("at least one primary key column is required for merge operations")
	}
	return nil
}