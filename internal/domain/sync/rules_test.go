package sync

import "testing"

func TestDetermineMode_Full(t *testing.T) {
	tests := []struct {
		name            string
		modeOverride    string
		watermarkColumn string
		lastWatermark   string
		want            SyncMode
	}{
		{"no watermark column", "", "", "", SyncModeFull},
		{"no last watermark", "", "updated_at", "", SyncModeFull},
		{"both empty", "", "", "", SyncModeFull},
		{"explicit full override", "full", "updated_at", "2024-01-01T00:00:00Z", SyncModeFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetermineMode(tt.modeOverride, tt.watermarkColumn, tt.lastWatermark)
			if got != tt.want {
				t.Errorf("DetermineMode(%q, %q, %q) = %v, want %v", tt.modeOverride, tt.watermarkColumn, tt.lastWatermark, got, tt.want)
			}
		})
	}
}

func TestDetermineMode_Incremental(t *testing.T) {
	got := DetermineMode("", "updated_at", "2024-01-01T00:00:00Z")
	if got != SyncModeIncremental {
		t.Errorf("DetermineMode with watermark = %v, want %v", got, SyncModeIncremental)
	}
}

func TestValidateTableName(t *testing.T) {
	if err := ValidateTableName(""); err == nil {
		t.Error("expected error for empty table name")
	}
	if err := ValidateTableName("orders"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidatePrimaryKeys(t *testing.T) {
	if err := ValidatePrimaryKeys(nil); err == nil {
		t.Error("expected error for nil primary keys")
	}
	if err := ValidatePrimaryKeys([]string{}); err == nil {
		t.Error("expected error for empty primary keys")
	}
	if err := ValidatePrimaryKeys([]string{"id"}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
