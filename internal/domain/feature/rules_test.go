package feature

import "testing"

func TestValidateTargetTable(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{"empty", "", true},
		{"raw schema rejected", "raw.orders", true},
		{"stage schema rejected", "stage.orders", true},
		{"v0 schema rejected", "v0.orders", true},
		{"_meta schema rejected", "_meta.versions", true},
		{"silver schema", "silver.customer_360", false},
		{"feat schema", "feat.customer_features", false},
		{"v1 schema", "v1.order_enriched", false},
		{"v2 schema", "v2.order_enriched", false},
		{"current schema", "current.order_enriched", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTargetTable(tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTargetTable(%q) error = %v, wantErr %v", tt.target, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTargetSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{"raw blocked", "raw", true},
		{"stage blocked", "stage", true},
		{"v0 blocked", "v0", true},
		{"_meta blocked", "_meta", true},
		{"v1 allowed", "v1", false},
		{"v2 allowed", "v2", false},
		{"silver allowed", "silver", false},
		{"feat allowed", "feat", false},
		{"current allowed", "current", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTargetSchema(tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTargetSchema(%q) error = %v, wantErr %v", tt.schema, err, tt.wantErr)
			}
		})
	}
}

func TestValidateSQLFile(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty", "", true},
		{"no extension", "features", true},
		{"wrong extension", "features.txt", true},
		{"correct", "features.sql", false},
		{"uppercase", "FEATURES.SQL", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSQLFile(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSQLFile(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestValidateOutputType(t *testing.T) {
	tests := []struct {
		name     string
		fileType string
		wantErr  bool
	}{
		{"parquet", "parquet", false},
		{"csv", "csv", false},
		{"json", "json", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOutputType(tt.fileType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateOutputType(%q) error = %v, wantErr %v", tt.fileType, err, tt.wantErr)
			}
		})
	}
}
