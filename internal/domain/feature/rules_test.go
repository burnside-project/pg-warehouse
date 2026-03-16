package feature

import "testing"

func TestValidateTargetTable(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{"empty", "", true},
		{"wrong schema", "raw.orders", true},
		{"correct schema", "feat.customer_features", false},
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
