package model

// Model is a user-authored SQL asset with dependency metadata.
type Model struct {
	Name            string
	FilePath        string
	Checksum        string
	Materialization string   // "table", "view", "parquet"
	Layer           string   // derived from directory: "silver", "marts", "features"
	DependsOn       []string // extracted ref() names
	Sources         []string // extracted source() names
	Contract        string   // optional contract this model satisfies
	Tags            []string
}
