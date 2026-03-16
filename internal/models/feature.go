package models

import "time"

// FeatureRun represents a feature job execution record.
type FeatureRun struct {
	RunID        string     `db:"run_id"`
	SQLFile      string     `db:"sql_file"`
	TargetTable  string     `db:"target_table"`
	OutputPath   string     `db:"output_path"`
	OutputType   string     `db:"output_type"`
	StartedAt    time.Time  `db:"started_at"`
	FinishedAt   *time.Time `db:"finished_at"`
	RowCount     int64      `db:"row_count"`
	Status       string     `db:"status"`
	ErrorMessage string     `db:"error_message"`
}

// FeatureDependency represents a feature dependency record.
type FeatureDependency struct {
	RunID       string `db:"run_id"`
	SourceTable string `db:"source_table"`
	TargetTable string `db:"target_table"`
}
