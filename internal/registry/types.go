package registry

import "time"

type BuildRecord struct {
	BuildID        int64
	ReleaseName    string
	ReleaseVersion string
	GitCommit      string
	Environment    string
	Status         string
	ModelCount     int
	RowCount       int64
	StartedAt      time.Time
	FinishedAt     *time.Time
	DurationMs     int64
}

type PromotionRecord struct {
	ReleaseName    string
	ReleaseVersion string
	Environment    string
	BuildID        int64
	PromotedBy     string
	PromotedAt     time.Time
}
