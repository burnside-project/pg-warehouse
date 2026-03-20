package models

import "time"

// SilverVersion represents a versioned schema in silver.duckdb.
type SilverVersion struct {
	Version     int        `json:"version"`
	Label       string     `json:"label"`
	Status      string     `json:"status"` // "active", "archived", "experiment"
	EpochID     *int64     `json:"epoch_id,omitempty"`
	Description string     `json:"description,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	PromotedAt  *time.Time `json:"promoted_at,omitempty"`
}

const (
	SilverVersionActive     = "active"
	SilverVersionArchived   = "archived"
	SilverVersionExperiment = "experiment"
)
