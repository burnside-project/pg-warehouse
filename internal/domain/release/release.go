package release

// Release is a versioned grouping of models built against specific contracts.
type Release struct {
	Name        string            `yaml:"name"`
	Version     string            `yaml:"version"`
	Description string            `yaml:"description"`
	Environment string            `yaml:"environment"`
	Models      []string          `yaml:"models"`
	Input       ReleaseInput      `yaml:"input"`
	Output      ReleaseOutput     `yaml:"output"`
	Validation  ReleaseValidation `yaml:"validation"`
	Promotion   ReleasePromotion  `yaml:"promotion"`
	FilePath    string            `yaml:"-"`
}

// ReleaseInput defines the input contracts and version for a release.
type ReleaseInput struct {
	SilverVersion string   `yaml:"silver_version"`
	Contracts     []string `yaml:"contracts"`
}

// ReleaseOutput defines the output target for a release.
type ReleaseOutput struct {
	Target              string `yaml:"target"`
	Path                string `yaml:"path"`
	RegisterInFeatureDB bool   `yaml:"register_in_feature_db"`
}

// ReleaseValidation defines validation rules for a release.
type ReleaseValidation struct {
	RequireGitCommit    bool `yaml:"require_git_commit"`
	FailOnChecksumDrift bool `yaml:"fail_on_checksum_drift"`
}

// ReleasePromotion defines promotion rules for a release.
type ReleasePromotion struct {
	Allow []string `yaml:"allow"`
}
