package parser

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/burnside-project/pg-warehouse/internal/domain/contract"
)

// ContractFile is the YAML structure for contract files.
type ContractFile struct {
	Contract contract.Contract `yaml:"contract"`
}

// ParseContractFile reads a YAML contract file.
func ParseContractFile(path string) (*contract.Contract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read contract file: %w", err)
	}
	var f ContractFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse contract YAML: %w", err)
	}
	f.Contract.FilePath = path
	return &f.Contract, nil
}
