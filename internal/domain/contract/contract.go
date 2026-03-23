package contract

import "fmt"

// Contract defines a stable input/output analytical shape.
type Contract struct {
	Name        string           `yaml:"name"`
	Version     int              `yaml:"version"`
	Layer       string           `yaml:"layer"`
	ObjectName  string           `yaml:"object_name"`
	Owner       string           `yaml:"owner"`
	Status      string           `yaml:"status"`
	Description string           `yaml:"description"`
	Grain       string           `yaml:"grain"`
	PrimaryKey  []string         `yaml:"primary_key"`
	Columns     []ContractColumn `yaml:"columns"`
	FilePath    string           `yaml:"-"`
}

// ContractColumn defines a column in a contract.
type ContractColumn struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Nullable bool   `yaml:"nullable"`
}

// QualifiedName returns "layer.name@vN" e.g. "silver.customer_orders@v1"
func (c *Contract) QualifiedName() string {
	return fmt.Sprintf("%s.%s@v%d", c.Layer, c.Name, c.Version)
}
