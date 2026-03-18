package warehouse

import "testing"

func TestRawTableName(t *testing.T) {
	got := RawTableName("orders")
	want := "raw.orders"
	if got != want {
		t.Errorf("RawTableName(orders) = %q, want %q", got, want)
	}
}

func TestStageTableName(t *testing.T) {
	got := StageTableName("orders")
	want := "stage.orders"
	if got != want {
		t.Errorf("StageTableName(orders) = %q, want %q", got, want)
	}
}

func TestFeatTableName(t *testing.T) {
	got := FeatTableName("features")
	want := "feat.features"
	if got != want {
		t.Errorf("FeatTableName(features) = %q, want %q", got, want)
	}
}

func TestSilverTableName(t *testing.T) {
	got := SilverTableName("customer_360")
	want := "silver.customer_360"
	if got != want {
		t.Errorf("SilverTableName(customer_360) = %q, want %q", got, want)
	}
}

func TestAllSchemas(t *testing.T) {
	schemas := AllSchemas()
	if len(schemas) != 4 {
		t.Errorf("AllSchemas() returned %d schemas, want 4", len(schemas))
	}
}
