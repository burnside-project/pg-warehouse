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

func TestAllSchemas(t *testing.T) {
	schemas := AllSchemas()
	if len(schemas) != 3 {
		t.Errorf("AllSchemas() returned %d schemas, want 3", len(schemas))
	}
}
