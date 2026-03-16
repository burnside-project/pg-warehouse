package postgres

import "testing"

func TestQuoteTable(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"orders", `"orders"`},
		{"public.orders", `"public"."orders"`},
		{"my_schema.my_table", `"my_schema"."my_table"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := quoteTable(tt.input)
			if got != tt.expected {
				t.Errorf("quoteTable(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
