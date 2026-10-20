package formatter

import (
	"testing"
)

func TestConvertToDateTime(t *testing.T) {
	tests := []struct {
		input       string
		target      string
		expectedStr string
		expectErr   bool
	}{
		{"2026-10-20 23:59:56", "2006-01-02 15:04:05", "2026-10-20 23:59:56", false},
		{"2026-10-20T23:59:56", "2006-01-02", "2026-10-20", false},
		{"20/10/2026 15:04:05", "2006-01-02", "2026-10-20", false},
		{"", "2006-01-02", "", true},
		{"invalid-date", "2006-01-02", "invalid-date", true},
	}

	for _, tc := range tests {
		formatted, _, err := ConvertToDateTime(tc.input, tc.target)
		if (err != nil) != tc.expectErr {
			t.Errorf("ConvertToDateTime(%q) unexpected error state: %v", tc.input, err)
		}
		if !tc.expectErr && formatted != tc.expectedStr {
			t.Errorf("ConvertToDateTime(%q) = %q, expected %q", tc.input, formatted, tc.expectedStr)
		}
	}
}

func TestConvertToDateOnly(t *testing.T) {
	if got := ConvertToDateOnly("2026-10-20 23:59:56"); got != "2026-10-20" {
		t.Errorf("ConvertToDateOnly failed, got: %s", got)
	}
	if got := ConvertToDateOnly("invalid"); got != "" {
		t.Errorf("ConvertToDateOnly expected empty, got: %s", got)
	}
}

func TestConvertToNumber(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"1,000,000.50", 1000000.50},
		{"1.000.000,50", 1000000.50},
		{"789731", 789731},
		{"", 0},
		{"50000", 50000},
	}

	for _, tc := range tests {
		got := ConvertToNumber(tc.input)
		if got != tc.expected {
			t.Errorf("ConvertToNumber(%q) = %f, expected %f", tc.input, got, tc.expected)
		}
	}
}

func TestConvertToColumnName(t *testing.T) {
	if got := ConvertToColumnName("Total Amount"); got != "total_amount" {
		t.Errorf("ConvertToColumnName failed, got: %s", got)
	}
	if got := ConvertToColumnName("Vendor-Ref-No"); got != "vendor_ref_no" {
		t.Errorf("ConvertToColumnName failed, got: %s", got)
	}
}
