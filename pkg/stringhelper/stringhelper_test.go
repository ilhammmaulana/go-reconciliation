package stringhelper

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetDelimiter(t *testing.T) {
	if d := GetDelimiter("id,name,amount"); d != "," {
		t.Errorf("expected comma, got %s", d)
	}
	if d := GetDelimiter("id;name;amount"); d != ";" {
		t.Errorf("expected semicolon, got %s", d)
	}
	if d := GetDelimiter("id\tname\tamount"); d != "\t" {
		t.Errorf("expected tab, got %s", d)
	}
	if d := GetDelimiter("nodelimiter"); d != "" {
		t.Errorf("expected empty delimiter, got %s", d)
	}
}

func TestCleanFilename(t *testing.T) {
	raw := "trx_2026/09/02 [test] (1).csv"
	cleaned := CleanFilename(raw)
	if cleaned != "trx_2026_09_02__test___1_.csv" {
		t.Errorf("unexpected CleanFilename output: %s", cleaned)
	}
}

func TestExtractDateFromFilename(t *testing.T) {
	cases := map[string]string{
		"trx_2026-10-20.csv": "2026-10-20",
		"report_20261020.csv": "2026-10-20",
		"data_20-10-2026.csv": "2026-10-20",
		"nodatefile.csv": "",
	}
	for name, expected := range cases {
		if got := ExtractDateFromFilename(name); got != expected {
			t.Errorf("ExtractDateFromFilename(%s) = %s, expected %s", name, got, expected)
		}
	}
}

func TestExtractTransactionTypeFromFilename(t *testing.T) {
	if ExtractTransactionTypeFromFilename("report_paid_2026.csv") != "paid" {
		t.Errorf("expected paid")
	}
	if ExtractTransactionTypeFromFilename("report_disbursement.csv") != "payout" {
		t.Errorf("expected payout")
	}
}

func TestRemoveEqualSignAndQuotes(t *testing.T) {
	if got := RemoveEqualSign("=\"12345\""); got != "12345" {
		t.Errorf("RemoveEqualSign failed, got: %s", got)
	}
	if got := RemoveTrailingQuotes("\"hello\""); got != "hello" {
		t.Errorf("RemoveTrailingQuotes failed, got: %s", got)
	}
}

func TestComputeMD5(t *testing.T) {
	tmpDir := t.TempDir()
	fPath := filepath.Join(tmpDir, "test.txt")
	_ = os.WriteFile(fPath, []byte("hello world"), 0644)

	hash, err := ComputeMD5(fPath)
	if err != nil {
		t.Fatalf("ComputeMD5 error: %v", err)
	}
	// MD5("hello world") = 5eb63bbbe01eeed093cb22bb8f5acdc3
	if hash != "5eb63bbbe01eeed093cb22bb8f5acdc3" {
		t.Errorf("unexpected md5: %s", hash)
	}
}
