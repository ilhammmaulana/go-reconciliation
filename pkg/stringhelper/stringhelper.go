package stringhelper

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	dateRegexes = []*regexp.Regexp{
		regexp.MustCompile(`(\d{4})[-_](\d{2})[-_](\d{2})`), // 2026-09-02 or 2026_09_02
		regexp.MustCompile(`(\d{4})(\d{2})(\d{2})`),         // 20260902
		regexp.MustCompile(`(\d{2})[-_](\d{2})[-_](\d{4})`), // 02-09-2026
	}
	cleanFilenameRegex = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	equalSignRegex     = regexp.MustCompile(`^=\s*"?([^"]*)"?$`)
)

// GetDelimiter auto-detects delimiter from header string
func GetDelimiter(line string) string {
	delimiters := []string{";", ",", "\t", "|"}
	maxCount := 0
	chosen := ""

	for _, d := range delimiters {
		count := strings.Count(line, d)
		if count > maxCount {
			maxCount = count
			chosen = d
		}
	}

	if maxCount == 0 {
		return ""
	}
	return chosen
}

// CleanFilename removes invalid characters from filename
func CleanFilename(filename string) string {
	return cleanFilenameRegex.ReplaceAllString(filename, "_")
}

// ExtractDateFromFilename extracts YYYY-MM-DD from filename
func ExtractDateFromFilename(filename string) string {
	for _, re := range dateRegexes {
		matches := re.FindStringSubmatch(filename)
		if len(matches) == 4 {
			// Check if first is 4 digits (year)
			if len(matches[1]) == 4 {
				return matches[1] + "-" + matches[2] + "-" + matches[3]
			}
			// Or 3rd is 4 digits (year)
			if len(matches[3]) == 4 {
				return matches[3] + "-" + matches[2] + "-" + matches[1]
			}
		}
	}
	return ""
}

// ExtractTransactionTypeFromFilename detects paid/payout
func ExtractTransactionTypeFromFilename(filename string) string {
	lower := strings.ToLower(filename)
	if strings.Contains(lower, "payout") || strings.Contains(lower, "disbursement") || strings.Contains(lower, "out") {
		return "payout"
	}
	if strings.Contains(lower, "paid") || strings.Contains(lower, "payment") || strings.Contains(lower, "in") {
		return "paid"
	}
	return ""
}

// RemoveEqualSign removes leading excel formulas like ="12345"
func RemoveEqualSign(val string) string {
	val = strings.TrimSpace(val)
	if strings.HasPrefix(val, "=\"") && strings.HasSuffix(val, "\"") {
		return strings.TrimSuffix(strings.TrimPrefix(val, "=\""), "\"")
	}
	if strings.HasPrefix(val, "=") {
		return strings.TrimPrefix(val, "=")
	}
	return val
}

// RemoveTrailingQuotes strips surrounding quotes
func RemoveTrailingQuotes(val string) string {
	return strings.Trim(strings.TrimSpace(val), "\"")
}

// JoinPaths joins multiple path segments cleanly
func JoinPaths(elem ...string) string {
	return filepath.Clean(filepath.Join(elem...))
}

// ComputeMD5 computes md5 checksum of a file
func ComputeMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// NormalizeCsvContent standardizes CRLF, removes UTF-8 BOM
func NormalizeCsvContent(content string) string {
	// Strip UTF-8 BOM
	content = strings.TrimPrefix(content, "\xef\xbb\xbf")
	// Standardize line breaks
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	return content
}
