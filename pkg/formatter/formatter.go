package formatter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	jakartaLocation *time.Location
	cleanNumberRe   = regexp.MustCompile(`[^0-9.-]`)
	cleanColRe      = regexp.MustCompile(`[^a-z0-9_]`)
)

func init() {
	loc, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		loc = time.FixedZone("WIB", 7*3600)
	}
	jakartaLocation = loc
}

var dateFormats = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04:05Z07:00",
	"2006/01/02 15:04:05",
	"02/01/2006 15:04:05",
	"02-01-2006 15:04:05",
	"2006-01-02",
	"02/01/2006",
	"02-01-2006",
	"02 Jan 2006 15:04:05",
	"02 January 2006 15:04:05",
	"20060102150405",
	"20060102",
}

// ConvertToDateTime parses raw date string and returns formatted UTC or Asia/Jakarta string
func ConvertToDateTime(raw string, targetLayout string) (string, time.Time, error) {
	raw = strings.Trim(strings.TrimSpace(raw), "\"")
	if raw == "" {
		return "", time.Time{}, fmt.Errorf("empty date")
	}

	for _, layout := range dateFormats {
		if t, err := time.ParseInLocation(layout, raw, jakartaLocation); err == nil {
			if targetLayout == "" {
				targetLayout = "2006-01-02 15:04:05"
			}
			return t.Format(targetLayout), t, nil
		}
	}

	return raw, time.Time{}, fmt.Errorf("cannot parse date: %s", raw)
}

// ConvertToDateOnly parses raw date and returns YYYY-MM-DD
func ConvertToDateOnly(raw string) string {
	formatted, t, err := ConvertToDateTime(raw, "2006-01-02")
	if err != nil {
		return ""
	}
	_ = t
	return formatted
}

// ConvertToNumber converts amount string to float64 or clean numeric string
func ConvertToNumber(raw string) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}

	// Handle standard Indonesian 1.000.000,00 or US 1,000,000.00
	if strings.Contains(raw, ",") && strings.Contains(raw, ".") {
		lastComma := strings.LastIndex(raw, ",")
		lastDot := strings.LastIndex(raw, ".")
		if lastComma > lastDot {
			// Indonesian style 1.000.000,00
			raw = strings.ReplaceAll(raw, ".", "")
			raw = strings.ReplaceAll(raw, ",", ".")
		} else {
			// US style 1,000,000.00
			raw = strings.ReplaceAll(raw, ",", "")
		}
	} else if strings.Contains(raw, ",") {
		// Only comma, could be decimal (100,50) or thousand (1,000)
		parts := strings.Split(raw, ",")
		if len(parts) == 2 && len(parts[1]) <= 2 {
			raw = strings.ReplaceAll(raw, ",", ".")
		} else {
			raw = strings.ReplaceAll(raw, ",", "")
		}
	}

	cleaned := cleanNumberRe.ReplaceAllString(raw, "")
	val, err := strconv.ParseFloat(cleaned, 64)
	if err != nil {
		return 0
	}
	return val
}

// ConvertToColumnName normalizes raw header into snake_case
func ConvertToColumnName(raw string) string {
	lower := strings.ToLower(strings.Trim(strings.TrimSpace(raw), "\""))
	lower = strings.ReplaceAll(lower, " ", "_")
	lower = strings.ReplaceAll(lower, "-", "_")
	return cleanColRe.ReplaceAllString(lower, "")
}
