package utils

import (
	"fmt"
	"time"
)

// ParsePostgresDuration parses a Postgres duration string like "90min", "1h", "1050ms"
// and converts it to time.Duration.
//
// It is used to parse various settings that represent duration, i.e. wal_sender_timeout
func ParsePostgresDuration(s string) (time.Duration, error) {
	var value int64
	var unit string
	_, err := fmt.Sscanf(s, "%d%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}

	switch unit {
	case "us":
		return time.Duration(value) * time.Microsecond, nil
	case "ms":
		return time.Duration(value) * time.Millisecond, nil
	case "s":
		return time.Duration(value) * time.Second, nil
	case "min":
		return time.Duration(value) * time.Minute, nil
	case "h":
		return time.Duration(value) * time.Hour, nil
	case "d":
		return time.Duration(value) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}
}
