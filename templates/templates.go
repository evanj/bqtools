package templates

import (
	"math"

	"strconv"
)

// Determine the lowest x such that x/divisor rounded to 1 decimal place == 1.0
func leastRoundedOne(divisor int64) int64 {
	roundUpValue := float64(divisor) * 0.95
	return int64(math.Ceil(roundUpValue))
}

type unit struct {
	divisor         int64
	suffix          string
	leastRoundedOne int64
}

var siByteUnits = [...]unit{
	{1024, "Ki", leastRoundedOne(1024)},
	{1024 * 1024, "Mi", leastRoundedOne(1024 * 1024)},
	{1024 * 1024 * 1024, "Gi", leastRoundedOne(1024 * 1024 * 1024)},
	{1024 * 1024 * 1024 * 1024, "Ti", leastRoundedOne(1024 * 1024 * 1024 * 1024)},
	{1024 * 1024 * 1024 * 1024 * 1024, "Pi", leastRoundedOne(1024 * 1024 * 1024 * 1024 * 1024)},
}

func HumanBytes(bytes int64) string {
	last := unit{1, "", 0}
	for _, byteUnit := range siByteUnits {
		// edge case: we round to one decimal place; if this rounds up:
		if bytes < byteUnit.leastRoundedOne {
			break
		}
		last = byteUnit
	}

	if last.divisor == 1 {
		return strconv.FormatInt(bytes, 10) + " B"
	}
	value := float64(bytes) / float64(last.divisor)
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + last.suffix + "B"
}
