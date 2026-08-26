package sqlite

import (
	"fmt"
	"strconv"
	"strings"
)

func sqliteAffinity(declaredType string) string {
	declaredType = strings.ToUpper(declaredType)
	switch {
	case strings.Contains(declaredType, "INT"):
		return affinityInteger
	case containsAny(declaredType, "CHAR", "CLOB", affinityText):
		return affinityText
	case declaredType == "", strings.Contains(declaredType, affinityBlob):
		return affinityBlob
	case containsAny(declaredType, "REAL", "FLOA", doubleTypeToken):
		return "REAL"
	default:
		return "NUMERIC"
	}
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}

	return false
}

func normalizeDefault(value string) string {
	value = strings.TrimSpace(value)
	for len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}

	if integer, err := strconv.ParseInt(value, 10, 64); err == nil {
		return strconv.FormatInt(integer, 10)
	}

	return value
}

func columnsEqual(current, wanted *schemaColumn) bool {
	return current.affinity == wanted.affinity && current.defaultSQL == wanted.defaultSQL &&
		current.notNull == wanted.notNull && current.primaryKey == wanted.primaryKey &&
		(wanted.declaredType == "" || current.declaredType == wanted.declaredType)
}

func describeColumn(current *schemaColumn) string {
	return fmt.Sprintf(
		"declared-type=%q affinity=%s not-null=%t default=%q primary-key-position=%d",
		current.declaredType, current.affinity, current.notNull, current.defaultSQL, current.primaryKey,
	)
}

func equalStrings(left, right []string) bool {
	return strings.Join(left, "\x00") == strings.Join(right, "\x00")
}

func containsForeignKey(foreignKeys []schemaForeignKey, expected *schemaForeignKey) bool {
	for _, current := range foreignKeys {
		if current.table == expected.table && current.onDelete == expected.onDelete &&
			equalStrings(current.from, expected.from) && equalStrings(current.to, expected.to) {
			return true
		}
	}

	return false
}
