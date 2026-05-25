package gormcache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const keySep = ":"

type cacheKey struct {
	table string
	key   string
	pk    string
	// digest is the SHA-256 digest of statement SQL and vars, reused for
	// search cache keys and singleflight keys within one query.
	digest     string
	isPrimary  bool
	tableToken tableToken
	rowToken   rowToken
}

func buildPrimaryKey(prefix string, token rowToken) string {
	var builder strings.Builder
	builder.Grow(len(prefix) + len(token.table) + len(token.pk) + 63)
	builder.WriteString(prefix)
	writeKeyPart(&builder, "g")
	writeKeyPart(&builder, token.global)
	writeKeyPart(&builder, token.table)
	writeKeyPart(&builder, "sweep")
	writeKeyPart(&builder, token.sweep)
	writeKeyPart(&builder, "p")
	writeKeyPart(&builder, token.pk)
	writeKeyPart(&builder, token.epoch)
	return builder.String()
}

func buildSearchKey(prefix string, token tableToken, digest string) string {
	var builder strings.Builder
	builder.Grow(len(prefix) + len(token.table) + len(digest) + 40)
	builder.WriteString(prefix)
	writeKeyPart(&builder, "g")
	writeKeyPart(&builder, token.global)
	writeKeyPart(&builder, token.table)
	writeKeyPart(&builder, token.epoch)
	writeKeyPart(&builder, "s")
	writeKeyPart(&builder, digest)
	return builder.String()
}

func buildSingleflightKey(prefix string, stm *gorm.Statement, digest string) string {
	destType := "nil"
	if stm != nil && stm.Dest != nil {
		destType = reflect.TypeOf(stm.Dest).String()
	}
	table := tableName(stm)
	var builder strings.Builder
	builder.Grow(len(prefix) + len(table) + len(destType) + len(digest) + 8)
	builder.WriteString(prefix)
	writeKeyPart(&builder, "sf")
	writeKeyPart(&builder, table)
	writeKeyPart(&builder, destType)
	writeKeyPart(&builder, digest)
	return builder.String()
}

func digestStatement(stm *gorm.Statement) string {
	h := sha256.New()
	if stm != nil {
		_, _ = h.Write([]byte(stm.SQL.String()))
	}
	_, _ = h.Write([]byte{0})
	if stm != nil {
		for _, val := range stm.Vars {
			writeDigestValue(h, val)
			_, _ = h.Write([]byte{0})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func isDigits(val string) bool {
	if val == "" {
		return false
	}
	for i := 0; i < len(val); i++ {
		ch := val[i]
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func isGormPrimaryShortcut(colValue any) bool {
	col, ok := colValue.(clause.Column)
	return ok && col.Name == "~~~py~~~"
}

func isPKColumn(colValue any, pkName string) bool {
	col, ok := colValue.(clause.Column)
	return ok && col.Name == pkName
}

func keyForStatement(prefix string, epochs *tableEpochs, token tableToken, stm *gorm.Statement, digest string) cacheKey {
	pk := primaryFromWhere(stm)
	if pk != "" {
		row := epochs.snapshotRow(token.table, pk, token.global)
		return cacheKey{
			table:      token.table,
			key:        buildPrimaryKey(prefix, row),
			pk:         pk,
			digest:     digest,
			isPrimary:  true,
			tableToken: token,
			rowToken:   row,
		}
	}
	if digest == "" {
		digest = digestStatement(stm)
	}
	return cacheKey{
		table:      token.table,
		key:        buildSearchKey(prefix, token, digest),
		digest:     digest,
		tableToken: token,
	}
}

func pkFromEq(expr clause.Eq, pkName string) string {
	if !isPKColumn(expr.Column, pkName) {
		return ""
	}
	return fmt.Sprint(expr.Value)
}

func pkFromExpr(expr clause.Expr, pkName string) string {
	sql := strings.TrimSpace(expr.SQL)
	idx := strings.IndexByte(sql, '=')
	if idx <= 0 || idx >= len(sql)-1 {
		return ""
	}

	left := strings.Trim(strings.TrimSpace(sql[:idx]), "`\"")
	right := strings.TrimSpace(sql[idx+1:])
	if left != pkName {
		return ""
	}
	if right == "?" {
		if len(expr.Vars) != 1 {
			return ""
		}
		return fmt.Sprint(expr.Vars[0])
	}
	if !isDigits(right) {
		return ""
	}
	return right
}

func pkFromExpression(expr clause.Expression, pkName string, onlyExpr bool) string {
	switch val := expr.(type) {
	case clause.Eq:
		return pkFromEq(val, pkName)
	case clause.IN:
		return pkFromIN(val, pkName, onlyExpr)
	case clause.Expr:
		return pkFromExpr(val, pkName)
	default:
		return ""
	}
}

func pkFromIN(expr clause.IN, pkName string, onlyExpr bool) string {
	if len(expr.Values) != 1 {
		return ""
	}
	if isPKColumn(expr.Column, pkName) {
		return fmt.Sprint(expr.Values[0])
	}
	if !onlyExpr || !isGormPrimaryShortcut(expr.Column) {
		return ""
	}
	return fmt.Sprint(expr.Values[0])
}

func primaryFromWhere(stm *gorm.Statement) string {
	if stm == nil || stm.Schema == nil || len(stm.Schema.PrimaryFields) != 1 {
		return ""
	}

	clauseValue, ok := stm.Clauses["WHERE"]
	if !ok {
		return ""
	}
	where, ok := clauseValue.Expression.(clause.Where)
	if !ok || len(where.Exprs) == 0 {
		return ""
	}

	pkName := stm.Schema.PrimaryFields[0].DBName
	found := ""
	for _, expr := range where.Exprs {
		next := pkFromExpression(expr, pkName, len(where.Exprs) == 1)
		if next == "" {
			return ""
		}
		if found != "" && found != next {
			return ""
		}
		found = next
	}
	return found
}

func tableName(stm *gorm.Statement) string {
	if stm == nil {
		return ""
	}
	return stm.Table
}

func writeDigestValue(h hash.Hash, val any) {
	_, _ = fmt.Fprintf(h, "%T=", val)
	switch typed := val.(type) {
	case nil:
		_, _ = h.Write([]byte("<nil>"))
	case string:
		writeLargeDigestValue(h, []byte(typed))
	case []byte:
		writeLargeDigestValue(h, typed)
	case fmt.Stringer:
		writeLargeDigestValue(h, []byte(typed.String()))
	default:
		_, _ = fmt.Fprintf(h, "%v", val)
	}
}

func writeKeyPart(builder *strings.Builder, part string) {
	builder.WriteString(keySep)
	builder.WriteString(part)
}

func writeLargeDigestValue(h hash.Hash, val []byte) {
	sum := sha256.Sum256(val)
	_, _ = fmt.Fprintf(h, "len=%d;sha256=%x", len(val), sum)
}
