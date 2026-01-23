package filter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
	"github.com/zilliztech/milvus-backup/internal/namespace"
)

// filter format
//
// rule 1. key: db1.*
// rule 2. key: db1.coll1
// rule 3. key: coll1, means use default db
// rule 4. key: db1.

var (
	_rule1Regex = regexp.MustCompile(`^(\w+)\.\*$`)
	_rule2Regex = regexp.MustCompile(`^(\w+)\.(\w+)$`)
	_rule3Regex = regexp.MustCompile(`^(\w+)$`)
	_rule4Regex = regexp.MustCompile(`^(\w+)\.$`)
)

// inferFilterRuleType determines the type of filter rule from a string.
// Rule types:
//   - 1: db1.* (all collections in database)
//   - 2: db1.coll1 (specific database and collection)
//   - 3: coll1 (collection in default database)
//   - 4: db1. (database only, empty collection list)
func inferFilterRuleType(rule string) (int, error) {
	if _rule1Regex.MatchString(rule) {
		return 1, nil
	}

	if _rule2Regex.MatchString(rule) {
		return 2, nil
	}

	if _rule3Regex.MatchString(rule) {
		return 3, nil
	}

	if _rule4Regex.MatchString(rule) {
		return 4, nil
	}

	return 0, fmt.Errorf("filter: invalid filter rule: %s", rule)
}

func FromPB(filter map[string]*backuppb.CollFilter) (Filter, error) {
	dbCollFilter := make(map[string]CollFilter)
	for dbName, f := range filter {
		if len(f.GetColls()) == 1 && f.GetColls()[0] == "*" {
			dbCollFilter[dbName] = CollFilter{AllowAll: true}
			continue
		}

		dbCollFilter[dbName] = CollFilter{CollName: make(map[string]struct{}, len(f.GetColls()))}
		for _, coll := range f.GetColls() {
			dbCollFilter[dbName].CollName[coll] = struct{}{}
		}
	}

	return Filter{DBCollFilter: dbCollFilter}, nil
}

func Parse(s string) (Filter, error) {
	if s == "" {
		return Filter{}, nil
	}

	ruleStrs := strings.Split(s, ",")
	dbCollFilter := make(map[string]CollFilter)
	for _, ruleStr := range ruleStrs {
		ruleType, err := inferFilterRuleType(ruleStr)
		if err != nil {
			return Filter{}, err
		}

		switch ruleType {
		case 1:
			db := ruleStr[:len(ruleStr)-2]
			dbCollFilter[db] = CollFilter{AllowAll: true}
		case 2, 3:
			ns, err := namespace.Parse(ruleStr)
			if err != nil {
				return Filter{}, fmt.Errorf("filter: invalid collection name %s", ruleStr)
			}

			if _, ok := dbCollFilter[ns.DBName()]; !ok {
				dbCollFilter[ns.DBName()] = CollFilter{CollName: make(map[string]struct{})}
			}
			dbCollFilter[ns.DBName()].CollName[ns.CollName()] = struct{}{}
		case 4:
			db := ruleStr[:len(ruleStr)-1]
			dbCollFilter[db] = CollFilter{}
		default:
			return Filter{}, fmt.Errorf("invalid filter rule: %s", ruleStr)
		}
	}

	return Filter{DBCollFilter: dbCollFilter}, nil
}

type CollFilter struct {
	AllowAll bool
	CollName map[string]struct{}
}

type Filter struct {
	DBCollFilter map[string]CollFilter
}

func (f Filter) AllowDB(dbName string) bool {
	if f.DBCollFilter == nil {
		return true
	}

	_, ok := f.DBCollFilter[dbName]
	return ok
}

func (f Filter) AllowDBs(dbNames []string) []string {
	var filtered []string
	for _, dbName := range dbNames {
		if f.AllowDB(dbName) {
			filtered = append(filtered, dbName)
		}
	}
	return filtered
}

func (f Filter) AllowNS(ns namespace.NS) bool {
	if f.DBCollFilter == nil {
		return true
	}

	collFilter, ok := f.DBCollFilter[ns.DBName()]
	if !ok {
		return false
	}

	if collFilter.AllowAll {
		return true
	}

	_, ok = collFilter.CollName[ns.CollName()]
	return ok
}

func (f Filter) AllowNSS(nss []namespace.NS) []namespace.NS {
	var filtered []namespace.NS
	for _, ns := range nss {
		if f.AllowNS(ns) {
			filtered = append(filtered, ns)
		}
	}
	return filtered
}

// InferMapperRuleType determines if both key and value match the same rule type.
// Used for rename/mapping operations.
func InferMapperRuleType(k, v string) (int, error) {
	if _rule1Regex.MatchString(k) && _rule1Regex.MatchString(v) {
		return 1, nil
	}

	if _rule2Regex.MatchString(k) && _rule2Regex.MatchString(v) {
		return 2, nil
	}

	if _rule3Regex.MatchString(k) && _rule3Regex.MatchString(v) {
		return 3, nil
	}

	if _rule4Regex.MatchString(k) && _rule4Regex.MatchString(v) {
		return 4, nil
	}

	return 0, fmt.Errorf("filter: invalid mapper rule: %s -> %s", k, v)
}
