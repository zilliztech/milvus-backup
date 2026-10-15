package filter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zilliztech/milvus-backup/internal/collref"
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

// FromDBColls builds the filter from a plain database-to-collections map,
// the shape the transports normalize their filter fields into. The single
// entry ["*"] selects every collection of the database.
func FromDBColls(filter map[string][]string) (Filter, error) {
	dbCollFilter := make(map[string]CollFilter)
	for dbName, colls := range filter {
		if len(colls) == 1 && colls[0] == "*" {
			dbCollFilter[dbName] = CollFilter{AllowAll: true}
			continue
		}

		dbCollFilter[dbName] = CollFilter{CollName: make(map[string]struct{}, len(colls))}
		for _, coll := range colls {
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
			collRef, err := collref.Parse(ruleStr)
			if err != nil {
				return Filter{}, fmt.Errorf("filter: invalid collection name %s", ruleStr)
			}

			if _, ok := dbCollFilter[collRef.DBName()]; !ok {
				dbCollFilter[collRef.DBName()] = CollFilter{CollName: make(map[string]struct{})}
			}
			dbCollFilter[collRef.DBName()].CollName[collRef.CollName()] = struct{}{}
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

func (f Filter) AllowName(collRef collref.Name) bool {
	if f.DBCollFilter == nil {
		return true
	}

	collFilter, ok := f.DBCollFilter[collRef.DBName()]
	if !ok {
		return false
	}

	if collFilter.AllowAll {
		return true
	}

	_, ok = collFilter.CollName[collRef.CollName()]
	return ok
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
