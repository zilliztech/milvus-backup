package restore

import (
	"fmt"
	"strings"

	"github.com/zilliztech/milvus-backup/core/namespace"
)

type nsRenamer interface {
	// generate generates a new dbName and collName for the given dbName and collName.
	rename(ns namespace.NS) namespace.NS
}

var _ nsRenamer = (*defaultNSRenamer)(nil)

type defaultNSRenamer struct{}

func newDefaultNSRenamer() *defaultNSRenamer                  { return &defaultNSRenamer{} }
func (*defaultNSRenamer) rename(ns namespace.NS) namespace.NS { return ns }

var _ nsRenamer = (*suffixReNamer)(nil)

type suffixReNamer struct {
	suffix string
}

func newSuffixNameRenamer(suffix string) *suffixReNamer {
	return &suffixReNamer{suffix: suffix}
}

func (s *suffixReNamer) rename(ns namespace.NS) namespace.NS {
	return namespace.New(ns.DBName(), ns.CollName()+s.suffix)
}

var _ nsRenamer = (*mapRenamer)(nil)

type mapRenamer struct {
	dbWildcard  map[string]string       // dbName -> newDbName, from db1.*:db2.*
	nsRenameMap map[string]namespace.NS // dbName.collName -> newCollName, from db1.coll1:db2.coll2 and coll1:coll2
}

// newRenameGenerator creates a new mapRenamer with the given rename map.
// rename map format: key: oldName, value: newName
// 1. key: db1.*: newName: db2.*
// 2. key: db1.coll1: newName: db2.coll2
// 3. key: coll1: newName: coll2
func newMapRenamer(nsMap map[string]string) (*mapRenamer, error) {
	// add default db in collection_renames if not set
	dbWildcard := make(map[string]string)
	nsRenameMap := make(map[string]namespace.NS, len(nsMap))

	for oldNSStr, newNSStr := range nsMap {
		// 1. db1.*:db2.*
		if strings.HasSuffix(oldNSStr, ".*") && strings.HasSuffix(newNSStr, ".*") {
			oldDBName := strings.TrimSuffix(oldNSStr, ".*")
			newDBName := strings.TrimSuffix(newNSStr, ".*")
			dbWildcard[oldDBName] = newDBName
			continue
		}

		oldNS, err := namespace.Parse(oldNSStr)
		if err != nil {
			return nil, fmt.Errorf("restore: parse old namespace %s: %w", oldNSStr, err)
		}

		newNS, err := namespace.Parse(newNSStr)
		if err != nil {
			return nil, fmt.Errorf("restore: parse new namespace %s: %w", newNSStr, err)
		}

		nsRenameMap[oldNS.String()] = newNS
	}

	return &mapRenamer{dbWildcard: dbWildcard, nsRenameMap: nsRenameMap}, nil
}

func (r *mapRenamer) rename(ns namespace.NS) namespace.NS {
	nsString := ns.String()
	if newNS, ok := r.nsRenameMap[nsString]; ok {
		return newNS
	}

	if newDBName, ok := r.dbWildcard[ns.DBName()]; ok {
		return namespace.New(newDBName, ns.CollName())
	}

	return ns
}
