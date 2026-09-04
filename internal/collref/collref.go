package collref

import (
	"fmt"
	"strings"
)

const DefaultDBName = "default"

// Name is a collection reference: the pair of database name and collection
// name that locates a collection, rendered as "db.coll".
type Name struct {
	dbName   string
	collName string
}

// Parse parses a "db.coll" or "coll" (under the default db) string.
func Parse(s string) (Name, error) {
	if s == "" {
		return Name{}, fmt.Errorf("collection reference is empty")
	}

	if strings.Contains(s, ".") {
		split := strings.Split(s, ".")
		if len(split) != 2 {
			return Name{}, fmt.Errorf("collection reference format is invalid")
		}
		return Name{dbName: split[0], collName: split[1]}, nil
	}

	return Name{dbName: DefaultDBName, collName: s}, nil
}

func New(dbName, collName string) Name {
	if dbName == "" {
		dbName = DefaultDBName
	}
	return Name{dbName: dbName, collName: collName}
}

func (n Name) String() string   { return n.dbName + "." + n.collName }
func (n Name) DBName() string   { return n.dbName }
func (n Name) CollName() string { return n.collName }
