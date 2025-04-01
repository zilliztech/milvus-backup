package namespace

import (
	"fmt"
	"strings"
)

const DefaultDBName = "default"

type NS struct {
	dbName   string
	collName string
}

func Parse(ns string) (NS, error) {
	if ns == "" {
		return NS{}, fmt.Errorf("namespace is empty")
	}

	if strings.Contains(ns, ".") {
		split := strings.Split(ns, ".")
		if len(split) != 2 {
			return NS{}, fmt.Errorf("namespace format is invalid")
		}
		return NS{dbName: split[0], collName: split[1]}, nil
	}

	return NS{dbName: DefaultDBName, collName: ns}, nil
}

func New(dbName, collName string) NS {
	if dbName == "" {
		dbName = DefaultDBName
	}
	return NS{dbName: dbName, collName: collName}
}

func (n NS) String() string {
	return n.dbName + "." + n.collName
}

func (n NS) DBName() string   { return n.dbName }
func (n NS) CollName() string { return n.collName }
