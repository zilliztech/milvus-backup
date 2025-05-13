package version

import (
	"fmt"
	"runtime"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	Date      = "unknown"
	GoVersion = runtime.Version()
)

func String() string {
	return fmt.Sprintf("%s (Built on %s from Git SHA %s by %s)", Version, Date, Commit, GoVersion)
}

func Print() {
	fmt.Println(String())
}
