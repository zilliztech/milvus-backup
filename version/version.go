package version

import (
	"fmt"
	"os"
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

// Print writes the version banner to stderr. It is a diagnostic, so it stays
// off stdout where a command may write a machine-consumable document.
func Print() {
	fmt.Fprintln(os.Stderr, String())
}
