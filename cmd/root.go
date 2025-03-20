package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zilliztech/milvus-backup/internal/log"
	"github.com/zilliztech/milvus-backup/internal/util/logutil"
)

var (
	config        string
	yamlOverrides []string
	verbosity     int
)

var rootCmd = &cobra.Command{
	Use:   "milvus-backup",
	Short: "Backup and restore tool for Milvus",
	Long: `Milvus Backup is a comprehensive tool for backing up and restoring Milvus collections.

It allows you to create, list, copy, and restore backups across different Milvus instances.
Use aliases to manage multiple Milvus deployments easily.`,
	Run: func(cmd *cobra.Command, args []string) {
		Error(cmd, args, errors.New("unrecognized command"))
	},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setEnvs(yamlOverrides)
		
		// Configure logging based on verbosity level
		// Determine log level based on verbosity
		logLevel := "info" // Default level
		showConsole := false // Default: no console output
		
		if verbosity > 0 {
			showConsole = true // Enable console output if any verbosity level is set
			
			// Set log level based on verbosity
			switch verbosity {
			case 1: // -v
				logLevel = "info"
			case 2: // -vv
				logLevel = "debug"
			case 3: // -vvv or more
				logLevel = "debug" // You can set this to a more verbose level if available
			}
			
			log.Info(fmt.Sprintf("Verbosity level set to %d, log level: %s", verbosity, logLevel))
		}
		
		// Create log configuration
		logCfg := &log.Config{
			Level:   logLevel,
			Console: showConsole,
			File: log.FileLogConfig{
				Filename: "logs/backup.log", // Use default log file
				MaxSize:  300,               // Default size
			},
		}
		
		// Initialize new log configuration
		newLogger, props, err := log.InitLogger(logCfg)
		if err == nil {
			// Replace global log configuration
			log.ReplaceGlobals(newLogger, props)
			
			// Also update logutil configuration
			logutil.SetupLogger(logCfg)
		}
	},
}

func Execute() {
	// Add a custom template for better help formatting
	rootCmd.SetUsageTemplate(usageTemplate)
	
	// Configure flags
	rootCmd.PersistentFlags().StringVarP(&config, "config", "", "backup.yaml", "Config YAML file for Milvus connection")
	rootCmd.PersistentFlags().StringSliceVar(&yamlOverrides, "set", []string{}, "Override YAML values using a capitalized snake case format (--set MILVUS_USER=username)")
	rootCmd.PersistentFlags().CountVarP(&verbosity, "verbose", "v", "Set verbosity level: -v (info), -vv (debug), -vvv (trace)")
	
	// Disable default completion command
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	
	// Add version template
	rootCmd.SetVersionTemplate(fmt.Sprintf("%s\n", colorTitle("Milvus Backup {{.Version}}")))
	
	// Execute the root command
	rootCmd.Execute()
}

func SetVersionInfo(version, commit, date string) {
	rootCmd.Version = fmt.Sprintf("%s (Built on %s from Git SHA %s)", version, date, commit)
	fmt.Println(colorTitle("Milvus Backup " + version))
	log.Info(fmt.Sprintf("Milvus backup version: %s", rootCmd.Version))
}

// Set environment variables from yamlOverrides
func setEnvs(envs []string) {
	for _, e := range envs {
		env := strings.Split(e, "=")
		if len(env) == 2 {
			os.Setenv(env[0], env[1])
		}
	}
}

// Custom usage template for better help formatting
var usageTemplate = `{{colorize "cyan" "Usage:"}}{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

{{colorize "cyan" "Aliases:"}}{{range .Aliases}}
  {{.}}{{end}}{{end}}{{if .HasExample}}

{{colorize "cyan" "Examples:"}}{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

{{colorize "cyan" "Available Commands:"}}{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

{{colorize "cyan" "Flags:"}}{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

{{colorize "cyan" "Global Flags:"}}{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

{{colorize "cyan" "Additional help topics:"}}{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

{{end}}
`

// Initialize custom template functions
func init() {
	cobra.AddTemplateFunc("colorize", func(color string, text string) string {
		switch color {
		case "cyan":
			return colorTitle(text)
		default:
			return text
		}
	})
}
