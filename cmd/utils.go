package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/zilliztech/milvus-backup/core/proto/backuppb"
)

// Colors and symbols for CLI output
var (
	// Color functions
	colorSuccess = color.New(color.FgGreen).SprintFunc()
	colorError   = color.New(color.FgRed).SprintFunc()
	colorWarning = color.New(color.FgYellow).SprintFunc()
	colorInfo    = color.New(color.FgBlue).SprintFunc()
	colorHeader  = color.New(color.FgHiWhite, color.Bold).SprintFunc()
	colorTitle   = color.New(color.FgHiCyan, color.Bold).SprintFunc()
	colorLabel   = color.New(color.FgHiWhite).SprintFunc()
	colorValue   = color.New(color.FgWhite).SprintFunc()

	// Symbols
	symbolSuccess = "✓"
	symbolError   = "✗"
	symbolWarning = "!"
	symbolInfo    = "ℹ"
	symbolBullet  = "•"
	symbolArrow   = "→"
)

// PrintError prints an error message and optionally exits the program
func PrintError(msg string, err error, exitCode int) {
	fmt.Printf("%s %s", colorError(symbolError), colorError("ERROR:"))
	if err != nil {
		fmt.Printf(" %s\n", msg)
		fmt.Printf("       %s %v\n", colorError(symbolArrow), err)
	} else {
		fmt.Printf(" %s\n", msg)
	}
	
	if exitCode > 0 {
		os.Exit(exitCode)
	}
}

// PrintSuccess prints a success message
func PrintSuccess(msg string) {
	fmt.Printf("%s %s %s\n", colorSuccess(symbolSuccess), colorSuccess("SUCCESS:"), msg)
}

// PrintWarning prints a warning message
func PrintWarning(msg string) {
	fmt.Printf("%s %s %s\n", colorWarning(symbolWarning), colorWarning("WARNING:"), msg)
}

// PrintInfo prints an informational message
func PrintInfo(msg string) {
	fmt.Printf("%s %s %s\n", colorInfo(symbolInfo), colorInfo("INFO:"), msg)
}

// FormatTime formats a timestamp as a readable string
func FormatTime(timestamp int64) string {
	// Convert from milliseconds to seconds if needed
	if timestamp > 1000000000000 {
		timestamp = timestamp / 1000
	}
	return time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")
}

// FormatSize formats a size in bytes as a human-readable string
func FormatSize(sizeInBytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	
	if sizeInBytes < KB {
		return fmt.Sprintf("%d B", sizeInBytes)
	} else if sizeInBytes < MB {
		return fmt.Sprintf("%.2f KB", float64(sizeInBytes)/float64(KB))
	} else if sizeInBytes < GB {
		return fmt.Sprintf("%.2f MB", float64(sizeInBytes)/float64(MB))
	} else {
		return fmt.Sprintf("%.2f GB", float64(sizeInBytes)/float64(GB))
	}
}

// FormatDuration formats a duration in milliseconds as a readable string
func FormatDuration(durationMs int64) string {
	durationSec := durationMs / 1000
	
	if durationSec < 60 {
		return fmt.Sprintf("%d seconds", durationSec)
	} else if durationSec < 3600 {
		minutes := durationSec / 60
		seconds := durationSec % 60
		return fmt.Sprintf("%d minutes %d seconds", minutes, seconds)
	} else {
		hours := durationSec / 3600
		minutes := (durationSec % 3600) / 60
		seconds := durationSec % 60
		return fmt.Sprintf("%d hours %d minutes %d seconds", hours, minutes, seconds)
	}
}

// FormatBackupStatus formats a backup status as colored text
func FormatBackupStatus(stateCode backuppb.BackupTaskStateCode) string {
	var statusText string
	var statusColor func(a ...interface{}) string
	var statusSymbol string
	
	switch stateCode {
	case backuppb.BackupTaskStateCode_BACKUP_SUCCESS:
		statusText = "Completed"
		statusColor = colorSuccess
		statusSymbol = symbolSuccess
	case backuppb.BackupTaskStateCode_BACKUP_FAIL:
		statusText = "Failed"
		statusColor = colorError
		statusSymbol = symbolError
	case backuppb.BackupTaskStateCode_BACKUP_EXECUTING:
		statusText = "In Progress"
		statusColor = colorWarning
		statusSymbol = "⟳"
	default:
		statusText = stateCode.String()
		statusColor = colorValue
		statusSymbol = symbolBullet
	}
	
	return statusColor(statusSymbol + " " + statusText)
}

// PrintSectionTitle prints a section title with a separator
func PrintSectionTitle(title string) {
	fmt.Println()
	fmt.Printf("%s\n", colorTitle(strings.ToUpper(title)))
	fmt.Printf("%s\n", colorTitle(strings.Repeat("━", len(title))))
}

// PrintSubsectionTitle prints a subsection title
func PrintSubsectionTitle(title string) {
	fmt.Println()
	fmt.Printf("%s %s\n", colorHeader(symbolBullet), colorHeader(title))
}

// PrintKeyValue prints a key-value pair with consistent formatting
func PrintKeyValue(key string, value interface{}) {
	fmt.Printf("  %s: %v\n", colorLabel(key), colorValue(value))
}

// CreateTable creates and returns a formatted table
func CreateTable(headers []string) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(headers)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("  ")
	table.SetNoWhiteSpace(true)
	
	// Style the header
	headerColors := make([]tablewriter.Colors, len(headers))
	for i := range headerColors {
		headerColors[i] = tablewriter.Colors{tablewriter.Bold, tablewriter.FgHiWhiteColor}
	}
	table.SetHeaderColor(headerColors...)
	
	return table
}

// PrintProgressBar prints a progress bar
func PrintProgressBar(current, total int64, prefix string) {
	width := 40
	percent := float64(current) / float64(total)
	completed := int(percent * float64(width))
	
	bar := strings.Repeat("█", completed) + strings.Repeat("░", width-completed)
	percentStr := fmt.Sprintf("%.1f%%", percent*100)
	
	fmt.Printf("\r%s [%s] %s (%d/%d)", prefix, bar, percentStr, current, total)
	
	if current >= total {
		fmt.Println()
	}
}
