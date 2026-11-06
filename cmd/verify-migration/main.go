package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/internal/migrationcheck"
)

func main() {
	source := flag.String("source", "", "SQLite snapshot to open read-only (schema 5 or later)")
	destination := flag.String("copy", "", "new, non-existing scratch database to create and migrate")
	timeout := flag.Duration("timeout", 30*time.Minute, "total qualification timeout")
	flag.Parse()
	if *source == "" || *destination == "" || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "Specify -source SNAPSHOT.db -copy NEW_SCRATCH.db [-timeout 30m]")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	report, err := migrationcheck.Verify(ctx, *source, *destination)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Verification failed; source was opened read-only:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
