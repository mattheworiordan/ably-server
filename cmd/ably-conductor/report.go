package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ably/ably-server/internal/loadgen"
)

func cmdReport(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ably-conductor report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("out", "", "write the report here instead of stdout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(stderr, "usage: ably-conductor report [--out FILE] <summary.json>...")
		return 2
	}
	recs, err := loadgen.LoadRunRecords(fs.Args())
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	md := loadgen.Report(recs)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprint(stdout, md)
	return 0
}
