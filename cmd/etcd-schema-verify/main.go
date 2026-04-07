package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	os.Exit(run())
}

func run() int {
	srcEndpoints := flag.String("src-endpoints", "", "source etcd endpoints (comma-separated, required)")
	dstEndpoints := flag.String("dst-endpoints", "", "destination etcd endpoints (comma-separated, required)")
	srcRootPath := flag.String("src-root-path", "by-dev", "source Milvus etcd rootPath")
	dstRootPath := flag.String("dst-root-path", "by-dev", "destination Milvus etcd rootPath")
	collection := flag.String("collection", "", "collection name to verify (required)")
	dbName := flag.String("db", "", "database name (optional, scans all databases if empty)")
	timeout := flag.Duration("timeout", 30*time.Second, "etcd operation timeout")
	flag.Parse()

	if *collection == "" || *srcEndpoints == "" || *dstEndpoints == "" {
		fmt.Fprintf(os.Stderr, "Usage: etcd-schema-verify --src-endpoints=HOST:PORT --dst-endpoints=HOST:PORT --collection=NAME\n\n")
		flag.PrintDefaults()
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	srcCli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*srcEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect source etcd: %v\n", err)
		return 1
	}
	defer srcCli.Close() //nolint:errcheck // best-effort cleanup

	dstCli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(*dstEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect destination etcd: %v\n", err)
		return 1
	}
	defer dstCli.Close() //nolint:errcheck // best-effort cleanup

	srcReader := &reader{cli: srcCli, rootPath: *srcRootPath}
	srcDump, err := srcReader.findCollection(ctx, *dbName, *collection)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: source: %v\n", err)
		return 1
	}

	dstReader := &reader{cli: dstCli, rootPath: *dstRootPath}
	dstDump, err := dstReader.findCollection(ctx, *dbName, *collection)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: target: %v\n", err)
		return 1
	}

	diffs := diffCollections(srcDump, dstDump)

	result := &VerifyResult{
		Collection: *collection,
		Database:   *dbName,
		Aligned:    len(diffs) == 0,
		Source:     srcDump,
		Target:     dstDump,
		Diffs:      diffs,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fmt.Fprintf(os.Stderr, "error: encode JSON: %v\n", err)
		return 1
	}

	if !result.Aligned {
		return 1
	}
	return 0
}
