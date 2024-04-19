package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/dottedmag/parallel"
)

const maxDigesters = 20

type fileChecksum struct {
	Path     string
	Checksum string
}

func md5SumFile(filename string) (hexSum string, err error) {
	fh, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer fh.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, fh); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func fetchChecksums(ctx context.Context, root string, resultsCh chan<- fileChecksum) error {
	defer close(resultsCh)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		jobsCh := make(chan string)

		for i := 0; i < maxDigesters; i++ {
			spawn(fmt.Sprintf("digester-%d", i), parallel.Continue, func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case path, more := <-jobsCh:
						if !more {
							return nil
						}
						sum, err := md5SumFile(path)
						if err != nil {
							return err
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case resultsCh <- fileChecksum{Path: path, Checksum: sum}:
						}
					}
				}
			})
		}
		spawn("walker", parallel.Continue, func(ctx context.Context) error {
			defer close(jobsCh)

			return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case jobsCh <- path:
				}
				return nil
			})
		})
		return nil
	})
}

func checksumTree(ctx context.Context, root string) ([]fileChecksum, error) {
	var fileChecksums []fileChecksum

	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		resultsCh := make(chan fileChecksum)

		spawn("collector", parallel.Continue, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case cs, more := <-resultsCh:
					if !more {
						return nil
					}
					fileChecksums = append(fileChecksums, cs)
				}
			}
		})
		spawn("fetcher", parallel.Continue, func(ctx context.Context) error {
			return fetchChecksums(ctx, root, resultsCh)
		})
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("Error checksumming tree: %w", err)
	}

	sort.Slice(fileChecksums, func(i, j int) bool {
		return fileChecksums[i].Path < fileChecksums[j].Path
	})
	return fileChecksums, nil
}

func main() {
	// TODO: handle SIGINT and cancel the passed context
	fileChecksums, err := checksumTree(context.Background(), os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	for _, fc := range fileChecksums {
		fmt.Printf("%s %s\n", fc.Checksum, fc.Path)
	}
}
