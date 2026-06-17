package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dungkimtran3d/Grafana-Loki/pkg/iter"
	"github.com/dungkimtran3d/Grafana-Loki/pkg/querier"
)

func main() {
	fmt.Println("Starting Grafana Loki Deduplication Demo...")

	q := querier.NewQuerier()
	t1 := time.Unix(1000, 0)

	ingesterStream := iter.Stream{
		Labels: `{app="demo"}`,
		Entries: []iter.Entry{
			{Timestamp: t1, Line: "duplicate log line"},
		},
	}

	storeStream := iter.Stream{
		Labels: `{app="demo"}`,
		Entries: []iter.Entry{
			{Timestamp: t1, Line: "duplicate log line"},
		},
	}

	iter1 := &sliceStreamIterator{streams: []iter.Stream{ingesterStream}}
	iter2 := &sliceStreamIterator{streams: []iter.Stream{storeStream}}

	mergedIter := q.MergeAndDeduplicateStreams(context.Background(), []iter.StreamIterator{iter1, iter2})
	defer mergedIter.Close()

	fmt.Println("Merging and deduplicating streams...")
	count := 0
	for mergedIter.Next() {
		stream := mergedIter.Stream()
		fmt.Printf("Stream Labels: %s\n", stream.Labels)
		for _, entry := range stream.Entries {
			fmt.Printf("  [%s] %s\n", entry.Timestamp.Format(time.RFC3339), entry.Line)
			count++
		}
	}

	fmt.Printf("Total entries returned: %d (Expected: 1)\n", count)
}

type sliceStreamIterator struct {
	streams []iter.Stream
	idx     int
}

func (s *sliceStreamIterator) Next() bool {
	s.idx++
	return s.idx < len(s.streams)
}

func (s *sliceStreamIterator) Stream() iter.Stream {
	return s.streams[s.idx]
}

func (s *sliceStreamIterator) Err() error {
	return nil
}

func (s *sliceStreamIterator) Close() error {
	return nil
}
