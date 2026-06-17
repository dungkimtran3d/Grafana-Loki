package querier

import (
	"context"
	"testing"
	"time"

	"github.com/dungkimtran3d/Grafana-Loki/pkg/iter"
)

func TestQuerierMergeAndDeduplicateStreams(t *testing.T) {
	q := NewQuerier()

	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)

	ingesterStream := iter.Stream{
		Labels: `{app="web"}`,
		Entries: []iter.Entry{
			{Timestamp: t1, Line: "request started"},
			{Timestamp: t2, Line: "request completed"},
		},
	}

	storeStream := iter.Stream{
		Labels: `{app="web"}`,
		Entries: []iter.Entry{
			{Timestamp: t1, Line: "request started"},
			{Timestamp: t2, Line: "request completed"},
		},
	}

	iter1 := &sliceStreamIterator{streams: []iter.Stream{ingesterStream}}
	iter2 := &sliceStreamIterator{streams: []iter.Stream{storeStream}}

	mergedIter := q.MergeAndDeduplicateStreams(context.Background(), []iter.StreamIterator{iter1, iter2})
	defer mergedIter.Close()

	var results []iter.Stream
	for mergedIter.Next() {
		results = append(results, mergedIter.Stream())
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(results))
	}

	stream := results[0]
	if len(stream.Entries) != 2 {
		t.Fatalf("expected 2 entries after deduplication, got %d", len(stream.Entries))
	}

	expected := []iter.Entry{
		{Timestamp: t1, Line: "request started"},
		{Timestamp: t2, Line: "request completed"},
	}

	for i, entry := range stream.Entries {
		if !entry.Timestamp.Equal(expected[i].Timestamp) || entry.Line != expected[i].Line {
			t.Errorf("at index %d: expected %+v, got %+v", i, expected[i], entry)
		}
	}
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
