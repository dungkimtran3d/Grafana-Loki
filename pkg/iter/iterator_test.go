package iter

import (
	"testing"
	"time"
)

func TestHeapIteratorDeduplication(t *testing.T) {
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)
	t3 := time.Unix(3000, 0)

	iter1 := NewSliceIterator([]Entry{
		{Timestamp: t1, Line: "log 1"},
		{Timestamp: t2, Line: "log 2"},
	})
	iter2 := NewSliceIterator([]Entry{
		{Timestamp: t2, Line: "log 2"},
		{Timestamp: t3, Line: "log 3"},
	})

	merged := NewHeapIterator([]EntryIterator{iter1, iter2}, Forward)
	defer merged.Close()

	var results []Entry
	for merged.Next() {
		results = append(results, merged.Entry())
	}

	if err := merged.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []Entry{
		{Timestamp: t1, Line: "log 1"},
		{Timestamp: t2, Line: "log 2"},
		{Timestamp: t3, Line: "log 3"},
	}

	if len(results) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(results))
	}

	for i, entry := range results {
		if !entry.Timestamp.Equal(expected[i].Timestamp) || entry.Line != expected[i].Line {
			t.Errorf("at index %d: expected %+v, got %+v", i, expected[i], entry)
		}
	}
}

func TestHeapIteratorIdenticalTimestampsDifferentContent(t *testing.T) {
	t1 := time.Unix(1000, 0)

	iter1 := NewSliceIterator([]Entry{
		{Timestamp: t1, Line: "log A"},
	})
	iter2 := NewSliceIterator([]Entry{
		{Timestamp: t1, Line: "log B"},
	})

	merged := NewHeapIterator([]EntryIterator{iter1, iter2}, Forward)
	defer merged.Close()

	var results []Entry
	for merged.Next() {
		results = append(results, merged.Entry())
	}

	if err := merged.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(results))
	}

	if results[0].Line != "log A" || results[1].Line != "log B" {
		t.Errorf("unexpected order or content: %+v", results)
	}
}

func TestMergeStreamIterator(t *testing.T) {
	t1 := time.Unix(1000, 0)
	t2 := time.Unix(2000, 0)
	t3 := time.Unix(3000, 0)

	s1 := Stream{
		Labels: `{app="foo"}`,
		Entries: []Entry{
			{Timestamp: t1, Line: "log 1"},
			{Timestamp: t2, Line: "log 2"},
		},
	}
	s2 := Stream{
		Labels: `{app="foo"}`,
		Entries: []Entry{
			{Timestamp: t2, Line: "log 2"},
			{Timestamp: t3, Line: "log 3"},
		},
	}

	iter1 := &sliceStreamIterator{streams: []Stream{s1}}
	iter2 := &sliceStreamIterator{streams: []Stream{s2}}

	merged := NewMergeStreamIterator([]StreamIterator{iter1, iter2})
	defer merged.Close()

	var results []Stream
	for merged.Next() {
		results = append(results, merged.Stream())
	}

	if err := merged.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(results))
	}

	stream := results[0]
	if stream.Labels != `{app="foo"}` {
		t.Errorf("expected labels `{app=\"foo\"}`, got %q", stream.Labels)
	}

	expectedEntries := []Entry{
		{Timestamp: t1, Line: "log 1"},
		{Timestamp: t2, Line: "log 2"},
		{Timestamp: t3, Line: "log 3"},
	}

	if len(stream.Entries) != len(expectedEntries) {
		t.Fatalf("expected %d entries, got %d", len(expectedEntries), len(stream.Entries))
	}

	for i, entry := range stream.Entries {
		if !entry.Timestamp.Equal(expectedEntries[i].Timestamp) || entry.Line != expectedEntries[i].Line {
			t.Errorf("at index %d: expected %+v, got %+v", i, expectedEntries[i], entry)
		}
	}
}

type sliceStreamIterator struct {
	streams []Stream
	idx     int
}

func (s *sliceStreamIterator) Next() bool {
	s.idx++
	return s.idx < len(s.streams)
}

func (s *sliceStreamIterator) Stream() Stream {
	return s.streams[s.idx]
}

func (s *sliceStreamIterator) Err() error {
	return nil
}

func (s *sliceStreamIterator) Close() error {
	return nil
}
