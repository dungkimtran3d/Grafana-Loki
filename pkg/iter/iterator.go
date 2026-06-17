package iter

import (
	"container/heap"
	"time"
)

type Entry struct {
	Timestamp time.Time
	Line      string
}

type EntryIterator interface {
	Next() bool
	Entry() Entry
	Err() error
	Close() error
}

type Stream struct {
	Labels  string
	Entries []Entry
}

type StreamIterator interface {
	Next() bool
	Stream() Stream
	Err() error
	Close() error
}

type Direction int

const (
	Forward Direction = iota
	Backward
)

type heapElement struct {
	iter  EntryIterator
	entry Entry
	index int
}

type iteratorHeap struct {
	elements  []*heapElement
	direction Direction
}

func (h iteratorHeap) Len() int { return len(h.elements) }

func (h iteratorHeap) Less(i, j int) bool {
	t1 := h.elements[i].entry.Timestamp
	t2 := h.elements[j].entry.Timestamp
	if t1.Equal(t2) {
		return h.elements[i].entry.Line < h.elements[j].entry.Line
	}
	if h.direction == Forward {
		return t1.Before(t2)
	}
	return t1.After(t2)
}

func (h iteratorHeap) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
	h.elements[i].index = i
	h.elements[j].index = j
}

func (h *iteratorHeap) Push(x interface{}) {
	n := len(h.elements)
	item := x.(*heapElement)
	item.index = n
	h.elements = append(h.elements, item)
}

func (h *iteratorHeap) Pop() interface{} {
	old := h.elements
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	h.elements = old[0 : n-1]
	return item
}

type HeapIterator struct {
	iters     []EntryIterator
	direction Direction
	h         iteratorHeap
	curr      Entry
	err       error

	hasLast  bool
	lastTime time.Time
	lastLine string
}

func NewHeapIterator(iters []EntryIterator, direction Direction) EntryIterator {
	h := iteratorHeap{
		elements:  make([]*heapElement, 0, len(iters)),
		direction: direction,
	}
	hi := &HeapIterator{
		iters:     iters,
		direction: direction,
		h:         h,
	}
	for _, it := range iters {
		if it.Next() {
			heap.Push(&hi.h, &heapElement{
				iter:  it,
				entry: it.Entry(),
			})
		} else if err := it.Err(); err != nil {
			hi.err = err
		}
	}
	return hi
}

func (hi *HeapIterator) Next() bool {
	for hi.h.Len() > 0 {
		elem := heap.Pop(&hi.h).(*heapElement)
		entry := elem.entry

		isDuplicate := false
		if hi.hasLast {
			if entry.Timestamp.Equal(hi.lastTime) && entry.Line == hi.lastLine {
				isDuplicate = true
			}
		}

		if elem.iter.Next() {
			elem.entry = elem.iter.Entry()
			heap.Push(&hi.h, elem)
		} else if err := elem.iter.Err(); err != nil {
			hi.err = err
		}

		if isDuplicate {
			continue
		}

		hi.curr = entry
		hi.lastTime = entry.Timestamp
		hi.lastLine = entry.Line
		hi.hasLast = true
		return true
	}
	return false
}

func (hi *HeapIterator) Entry() Entry {
	return hi.curr
}

func (hi *HeapIterator) Err() error {
	return hi.err
}

func (hi *HeapIterator) Close() error {
	var firstErr error
	for _, it := range hi.iters {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type sliceIterator struct {
	entries []Entry
	idx     int
}

func NewSliceIterator(entries []Entry) EntryIterator {
	return &sliceIterator{entries: entries, idx: -1}
}

func (s *sliceIterator) Next() bool {
	s.idx++
	return s.idx < len(s.entries)
}

func (s *sliceIterator) Entry() Entry {
	return s.entries[s.idx]
}

func (s *sliceIterator) Err() error {
	return nil
}

func (s *sliceIterator) Close() error {
	return nil
}

type streamIteratorHeapElement struct {
	iter   StreamIterator
	stream Stream
	index  int
}

type streamIteratorHeap struct {
	elements []*streamIteratorHeapElement
}

func (h streamIteratorHeap) Len() int { return len(h.elements) }
func (h streamIteratorHeap) Less(i, j int) bool {
	return h.elements[i].stream.Labels < h.elements[j].stream.Labels
}
func (h streamIteratorHeap) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
	h.elements[i].index = i
	h.elements[j].index = j
}
func (h *streamIteratorHeap) Push(x interface{}) {
	n := len(h.elements)
	item := x.(*streamIteratorHeapElement)
	item.index = n
	h.elements = append(h.elements, item)
}
func (h *streamIteratorHeap) Pop() interface{} {
	old := h.elements
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	h.elements = old[0 : n-1]
	return item
}

type MergeStreamIterator struct {
	iters []StreamIterator
	h     streamIteratorHeap
	curr  Stream
	err   error
}

func NewMergeStreamIterator(iters []StreamIterator) StreamIterator {
	h := streamIteratorHeap{
		elements: make([]*streamIteratorHeapElement, 0, len(iters)),
	}
	msi := &MergeStreamIterator{
		iters: iters,
		h:     h,
	}
	for _, it := range iters {
		if it.Next() {
			heap.Push(&msi.h, &streamIteratorHeapElement{
				iter:   it,
				stream: it.Stream(),
			})
		} else if err := it.Err(); err != nil {
			msi.err = err
		}
	}
	return msi
}

func (msi *MergeStreamIterator) Next() bool {
	if msi.h.Len() == 0 {
		return false
	}

	elem := heap.Pop(&msi.h).(*streamIteratorHeapElement)
	labels := elem.stream.Labels

	var matchingIters []EntryIterator
	matchingIters = append(matchingIters, NewSliceIterator(elem.stream.Entries))

	if elem.iter.Next() {
		elem.stream = elem.iter.Stream()
		heap.Push(&msi.h, elem)
		} else if err := elem.iter.Err(); err != nil {
		msi.err = err
	}

	for msi.h.Len() > 0 && msi.h.elements[0].stream.Labels == labels {
		nextElem := heap.Pop(&msi.h).(*streamIteratorHeapElement)
		matchingIters = append(matchingIters, NewSliceIterator(nextElem.stream.Entries))

		if nextElem.iter.Next() {
			nextElem.stream = nextElem.iter.Stream()
			heap.Push(&msi.h, nextElem)
		} else if err := nextElem.iter.Err(); err != nil {
			msi.err = err
		}
	}

	entryIter := NewHeapIterator(matchingIters, Forward)
	defer entryIter.Close()

	var mergedEntries []Entry
	for entryIter.Next() {
		mergedEntries = append(mergedEntries, entryIter.Entry())
	}
	if err := entryIter.Err(); err != nil {
		msi.err = err
		return false
	}

	msi.curr = Stream{
		Labels:  labels,
		Entries: mergedEntries,
	}
	return true
}

func (msi *MergeStreamIterator) Stream() Stream {
	return msi.curr
}

func (msi *MergeStreamIterator) Err() error {
	return msi.err
}

func (msi *MergeStreamIterator) Close() error {
	var firstErr error
	for _, it := range msi.iters {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
