package querier

import (
	"context"
	"github.com/dungkimtran3d/Grafana-Loki/pkg/iter"
)

type Querier struct{}

func NewQuerier() *Querier {
	return &Querier{}
}

func (q *Querier) MergeAndDeduplicateStreams(ctx context.Context, iters []iter.StreamIterator) iter.StreamIterator {
	return iter.NewMergeStreamIterator(iters)
}
