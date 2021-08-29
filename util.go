package kvtests

import (
	"context"
	"fmt"
)

func FillItems(ctx context.Context, opts *Options) (string, string, error) {
	nkeys := opts.NumItems
	tx, err := opts.NewTx(ctx)
	if err != nil {
		return "", "", err
	}
	for i := 0; i < nkeys; i++ {
		s := opts.getItem(i)
		if err := tx.Set(ctx, s, s); err != nil {
			return "", "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("could not fill the db: %w", err)
	}
	return opts.getItem(0), opts.getItem(nkeys - 1), nil
}
