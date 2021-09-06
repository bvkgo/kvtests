package kvtests

import (
	"context"
	"fmt"
)

func FillItems(ctx context.Context, opts *Options) (string, string, error) {
	nkeys := opts.NumKeys
	tx, err := opts.NewTx(ctx)
	if err != nil {
		return "", "", err
	}
	for i := 0; i < nkeys; i++ {
		s := opts.getKey(i)
		if err := tx.Set(ctx, s, s); err != nil {
			return "", "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("could not fill the db: %w", err)
	}
	return opts.getKey(0), opts.getKey(nkeys - 1), nil
}
