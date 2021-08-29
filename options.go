package kvtests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/bvkgo/kv"
)

type Options struct {
	NewTx func(context.Context) (kv.Transaction, error)
	NewIt func(context.Context) (kv.Iterator, error)

	Seed int64

	NumItems int

	rand *rand.Rand
}

func (opts *Options) setDefaults() {
	if opts.NumItems == 0 {
		opts.NumItems = 1000
	}
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	opts.rand = rand.New(rand.NewSource(opts.Seed))
}

func (opts *Options) Check() error {
	if opts.NewTx == nil || opts.NewIt == nil {
		return fmt.Errorf("NewTx and NewIt fields are required: %w", os.ErrInvalid)
	}
	return nil
}
