package kvtests

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/bvkgo/kvtests/txtest"
)

func RunAllIsolationTests(t *testing.T, ctx context.Context, opts *Options) {
	if err := SerializedTxes(ctx, opts); err != nil {
		t.Errorf("SerializedTxes: %v", err)
	}

	if err := NonConflictingTxes(ctx, opts); err != nil {
		t.Errorf("NonConflictingTxes: %v", err)
	}

	if err := ConflictingReadOnlyTxes(ctx, opts); err != nil {
		t.Errorf("ConflictingReadOnlyTxes: %v", err)
	}

	if err := ConflictingReadWriteTxes(ctx, opts); err != nil {
		t.Errorf("ConflictingReadWriteTxes: %v", err)
	}

	if err := ConflictingDeletes(ctx, opts); err != nil {
		t.Errorf("ConflictingDeletes: %v", err)
	}

	if err := NonConflictingDeletes(ctx, opts); err != nil {
		t.Errorf("NonConflictingDeletes: %v", err)
	}

	if err := AbortedReads(ctx, opts); err != nil {
		t.Errorf("AbortedReads: %v", err)
	}

	if err := RepeatedReads(ctx, opts); err != nil {
		t.Errorf("RepeatedReads: %v", err)
	}
}

func runIsolationTest(ctx context.Context, opts *Options, steps []string) (*txtest.IsolationTest, error) {
	opts.setDefaults()
	if err := opts.Check(); err != nil {
		return nil, err
	}
	if _, _, err := FillItems(ctx, opts); err != nil {
		return nil, err
	}
	it, err := txtest.NewIsolationTest(steps)
	if err != nil {
		return nil, err
	}
	keys, err := opts.selectKeys(it.NumKey())
	if err != nil {
		return nil, err
	}
	if _, err := it.Run(ctx, opts.NewTx, keys); err != nil {
		return nil, fmt.Errorf("run tx steps failed: %w", err)
	}
	return it, nil
}

func SerializedTxes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t0: get-k0",
		"t0: set-k0-t0",
		"t0: commit",

		"t1: begin",
		"t1: get-k0",
		"t1: set-k0-t1",
		"t1: commit",

		"t2: begin",
		"t2: get-k0",
		"t2: set-k0-t2",
		"t2: commit",

		"t3: begin",
		"t3: delete-k0",
		"t3: commit",

		"t4: begin",
		"t4: get-k0",
		"t4: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n != 5 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("all txes are expected to commit")
	}
	if v := it.GetResultAtLine(5); v != "t0" {
		return fmt.Errorf("unexpected get result %q at line 5", v)
	}
	if v := it.GetResultAtLine(9); v != "t1" {
		return fmt.Errorf("unexpected get result %q at line 9", v)
	}
	if v := it.GetResultAtLine(16); v != "os.ErrNotExist" {
		return fmt.Errorf("unexpected get result %q at line 16", v)
	}
	return nil
}

func NonConflictingTxes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",
		"t2: begin",

		"t0: get-k0",
		"t0: set-k0-t0",
		"t1: get-k1",
		"t1: set-k1-t1",

		"t0: commit",

		"t2: get-k2",
		"t2: set-k2-t2",

		"t1: commit",
		"t2: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n != 3 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("all txes are expected to commit")
	}
	return nil
}

func ConflictingReadOnlyTxes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",
		"t2: begin",
		"t0: get-k0",
		"t1: get-k0",
		"t0: commit",
		"t2: get-k0",
		"t1: commit",
		"t2: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n != 3 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("all txes are expected to commit")
	}
	return nil
}

func ConflictingReadWriteTxes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",

		"t0: set-k0-t0",
		"t1: get-k0",

		"t0: commit",
		"t1: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n < 1 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("at least one tx is expected to commit")
	}
	return nil
}

func ConflictingDeletes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",

		"t0: delete-k0",
		"t1: delete-k0",

		"t0: commit",
		"t1: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n < 1 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("at least one tx is expected to commit")
	}
	return nil
}

func NonConflictingDeletes(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",

		"t0: delete-k0",
		"t1: delete-k1",

		"t0: commit",
		"t1: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n != 2 {
		for i, result := range it.Results() {
			log.Printf("tx%d -> %v", i, result)
		}
		return fmt.Errorf("all txes are expected to commit")
	}
	return nil
}

func AbortedReads(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",
		"t0: set-k0-A",
		"t1: get-k0",
		"t0: abort",
		"t1: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n < 1 {
		return fmt.Errorf("one tx is expected to commit")
	}
	return nil
}

func RepeatedReads(ctx context.Context, opts *Options) error {
	steps := []string{
		"t0: begin",
		"t1: begin",
		"t1: get-k0",
		"t0: set-k0-A",
		"t0: commit",
		"t1: get-k0",
		"t1: commit",
	}
	it, err := runIsolationTest(ctx, opts, steps)
	if err != nil {
		return err
	}
	if n := it.NumSuccess(); n < 1 {
		return fmt.Errorf("at least one tx is expected to commit")
	}
	t1get0 := it.GetResultAtLine(2)
	t1get1 := it.GetResultAtLine(5)
	if t1get0 != t1get1 {
		return fmt.Errorf("noticed read-committed")
	}
	return nil
}
