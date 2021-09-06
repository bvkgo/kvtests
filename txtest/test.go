package txtest

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/bvkgo/kv"
)

type IsolationTest struct {
	steps []string

	// ntx, nkey keep track of number of txes and keys in the test.
	ntx  int
	nkey int

	// keeps holds keys used in the last recent run.
	keys []string

	// values holds the final value for each key.
	values []string

	// results holds commit or abort statuses for each tx.
	results []error

	// gets holds the history of results for the get operation as a mapping from
	// step index to the result.
	gets map[int]string
}

func NewIsolationTest(steps []string) (*IsolationTest, error) {
	ntx, nkey, err := ParseSteps(steps)
	if err != nil {
		return nil, err
	}

	it := &IsolationTest{
		ntx:     ntx,
		nkey:    nkey,
		steps:   steps,
		gets:    make(map[int]string),
		values:  make([]string, nkey),
		results: make([]error, ntx),
	}
	return it, nil
}

func (it *IsolationTest) GetResultAtLine(index int) string {
	if v, ok := it.gets[index]; ok {
		return v
	}
	return ""
}

func (it *IsolationTest) NumTx() int {
	return it.ntx
}

func (it *IsolationTest) NumKey() int {
	return it.nkey
}

func (it *IsolationTest) NumSuccess() int {
	cnt := 0
	for _, err := range it.results {
		if err == nil {
			cnt++
		}
	}
	return cnt
}

func (it *IsolationTest) Keys() []string {
	return append([]string{}, it.keys...)
}

func (it *IsolationTest) Values() []string {
	return append([]string{}, it.values...)
}

func (it *IsolationTest) Results() []error {
	return append([]error{}, it.results...)
}

func (it *IsolationTest) Run(ctx context.Context, newTx kv.NewTxFunc, keys []string) ([]string, error) {
	if err := it.runSteps(ctx, newTx, keys); err != nil {
		return nil, err
	}
	result, err := getKeys(ctx, newTx, keys)
	if err != nil {
		return nil, err
	}
	it.keys = append([]string{}, keys...)
	it.values = append([]string{}, result...)
	return result, nil
}

func (it *IsolationTest) runSteps(ctx context.Context, newTx kv.NewTxFunc, keys []string) (status error) {
	if err := clearKeys(ctx, newTx, keys); err != nil {
		return fmt.Errorf("could not clear keys %v: %w", keys, err)
	}

	txes := make([]kv.Transaction, it.ntx)
	defer func() {
		if status != nil {
			for _, tx := range txes {
				if tx != nil {
					_ = tx.Rollback(ctx)
				}
			}
		}
	}()

	for line, step := range it.steps {
		re, i, j, newvalue, err := parseStep(step)
		if err != nil {
			return os.ErrInvalid
		}

		// Take the appropriate action.

		if re == BeginRe {
			tx, err := newTx(ctx)
			if err != nil {
				return fmt.Errorf("could not create tx %d: %w", i, err)
			}
			txes[i] = tx
			continue
		}

		if re == GetRe {
			v, err := txes[i].Get(ctx, keys[j])
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("get in line %d failed: %w", line, err)
				}
				v = "os.ErrNotExist"
			}
			// Add it to the get history.
			it.gets[line] = v
			continue
		}

		if re == SetRe {
			if err := txes[i].Set(ctx, keys[j], newvalue); err != nil {
				return fmt.Errorf("set in line %d failed: %w", line, err)
			}
			continue
		}

		if re == DeleteRe {
			if err := txes[i].Delete(ctx, keys[j]); err != nil {
				return fmt.Errorf("delete in line %d failed: %w", line, err)
			}
			continue
		}

		if re == AbortRe {
			if err := txes[i].Rollback(ctx); err != nil {
				it.results[i] = err
			}
			txes[i] = nil
			continue
		}

		if re == CommitRe {
			if err := txes[i].Commit(ctx); err != nil {
				it.results[i] = err
			}
			txes[i] = nil
			continue
		}
	}

	// At least one tx must succeed.
	if it.NumSuccess() == 0 {
		return fmt.Errorf("all txes failed to commit")
	}

	return nil
}

func clearKeys(ctx context.Context, newTx kv.NewTxFunc, keys []string) (status error) {
	tx, err := newTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if status != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	for _, k := range keys {
		if err := tx.Set(ctx, k, k); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func getKeys(ctx context.Context, newTx kv.NewTxFunc, keys []string) (vs []string, status error) {
	tx, err := newTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if status != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	for _, k := range keys {
		v, err := tx.Get(ctx, k)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			v = "os.ErrNotExist"
		}
		vs = append(vs, v)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return vs, nil
}
