package kvtests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"

	"github.com/bvkgo/kv"
)

type Callback struct {
	count int

	first, last string
}

func (c *Callback) HandleAscend(ctx context.Context, it kv.Iterator) error {
	var err error
	for k, _, err := it.GetNext(ctx); err == nil; k, _, err = it.GetNext(ctx) {
		if len(k) == 0 {
			return fmt.Errorf("key cannot be empty")
		}
		if k < c.last {
			return fmt.Errorf("last key %q was larger than current %q", c.last, k)
		}
		if k == c.last {
			return fmt.Errorf("last key %q was same as the current %q", c.last, k)
		}
		if len(c.first) == 0 {
			c.first = k
		}
		c.count++
		c.last = k
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (c *Callback) HandleDescend(ctx context.Context, it kv.Iterator) error {
	var err error
	for k, _, err := it.GetNext(ctx); err == nil; k, _, err = it.GetNext(ctx) {
		if len(k) == 0 {
			return fmt.Errorf("key cannot be empty")
		}
		if len(c.last) > 0 {
			if k > c.last {
				return fmt.Errorf("last key %q was smaller than current %q", c.last, k)
			}
			if k == c.last {
				return fmt.Errorf("last key %q was same as the current %q", c.last, k)
			}
		}
		if len(c.first) == 0 {
			c.first = k
		}
		c.count++
		c.last = k
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func RunAscendTest1(ctx context.Context, backend Backend) error {
	nkeys := 1000
	tx1, err := backend.NewTx(ctx)
	if err != nil {
		return err
	}

	for i := 0; i <= nkeys; i++ {
		s := fmt.Sprintf("%03d", i)
		if err := tx1.Set(ctx, s, s); err != nil {
			return err
		}
		nkeys++
	}
	if err := tx1.Commit(ctx); err != nil {
		return err
	}
	largest := fmt.Sprintf("%03d", nkeys-1)

	// Iterate all keys in ascending order.
	{
		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, "", "", it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.count != nkeys {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys, cb.count)
		}
	}

	// Iterate till the largest key with one of i or j as the empty string.
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, x, "", it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, cb.first)
		}
		if cb.last != largest {
			return fmt.Errorf("wanted %s as the last, got %s", largest, cb.last)
		}
		if cb.count != nkeys-r {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys-r, cb.count)
		}
	}
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, "", x, it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, cb.first)
		}
		if cb.last != largest {
			return fmt.Errorf("wanted %s as the last, got %s", largest, cb.last)
		}
		if cb.count != nkeys-r {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys-r, cb.count)
		}
	}

	// Iterate randomly picked range.
	{
		b := rand.Intn(nkeys)
		e := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", b)
		y := fmt.Sprintf("%03d", e)
		min, max, count := x, fmt.Sprintf("%03d", e-1), e-b
		if y < x {
			min, max, count = y, fmt.Sprintf("%03d", b-1), b-e
		}

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Ascend(ctx, x, y, it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleAscend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.count != count {
			return fmt.Errorf("wanted %d callbacks, got %d", count, cb.count)
		}
		if count > 0 {
			if cb.first != min {
				return fmt.Errorf("wanted %s as the first, got %s", min, cb.first)
			}
			if cb.last != max {
				return fmt.Errorf("wanted %s as the last, got %s", max, cb.last)
			}
		}
	}

	return nil
}

func RunDescendTest1(ctx context.Context, backend Backend) error {
	nkeys := 1000
	tx1, err := backend.NewTx(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < nkeys; i++ {
		s := fmt.Sprintf("%03d", i)
		if err := tx1.Set(ctx, s, s); err != nil {
			return err
		}
	}
	if err := tx1.Commit(ctx); err != nil {
		return err
	}
	smallest := fmt.Sprintf("%03d", 0)
	largest := fmt.Sprintf("%03d", nkeys-1)

	// Iterate all keys in ascending order.
	{

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, "", "", it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.count != nkeys {
			return fmt.Errorf("wanted %d callbacks, got %d", nkeys, cb.count)
		}
		if cb.first != largest {
			return fmt.Errorf("wanted %q as the first, got %q", largest, cb.first)
		}
		if cb.last != smallest {
			return fmt.Errorf("wanted %q as the last, got %q", smallest, cb.last)
		}
	}

	// Iterate till the smallest key with one of i or j as the empty string.
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, x, "", it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, cb.first)
		}
		if cb.last != smallest {
			return fmt.Errorf("wanted %s as the last, got %s", smallest, cb.last)
		}
		if cb.count != r+1 {
			return fmt.Errorf("wanted %d callbacks, got %d", r+1, cb.count)
		}
	}
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, "", x, it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.first != x {
			return fmt.Errorf("wanted %s as the first, got %s", x, cb.first)
		}
		if cb.last != smallest {
			return fmt.Errorf("wanted %s as the last, got %s", smallest, cb.last)
		}
		if cb.count != r+1 {
			return fmt.Errorf("wanted %d callbacks, got %d", r+1, cb.count)
		}
	}

	// Iterate randomly picked range.
	{
		f := rand.Intn(nkeys)
		l := rand.Intn(nkeys)
		if f < l {
			f, l = l, f
		}
		x := fmt.Sprintf("%03d", f)
		y := fmt.Sprintf("%03d", l)
		min, max, count := fmt.Sprintf("%03d", l+1), x, f-l

		tx, err := backend.NewTx(ctx)
		if err != nil {
			return err
		}
		it, err := backend.NewIt(ctx)
		if err != nil {
			return err
		}
		if err := tx.Descend(ctx, x, y, it); err != nil {
			return err
		}
		var cb Callback
		if err := cb.HandleDescend(ctx, it); err != nil {
			return err
		}
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		if cb.count != count {
			return fmt.Errorf("wanted %d callbacks, got %d", count, cb.count)
		}
		if count > 0 {
			if cb.first != max {
				return fmt.Errorf("wanted %s as the first, got %s", max, cb.first)
			}
			if cb.last != min {
				return fmt.Errorf("wanted %s as the last, got %s", min, cb.last)
			}
		}
	}

	return nil
}
