package txtest

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	BeginRe  = regexp.MustCompile(`^t(\d+): begin$`)
	AbortRe  = regexp.MustCompile(`^t(\d+): abort$`)
	CommitRe = regexp.MustCompile(`^t(\d+): commit$`)

	GetRe    = regexp.MustCompile(`^t(\d+): get-k(\d+)$`)
	SetRe    = regexp.MustCompile(`^t(\d+): set-k(\d+)-(\w+)$`)
	DeleteRe = regexp.MustCompile(`^t(\d+): delete-k(\d+)$`)

	// TODO: Add support for ascend/descend/scan operations.
)

// ParseStep parses a line in the Test and returns the tx-id, key-id and
// value.
func parseStep(step string) (*regexp.Regexp, int, int, string, error) {
	if BeginRe.MatchString(step) {
		ms := BeginRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return BeginRe, tx, -1, "", nil
	}
	if AbortRe.MatchString(step) {
		ms := AbortRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return AbortRe, tx, -1, "", nil
	}
	if CommitRe.MatchString(step) {
		ms := CommitRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return CommitRe, tx, -1, "", nil
	}
	if GetRe.MatchString(step) {
		ms := GetRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		key, err := strconv.Atoi(ms[2])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return GetRe, tx, key, "", nil
	}
	if SetRe.MatchString(step) {
		ms := SetRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		key, err := strconv.Atoi(ms[2])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return SetRe, tx, key, ms[3], nil
	}
	if DeleteRe.MatchString(step) {
		ms := DeleteRe.FindStringSubmatch(step)
		tx, err := strconv.Atoi(ms[1])
		if err != nil {
			return nil, -1, -1, "", err
		}
		key, err := strconv.Atoi(ms[2])
		if err != nil {
			return nil, -1, -1, "", err
		}
		return DeleteRe, tx, key, "", nil
	}
	return nil, -1, -1, "", os.ErrInvalid
}

// ParseSteps validates the steps and returns number of total txes and keys
// used by the steps.
func ParseSteps(steps []string) (int, int, error) {
	txids := make(map[int]int)
	keyids := make(map[int]int)

	begins := make(map[int][]int)
	aborts := make(map[int][]int)
	commits := make(map[int][]int)

	for line, step := range steps {
		re, tx, key, _, err := parseStep(step)
		if err != nil {
			return -1, -1, fmt.Errorf("could not parse %q on line %d: %w", step, line, err)
		}

		txids[tx]++
		if key >= 0 {
			keyids[key]++
		}

		switch re {
		case BeginRe:
			begins[tx] = append(begins[tx], line)
		case CommitRe:
			commits[tx] = append(commits[tx], line)
		case AbortRe:
			aborts[tx] = append(aborts[tx], line)
		}
	}

	// There should not be gaps in the txids or keyids. For example, we can't
	// have T1 and T5 without also T2, T3 and T4. Same applies to the key ids.
	for i := 0; i < len(txids); i++ {
		if _, ok := txids[i]; !ok {
			return -1, -1, fmt.Errorf("tx ids must be contiguous (tx%d is missing): %w", i, os.ErrInvalid)
		}
	}
	for i := 0; i < len(keyids); i++ {
		if _, ok := keyids[i]; !ok {
			return -1, -1, fmt.Errorf("key ids must be contiguous (key%d is missing): %w", i, os.ErrInvalid)
		}
	}

	// All txes must have only one instance of BEGIN and only one instance
	// of COMMIT or ABORT.
	for tx := range txids {
		if len(begins[tx]) != 1 {
			return -1, -1, fmt.Errorf("tx%d must've exactly one BEGIN: %w", tx, os.ErrInvalid)
		}
		if len(commits[tx])+len(aborts[tx]) != 1 {
			return -1, -1, fmt.Errorf("tx%d must've exactly one of COMMIT|ABORT: %w", tx, os.ErrInvalid)
		}
		if lines, ok := commits[tx]; ok {
			if begins[tx][0] > lines[0] {
				return -1, -1, fmt.Errorf("tx%d must've BEGIN before COMMIT: %w", tx, os.ErrInvalid)
			}
		}
		if lines, ok := aborts[tx]; ok {
			if begins[tx][0] > lines[0] {
				return -1, -1, fmt.Errorf("tx%d must've BEGIN before ABORT: %w", tx, os.ErrInvalid)
			}
		}
	}

	return len(txids), len(keyids), nil
}

// FilterSteps returns steps that correspond to the given txid.
func FilterSteps(steps []string, txid int) []string {
	prefix := fmt.Sprintf("tx%d", txid)
	var txsteps []string
	for _, step := range steps {
		if strings.HasPrefix(step, prefix) {
			txsteps = append(txsteps, step)
		}
	}
	return txsteps
}
