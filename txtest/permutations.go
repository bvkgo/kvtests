package txtest

import "sort"

// permutations returns all permutations of slice with integers [0...n).
func permutations(n int) [][]int {
	if n <= 0 {
		return nil
	}
	if n == 1 {
		return [][]int{{0}}
	}
	var result [][]int
	for i := 1; i < n; i++ {
		for _, perm := range permutations(n - 1) {
			for j := 0; j < len(perm)+1; j++ {
				newperm := make([]int, len(perm)+1)
				copy(newperm, perm[0:j])
				newperm[j] = i
				copy(newperm[j+1:], perm[j:])
				result = append(result, newperm)
			}
		}
	}
	return result
}

func sortedPermutations(n int) [][]int {
	perms := permutations(n)
	sort.Slice(perms, func(i, j int) bool {
		a, b := perms[i], perms[j]
		for x := 0; x < len(a); x++ {
			if a[x] == b[x] {
				continue
			}
			return a[x] < b[x]
		}
		return false
	})
	return perms
}

// SerializedPermutations serializes the tx steps and returns all possible
// serialization permutations.
func SerializedPermutations(steps []string) ([][]string, error) {
	ntx, _, err := ParseSteps(steps)
	if err != nil {
		return nil, err
	}

	txMap := make(map[int][]string)
	for i := 0; i < ntx; i++ {
		txMap[i] = FilterSteps(steps, i)
	}

	var serializedPerms [][]string
	for _, perm := range sortedPermutations(ntx) {
		var serialized []string
		for _, tx := range perm {
			serialized = append(serialized, txMap[tx]...)
		}
		serializedPerms = append(serializedPerms, serialized)
	}
	return serializedPerms, nil
}
