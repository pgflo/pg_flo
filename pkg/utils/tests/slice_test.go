package utils_tests

import (
	"testing"

	"github.com/pgflo/pg_flo/pkg/utils"
)

func TestSlicesEqual(t *testing.T) {
	type args[T comparable] struct {
		slice1 []T
		slice2 []T
	}
	type testCase[T comparable] struct {
		name string
		args args[T]
		want bool
	}
	tests := []testCase[string]{
		{
			name: "Equal slices with same order",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3"},
				slice2: []string{"table1", "table2", "table3"},
			},
			want: true,
		},
		{
			name: "Equal slices with different order",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3"},
				slice2: []string{"table3", "table1", "table2"},
			},
			want: true,
		},
		{
			name: "Empty slices",
			args: args[string]{
				slice1: []string{},
				slice2: []string{},
			},
			want: true,
		},
		{
			name: "Mismatched slices (different elements)",
			args: args[string]{
				slice1: []string{"table1", "table2", "table4"},
				slice2: []string{"table1", "table2", "table3"},
			},
			want: false,
		},
		{
			name: "Mismatched slices (one empty)",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3"},
				slice2: []string{},
			},
			want: false,
		},
		{
			name: "Mismatched slices (different lengths)",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3"},
				slice2: []string{"table1", "table2"},
			},
			want: false,
		},
		{
			name: "Slices with duplicates (equal)",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3", "table3"},
				slice2: []string{"table3", "table3", "table2", "table1"},
			},
			want: true,
		},
		{
			name: "Slices with duplicates (not equal)",
			args: args[string]{
				slice1: []string{"table1", "table2", "table3", "table3"},
				slice2: []string{"table3", "table2", "table1"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.SlicesEqual(tt.args.slice1, tt.args.slice2); got != tt.want {
				t.Errorf("SlicesEqual() = %v, want %v", got, tt.want)
			}
		})
	}

	intTests := []testCase[int]{
		{
			name: "Equal int slices with same order",
			args: args[int]{
				slice1: []int{1, 2, 3, 4, 5},
				slice2: []int{1, 2, 3, 4, 5},
			},
			want: true,
		},
		{
			name: "Equal int slices with different order",
			args: args[int]{
				slice1: []int{1, 2, 3, 4, 5},
				slice2: []int{5, 4, 3, 2, 1},
			},
			want: true,
		},
		{
			name: "Mismatched int slices (different elements)",
			args: args[int]{
				slice1: []int{1, 2, 3, 4, 6},
				slice2: []int{1, 2, 3, 4, 5},
			},
			want: false,
		},
		{
			name: "Slices with duplicates (equal, ints)",
			args: args[int]{
				slice1: []int{1, 2, 3, 3, 4},
				slice2: []int{3, 3, 4, 2, 1},
			},
			want: true,
		},
		{
			name: "Slices with duplicates (not equal, ints)",
			args: args[int]{
				slice1: []int{1, 2, 3, 3, 4},
				slice2: []int{1, 2, 3, 4},
			},
			want: false,
		},
	}

	for _, tt := range intTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.SlicesEqual(tt.args.slice1, tt.args.slice2); got != tt.want {
				t.Errorf("SlicesEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
