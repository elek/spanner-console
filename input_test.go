package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHeadAndTail(t *testing.T) {
	ti := Textinput{}

	checkSplit := func(t *testing.T, text string, pos int, head, crs, tail string) {
		ti.Text = text
		ti.pos = pos
		h, c, l := ti.HeadAndTail()
		require.Equal(t, tail, l, "tail")
		require.Equal(t, crs, c, "cursor")
		require.Equal(t, head, h, "head")
	}
	checkSplit(t, "abcd", 0, "", "a", "bcd")
	checkSplit(t, "abcd", 1, "a", "b", "cd")
	checkSplit(t, "a", 1, "a", "", "")
	checkSplit(t, "a", 0, "", "a", "")

}
