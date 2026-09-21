//go:build unix

package demo_test

import (
	"os"
	"strings"
	"testing"

	"github.com/fomoxa/frpc-go/examples/demo"
)

func TestTheFramesScenarioMatchesFomoxaRpc010ByteForByte(t *testing.T) {
	recorded, err := os.ReadFile("testdata/frames.txt")
	if err != nil {
		t.Fatal(err)
	}
	theirs := strings.Split(strings.TrimSpace(string(recorded)), "\n")
	ours := demo.Frames()
	if len(theirs) != len(ours) {
		t.Fatalf("frame count: %d recorded, %d here", len(theirs), len(ours))
	}
	for index := range ours {
		if ours[index] != strings.TrimSpace(theirs[index]) {
			t.Fatalf("frame %d:\n recorded %s\n here     %s", index, theirs[index], ours[index])
		}
	}
}
