//go:build darwin || linux

package amq

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/ohade/amq-keepalive/internal/executable"
)

func TestRunExpectedPassesAndHoldsExactInjectViaProofDescriptor(t *testing.T) {
	dir := t.TempDir()
	injectPath := writeExecutable(t, filepath.Join(dir, "injector"), "#!/bin/sh\nprintf ORIGINAL\n")
	replacement := writeExecutable(t, filepath.Join(dir, "replacement"), "#!/bin/sh\nprintf REPLACEMENT\n")
	t.Setenv("PROOF_INJECT_PATH", injectPath)
	t.Setenv("PROOF_REPLACEMENT", replacement)
	amqPath := writeExecutable(t, filepath.Join(dir, "amq"), `#!/bin/sh
proof=""
fd=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --inject-via-proof) proof="$2"; shift 2 ;;
    --inject-via-proof-fd) fd="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[ -n "$proof" ] && [ -n "$fd" ] || exit 90
mv "$PROOF_REPLACEMENT" "$PROOF_INJECT_PATH"
printf 'PROOF=%s\nFD=%s\n' "$proof" "$fd"
cat "/dev/fd/$fd"
`)
	amqIdentity, err := executable.Capture(amqPath)
	if err != nil {
		t.Fatal(err)
	}
	injectIdentity, err := executable.Capture(injectPath)
	if err != nil {
		t.Fatal(err)
	}
	wantProof, err := encodeInjectViaProof(injectIdentity)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err := NewCLI(amqPath).runExpected(context.Background(), amqIdentity, injectIdentity, "env")
	if err != nil {
		t.Fatalf("runExpected stderr=%q err=%v", stderr, err)
	}
	wantFD := 3
	if runtime.GOOS == "linux" {
		wantFD = 4 // fd 3 is the already-pinned AMQ executable.
	}
	output := string(stdout)
	if !strings.Contains(output, "PROOF="+wantProof+"\n") ||
		!strings.Contains(output, "FD="+strconv.Itoa(wantFD)+"\n") ||
		!strings.Contains(output, "printf ORIGINAL") || strings.Contains(output, "printf REPLACEMENT") {
		t.Fatalf("proof descriptor output=%q", output)
	}
	current, err := os.ReadFile(injectPath)
	if err != nil || !strings.Contains(string(current), "REPLACEMENT") {
		t.Fatalf("inject-via replacement did not occur: data=%q err=%v", current, err)
	}
}
