package amq

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/ohade/amq-keepalive/internal/executable"
)

func TestEncodeInjectViaProofMatchesSchemaOneIdentity(t *testing.T) {
	path := writeExecutable(t, filepath.Join(t.TempDir(), "injector"), "#!/bin/sh\nexit 0\n")
	identity, err := executable.Capture(path)
	if err != nil {
		t.Fatal(err)
	}
	token, err := encodeInjectViaProof(identity)
	if err != nil {
		t.Fatal(err)
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatal(err)
	}
	var proof injectViaProof
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Schema != injectViaProofSchema || proof.Path != identity.Path || proof.SHA256 != identity.SHA256 ||
		proof.Device != identity.Device || proof.Inode != identity.Inode || proof.Size != identity.Size ||
		proof.Mode != identity.Mode || proof.UID != identity.UID || proof.GID != identity.GID ||
		proof.ModTimeNanos != identity.ModTimeNanos {
		t.Fatalf("proof=%#v identity=%#v", proof, identity)
	}
	if _, err := encodeInjectViaProof(executable.Identity{}); err == nil {
		t.Fatal("incomplete executable identity produced a proof")
	}
}
