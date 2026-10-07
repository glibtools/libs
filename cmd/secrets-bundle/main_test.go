package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"
	"filippo.io/age/armor"
	"golang.org/x/crypto/ssh"
)

const keyRel = "private/rpc/v3-x25519.key"

// fixture is a synthetic project with a synthetic SSH identity; nothing touches the
// real ~/.ssh/ait.pem or the network.
type fixture struct {
	t        *testing.T
	root     string
	identity string
	logPath  string
}

func newFixture(t *testing.T, recovery []string, neverPack []string) *fixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, root: root, logPath: filepath.Join(root, "tmp", "sb.log")}
	f.identity = f.sshKey("id")
	f.writePolicy(recovery, neverPack)
	return f
}

func (f *fixture) writePolicy(recovery []string, neverPack []string) {
	pol := map[string]any{
		"version": 1, "private_dir": "private", "bundle": "deploy/secrets-bundle.age",
		"allowed": []string{"rpc/v3-x25519.key"}, "never_pack": neverPack,
		"recovery_recipients": recovery, "primary_only_candidate": len(recovery) == 0,
		"rpc_v3": map[string]string{"secret": "rpc/v3-x25519.key", "public_manifest": "deploy/rpc-v3-public.json",
			"rpc_aad_path": "/rpc", "public_url_path": "/api/rpc"},
	}
	b, _ := json.Marshal(pol)
	f.write("deploy/secrets-policy.json", string(b))
}

func (f *fixture) sshKey(name string) string {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		f.t.Fatal(err)
	}
	blk, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		f.t.Fatal(err)
	}
	p := filepath.Join(f.t.TempDir(), name)
	if err = os.WriteFile(p, pem.EncodeToMemory(blk), 0o600); err != nil {
		f.t.Fatal(err)
	}
	signer, _ := ssh.NewSignerFromKey(priv)
	if err = os.WriteFile(p+".pub", ssh.MarshalAuthorizedKey(signer.PublicKey()), 0o644); err != nil {
		f.t.Fatal(err)
	}
	return p
}

func (f *fixture) write(rel, s string) {
	p := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) read(rel string) []byte {
	b, _ := os.ReadFile(filepath.Join(f.root, rel))
	return b
}

func (f *fixture) run(identity string, args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	full := append([]string{"-project-root", f.root, "-policy", "deploy/secrets-policy.json", "-log", f.logPath}, args...)
	env := func(k string) string {
		if k == "SECRETS_KEY" {
			return identity
		}
		return ""
	}
	code := run(full, env, &out, &errOut)
	return code, out.String(), errOut.String()
}

func (f *fixture) must(args ...string) string {
	f.t.Helper()
	code, out, errOut := f.run(f.identity, args...)
	if code != 0 {
		f.t.Fatalf("%v: code=%d stderr=%q", args, code, errOut)
	}
	return out
}

func (f *fixture) wantErr(code failure, args ...string) {
	f.t.Helper()
	_, _, errOut := f.run(f.identity, args...)
	if strings.TrimSpace(errOut) != "error="+string(code) {
		f.t.Fatalf("%v: stderr=%q want %s", args, errOut, code)
	}
}

func (f *fixture) manifest() publicManifest {
	var m publicManifest
	if err := json.Unmarshal(f.read("deploy/rpc-v3-public.json"), &m); err != nil {
		f.t.Fatal(err)
	}
	return m
}

func TestPrepareOnceRestoreAndVerify(t *testing.T) {
	f := newFixture(t, nil, nil)
	out := f.must("prepare")
	key := f.read(keyRel)
	if !validSecret(key) || bytes.HasSuffix(key, []byte("\n")) {
		t.Fatal("generated secret invalid")
	}
	assertMode(t, filepath.Join(f.root, keyRel), 0o600)
	assertMode(t, filepath.Join(f.root, "private"), 0o700)
	m := f.manifest()
	pub, _ := publicKey(key)
	if m.X25519Public != pub || m.Version != 3 || m.ReleaseStatus != "candidate" || m.RPCAADPath != "/rpc" ||
		strings.Join(m.ReleaseGates, ",") != gateRecoveryMissing+","+gateCASetMissing {
		t.Fatalf("manifest %+v", m)
	}
	bundle := f.read("deploy/secrets-bundle.age")
	if n, _ := stanzas(bundle); n != 1 {
		t.Fatalf("stanzas=%d", n)
	}

	// Second prepare restores nothing and never regenerates or repacks.
	out += f.must("prepare")
	if !bytes.Equal(f.read(keyRel), key) || !bytes.Equal(f.read("deploy/secrets-bundle.age"), bundle) {
		t.Fatal("prepare regenerated or repacked")
	}
	out += f.must("pack")
	if !strings.Contains(out, "pack=unchanged") || !bytes.Equal(f.read("deploy/secrets-bundle.age"), bundle) {
		t.Fatal("pack not idempotent")
	}

	// Clear private: prepare must restore the same key from the bundle.
	if err := os.RemoveAll(filepath.Join(f.root, "private")); err != nil {
		t.Fatal(err)
	}
	out += f.must("prepare")
	if !bytes.Equal(f.read(keyRel), key) {
		t.Fatal("restore produced a different key")
	}
	out += f.must("verify")
	if !strings.Contains(out, "release_ready=false gates=recovery_missing,ca_set_missing") {
		t.Fatalf("verify status %q", out)
	}
	f.wantErr(errReleaseGate, "-release", "verify")
	assertNoLeak(t, f, key, out)
}

func TestPrepareWithForeignIdentityNeverGenerates(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	key := f.read(keyRel)
	if err := os.RemoveAll(filepath.Join(f.root, "private")); err != nil {
		t.Fatal(err)
	}
	other := f.sshKey("other")
	_, _, errOut := f.run(other, "prepare")
	if strings.TrimSpace(errOut) != "error="+string(errBundleDecrypt) || f.read(keyRel) != nil {
		t.Fatalf("stderr=%q", errOut)
	}
	f.must("restore")
	if !bytes.Equal(f.read(keyRel), key) {
		t.Fatal("restore mismatch")
	}
}

func TestRestoreNeverOverwritesDifferentKey(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	other := "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
	f.write(keyRel, other)
	f.wantErr(errConflict, "restore")
	f.wantErr(errConflict, "prepare")
	f.wantErr(errMismatch, "verify")
	if string(f.read(keyRel)) != other {
		t.Fatal("existing key overwritten")
	}
}

func TestPackRejectsUnsafeContent(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	f.write("private/extra.txt", "x")
	f.wantErr(errNotAllowed, "pack")
	if err := os.Remove(filepath.Join(f.root, "private/extra.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(f.root, keyRel), filepath.Join(f.root, "private/link")); err != nil {
		t.Fatal(err)
	}
	f.wantErr(errPath, "pack")
	if err := os.Remove(filepath.Join(f.root, "private/link")); err != nil {
		t.Fatal(err)
	}
	f.writePolicy(nil, []string{"private/backup/"})
	f.write("private/backup/old.key", "x")
	f.wantErr(errNeverPack, "pack")
	f.writePolicy(nil, []string{"private/rpc/"})
	f.wantErr(errNeverPack, "pack")
}

func TestPolicyRejectsBadPaths(t *testing.T) {
	f := newFixture(t, nil, nil)
	for _, bad := range []string{"../x", "/abs", "a/../b", "a//b", "./a", "a\\b", "a b"} {
		f.write("deploy/secrets-policy.json", `{"version":1,"private_dir":"private","bundle":"deploy/b.age",`+
			`"allowed":[`+string(mustJSON(bad))+`],"primary_only_candidate":true}`)
		f.wantErr(errPolicy, "pack")
	}
	f.write("deploy/secrets-policy.json", `{"version":1,"private_dir":"private","bundle":"deploy/b.age","allowed":["k"]}`)
	f.wantErr(errRecoveryMissing, "pack")
}

func TestGitIgnoreGateBeforePlaintext(t *testing.T) {
	for name, fake := range map[string]func(string, string) (bool, error){
		"negated":        func(_, rel string) (bool, error) { return rel != keyRel, nil },
		"tracked":        func(_, rel string) (bool, error) { return rel != keyRel+".partial", nil },
		"command_failed": func(string, string) (bool, error) { return false, errors.New("exit 128") },
		"real_git_not_a_repo": func(root, rel string) (bool, error) {
			return realGitIgnored(root, rel) // read-only git on a non-repository temp dir
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil, nil)
			gitIgnored = fake
			t.Cleanup(func() { gitIgnored = ignoreAll })
			_, out, errOut := f.run(f.identity, "prepare")
			if errOut != "error="+string(errGitignore)+"\n" || out != "" {
				t.Fatalf("stdout=%q stderr=%q", out, errOut)
			}
			if _, err := os.Lstat(filepath.Join(f.root, "private")); err == nil {
				t.Fatal("plaintext created despite gitignore gate")
			}
		})
	}
}

func TestStrictJSON(t *testing.T) {
	f := newFixture(t, nil, nil)
	base := `{"version":1,"private_dir":"private","bundle":"deploy/b.age","allowed":["k"],"primary_only_candidate":true}`
	for _, bad := range []string{
		base + "}", base + "{}", base + "x",
		strings.Replace(base, `"allowed":["k"]`, `"allowed":["k"],"allowed":["j"]`, 1),
		strings.Replace(base, `"allowed":["k"]`, `"allowed":["k"],"Allowed":["j"]`, 1),
		strings.Replace(base, `"version":1`, `"version":1,"extra":1`, 1),
	} {
		f.write("deploy/secrets-policy.json", bad)
		f.wantErr(errPolicy, "pack")
	}
	f.write("deploy/secrets-policy.json", base+" \n")
	f.wantErr(errMissing, "pack")

	f.writePolicy(nil, nil)
	f.must("prepare")
	if err := os.RemoveAll(filepath.Join(f.root, "private")); err != nil {
		t.Fatal(err)
	}
	k := f.keyring(f.identity)
	names, _ := json.Marshal(k.names)
	good := `{"format":"` + payloadFormat + `","recipients":` + string(names) + `,"files":[]}`
	for i, bad := range []string{
		good + " \n", good + "}", good + "{}",
		strings.Replace(good, `"files":[]`, `"files":[],"files":[]`, 1),
		strings.Replace(good, `"files":[]`, `"files":[],"FILES":[]`, 1),
	} {
		var buf bytes.Buffer
		aw := armor.NewWriter(&buf)
		w, err := age.Encrypt(aw, k.recipients...)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(bad))
		_ = w.Close()
		_ = aw.Close()
		f.write("deploy/secrets-bundle.age", buf.String())
		if i == 0 {
			f.must("restore") // baseline: only strictness makes the others fail
			continue
		}
		f.wantErr(errPayload, "restore")
	}
}

func TestBadBundleKeptAndRejected(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	bad := []byte("-----BEGIN AGE ENCRYPTED FILE-----\nYWdl\n-----END AGE ENCRYPTED FILE-----\n")
	f.write("deploy/secrets-bundle.age", string(bad))
	f.wantErr(errBundleDecrypt, "pack")
	f.wantErr(errBundleDecrypt, "restore")
	if !bytes.Equal(f.read("deploy/secrets-bundle.age"), bad) {
		t.Fatal("undecryptable bundle replaced")
	}
}

func TestMaliciousPayloadWritesNothing(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	if err := os.RemoveAll(filepath.Join(f.root, "private")); err != nil {
		t.Fatal(err)
	}
	k := f.keyring(f.identity)
	for _, tc := range []struct {
		files []payloadFile
		code  failure
	}{
		{[]payloadFile{{Path: "../escape", Data: []byte("x")}}, errPath},
		{[]payloadFile{{Path: "other.key", Data: []byte("x")}}, errNotAllowed},
		{[]payloadFile{{Path: "rpc/v3-x25519.key", Data: []byte("short")}}, errSecret},
	} {
		ct, err := encrypt(&payload{Format: payloadFormat, Recipients: k.names, Files: tc.files}, k.recipients)
		if err != nil {
			t.Fatal(err)
		}
		f.write("deploy/secrets-bundle.age", string(ct))
		f.wantErr(tc.code, "restore")
		if _, err = os.Stat(filepath.Join(f.root, "private")); err == nil {
			t.Fatal("restore wrote files before validation")
		}
	}
}

func TestDualRecipientAndRekey(t *testing.T) {
	rec, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t, []string{rec.Recipient().String()}, nil)
	f.must("prepare")
	key := f.read(keyRel)
	bundle := f.read("deploy/secrets-bundle.age")
	if n, _ := stanzas(bundle); n != 2 {
		t.Fatalf("stanzas=%d", n)
	}
	if got := decryptWith(t, bundle, rec); !bytes.Contains(got, []byte(payloadFormat)) {
		t.Fatal("recovery identity cannot decrypt")
	}
	if m := f.manifest(); strings.Join(m.ReleaseGates, ",") != gateCASetMissing {
		t.Fatalf("gates %v", m.ReleaseGates)
	}

	newID := f.sshKey("new")
	f.must("-new-primary", newID+".pub", "rekey")
	rekeyed := f.read("deploy/secrets-bundle.age")
	if bytes.Equal(rekeyed, bundle) || !bytes.Equal(f.read(keyRel), key) {
		t.Fatal("rekey did not re-encrypt or touched the secret")
	}
	_, _, errOut := f.run(f.identity, "verify")
	if strings.TrimSpace(errOut) != "error="+string(errBundleDecrypt) {
		t.Fatalf("old identity still decrypts: %q", errOut)
	}
	code, _, errOut := f.run(newID, "verify")
	if code != 0 {
		t.Fatalf("new identity verify: %q", errOut)
	}
	var p payload
	if err = json.Unmarshal(decryptWith(t, rekeyed, rec), &p); err != nil || len(p.Files) != 1 || !bytes.Equal(p.Files[0].Data, key) {
		t.Fatal("rekey changed payload")
	}
	code, out, _ := f.run(newID, "rekey")
	if code != 0 || !strings.Contains(out, "rekey=unchanged") {
		t.Fatalf("rekey not idempotent: %q", out)
	}
}

func TestBackupRequiresConfirmationAndStructuredArgv(t *testing.T) {
	f := newFixture(t, nil, nil)
	f.must("prepare")
	var got []string
	orig := runCommand
	runCommand = func(argv []string) error { got = argv; return nil }
	t.Cleanup(func() { runCommand = orig })
	f.wantErr(errNetwork, "-host", "backup-host", "-dest", "srv/ait", "backup")
	f.wantErr(errHost, "-allow-network", "-host", "-oProxyCommand=x", "-dest", "srv", "backup")
	f.wantErr(errHost, "-allow-network", "-host", "h", "-dest", "../x", "backup")
	if got != nil {
		t.Fatal("command executed despite rejection")
	}
	f.must("-allow-network", "-host", "backup-host", "-dest", "srv/ait", "backup")
	want := []string{"scp", "-q", "-o", "BatchMode=yes", "--", filepath.Join(f.root, "deploy/secrets-bundle.age"), "backup-host:srv/ait/secrets-bundle.age"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv %q", got)
	}
}

var (
	realGitIgnored = gitIgnored
	ignoreAll      = func(string, string) (bool, error) { return true, nil }
)

// TestMain swaps the read-only git checker: temp dirs are not repositories and tests
// must not run git writes or init repos. The real checker is covered separately.
func TestMain(m *testing.M) {
	gitIgnored = ignoreAll
	os.Exit(m.Run())
}

func (f *fixture) keyring(identity string) *keyring {
	a := &app{identityPath: identity, policy: &policy{PrimaryOnlyCandidate: true}}
	k, err := a.keys()
	if err != nil {
		f.t.Fatal(err)
	}
	return k
}

func decryptWith(t *testing.T, data []byte, id age.Identity) []byte {
	t.Helper()
	r, err := age.Decrypt(armor.NewReader(bytes.NewReader(data)), id)
	if err != nil {
		t.Fatal("decrypt failed")
	}
	var buf bytes.Buffer
	if _, err = buf.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func assertMode(t *testing.T, p string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil || fi.Mode().Perm() != want {
		t.Fatalf("mode of %s", filepath.Base(p))
	}
}

// assertNoLeak checks stdout and the 0600 log never contain the secret, the public
// key, the identity path or any ssh key text.
func assertNoLeak(t *testing.T, f *fixture, key []byte, out string) {
	t.Helper()
	assertMode(t, f.logPath, 0o600)
	pub, _ := publicKey(key)
	logText := string(f.read("tmp/sb.log"))
	for _, s := range []string{string(key), pub, f.identity, "ssh-ed25519", "BEGIN", keyRel} {
		if strings.Contains(out, s) || strings.Contains(logText, s) {
			t.Fatal("output or log leaks sensitive material")
		}
	}
	if !strings.Contains(logText, "result=ok") {
		t.Fatal("log missing fixed result lines")
	}
}

func mustJSON(s string) []byte {
	b, _ := json.Marshal(s)
	return b
}
