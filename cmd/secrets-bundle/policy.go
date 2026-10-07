package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"filippo.io/age"
	"filippo.io/age/agessh"
	"golang.org/x/crypto/ssh"
)

const (
	maxPolicySize = 64 << 10
	maxKeySize    = 64 << 10
	maxPathLen    = 256
	maxFiles      = 32
	maxFileSize   = 64 << 10
)

// policy is the per-project contract (e.g. deploy/secrets-policy.json). All paths are
// slash-separated and relative: private_dir/bundle/never_pack to the project root,
// allowed and rpc_v3.secret to private_dir.
type policy struct {
	Version              int          `json:"version"`
	PrivateDir           string       `json:"private_dir"`
	Bundle               string       `json:"bundle"`
	Allowed              []string     `json:"allowed"`
	NeverPack            []string     `json:"never_pack"`
	RecoveryRecipients   []string     `json:"recovery_recipients"`
	PrimaryOnlyCandidate bool         `json:"primary_only_candidate"`
	RPCV3                *rpcV3Policy `json:"rpc_v3"`
}

// rpcV3Policy enables the permanent X25519 HPKE server key and its public manifest.
type rpcV3Policy struct {
	Secret         string `json:"secret"`
	PublicManifest string `json:"public_manifest"`
	RPCAADPath     string `json:"rpc_aad_path"`
	PublicURLPath  string `json:"public_url_path"`
}

// keyring holds the decrypting identity and the recipient set derived from policy.
type keyring struct {
	identity   age.Identity
	recipients []age.Recipient
	names      []string
}

func (a *app) loadPolicy(rel string) (*policy, error) {
	b, err := a.readRel(rel, maxPolicySize)
	if err != nil {
		return nil, errPolicy
	}
	p := &policy{}
	if decodeStrict(b, p) != nil {
		return nil, errPolicy
	}
	return p, p.validate()
}

// decodeStrict rejects duplicate object keys (case-folded, as encoding/json matches
// field names case-insensitively), unknown fields and any data after the value.
func decodeStrict(b []byte, v any) error {
	if !uniqueKeys(json.NewDecoder(bytes.NewReader(b))) {
		return errPayload
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(v) != nil || dec.Decode(&json.RawMessage{}) != io.EOF {
		return errPayload
	}
	return nil
}

func uniqueKeys(dec *json.Decoder) bool {
	type frame struct {
		keys      map[string]bool
		expectKey bool
	}
	var stack []*frame
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return len(stack) == 0
		}
		if err != nil {
			return false
		}
		n := len(stack)
		if key, ok := tok.(string); ok && n > 0 && stack[n-1].expectKey {
			k := strings.ToLower(key)
			if stack[n-1].keys[k] {
				return false
			}
			stack[n-1].keys[k], stack[n-1].expectKey = true, false
			continue
		}
		switch tok {
		case json.Delim('{'):
			stack = append(stack, &frame{keys: map[string]bool{}, expectKey: true})
			continue
		case json.Delim('['):
			stack = append(stack, &frame{})
			continue
		case json.Delim('}'), json.Delim(']'):
			stack = stack[:n-1]
		}
		if len(stack) > 0 && stack[len(stack)-1].keys != nil {
			stack[len(stack)-1].expectKey = true
		}
	}
}

func (p *policy) validate() error {
	if p.Version != 1 || cleanRel(p.PrivateDir) != nil || cleanRel(p.Bundle) != nil || inDir(p.Bundle, p.PrivateDir) {
		return errPolicy
	}
	if len(p.Allowed) == 0 || len(p.Allowed) > maxFiles {
		return errPolicy
	}
	for i, f := range p.Allowed {
		if cleanRel(f) != nil || slices.Contains(p.Allowed[:i], f) {
			return errPolicy
		}
	}
	for _, n := range p.NeverPack {
		if cleanRel(strings.TrimSuffix(n, "/")) != nil {
			return errPolicy
		}
	}
	for _, f := range p.Allowed {
		if p.neverPack(path.Join(p.PrivateDir, f)) {
			return errNeverPack
		}
	}
	for _, r := range p.RecoveryRecipients {
		if _, err := age.ParseX25519Recipient(r); err != nil {
			return errRecipient
		}
	}
	if len(p.RecoveryRecipients) == 0 && !p.PrimaryOnlyCandidate {
		return errRecoveryMissing
	}
	if p.RPCV3 == nil {
		return nil
	}
	v := p.RPCV3
	if !slices.Contains(p.Allowed, v.Secret) || cleanRel(v.PublicManifest) != nil || inDir(v.PublicManifest, p.PrivateDir) {
		return errPolicy
	}
	if !urlPath(v.RPCAADPath) || !urlPath(v.PublicURLPath) {
		return errPolicy
	}
	return nil
}

// neverPack reports whether a project-relative path is listed as never packable.
// Entries ending in "/" match a whole directory.
func (p *policy) neverPack(rel string) bool {
	for _, n := range p.NeverPack {
		if rel == strings.TrimSuffix(n, "/") || inDir(rel, strings.TrimSuffix(n, "/")) {
			return true
		}
	}
	return false
}

// gitIgnored asks Git (read-only `check-ignore`, never --no-index) whether rel is
// ignored: tracked or negated paths, a missing git or a non-repository all fail.
// Structured argv, no shell, output discarded; tests replace it.
var gitIgnored = func(root, rel string) (bool, error) {
	cmd := exec.Command("git", "-C", root, "check-ignore", "--quiet", "--", rel)
	for _, pair := range os.Environ() {
		if !strings.HasPrefix(pair, "GIT_") {
			cmd.Env = append(cmd.Env, pair)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return err == nil, err
}

// checkIgnored runs before any plaintext is created: every allowed file and its
// .partial sibling must be ignored by the project's real Git rules.
func (a *app) checkIgnored() error {
	for _, f := range a.policy.Allowed {
		rel := path.Join(a.policy.PrivateDir, f)
		for _, p := range []string{rel, rel + ".partial"} {
			ok, err := gitIgnored(a.root, p)
			if err != nil || !ok {
				return errGitignore
			}
		}
	}
	return nil
}

func (a *app) keys() (*keyring, error) {
	b, err := readLimited(a.identityPath, maxKeySize)
	if err != nil {
		return nil, errIdentity
	}
	defer clear(b)
	signer, err := ssh.ParsePrivateKey(b)
	var missing *ssh.PassphraseMissingError
	if errors.As(err, &missing) {
		return nil, errIdentityLocked
	}
	if err != nil {
		return nil, errIdentity
	}
	id, err := agessh.ParseIdentity(b)
	if err != nil {
		return nil, errIdentity
	}
	k := &keyring{identity: id}
	return k, k.setRecipients(string(ssh.MarshalAuthorizedKey(signer.PublicKey())), a.policy.RecoveryRecipients)
}

func (k *keyring) setRecipients(primary string, recovery []string) error {
	primary = strings.TrimSpace(primary)
	r, err := agessh.ParseRecipient(primary)
	if err != nil {
		return errRecipient
	}
	k.recipients, k.names = []age.Recipient{r}, []string{primary}
	for _, s := range recovery {
		x, err := age.ParseX25519Recipient(s)
		if err != nil || slices.Contains(k.names, s) {
			return errRecipient
		}
		k.recipients, k.names = append(k.recipients, x), append(k.names, s)
	}
	slices.Sort(k.names)
	return nil
}

// cleanRel accepts only clean, local, slash-separated relative paths.
func cleanRel(p string) error {
	if p == "" || len(p) > maxPathLen || strings.ContainsAny(p, `\:`) || path.Clean(p) != p || !filepath.IsLocal(p) {
		return errPath
	}
	for _, c := range p {
		if c < 0x21 || c > 0x7e {
			return errPath
		}
	}
	for _, e := range strings.Split(p, "/") {
		if e == "." || e == ".." {
			return errPath
		}
	}
	return nil
}

func inDir(rel, dir string) bool { return strings.HasPrefix(rel, dir+"/") }

func urlPath(p string) bool {
	return strings.HasPrefix(p, "/") && cleanRel(strings.TrimPrefix(p, "/")) == nil
}

// safeRel validates a slash-separated project path and rejects any existing symlink
// or non-directory component, returning the native name for a.fsys. All project file
// I/O goes through a.fsys (os.Root), which also refuses to resolve outside the
// project root if a component is swapped after this check; an in-root swap race is
// not excluded.
func (a *app) safeRel(rel string) (string, error) {
	if cleanRel(rel) != nil {
		return "", errPath
	}
	parts := strings.Split(rel, "/")
	for i := range parts {
		fi, err := a.fsys.Lstat(filepath.FromSlash(strings.Join(parts[:i+1], "/")))
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil || fi.Mode()&os.ModeSymlink != 0 || (i < len(parts)-1 && !fi.IsDir()) {
			return "", errPath
		}
	}
	return filepath.FromSlash(rel), nil
}

func (a *app) readRel(rel string, limit int64) ([]byte, error) {
	name, err := a.safeRel(rel)
	if err != nil {
		return nil, err
	}
	f, err := a.fsys.Open(name)
	if err != nil {
		return nil, err
	}
	return readFile(f, limit)
}

func readLimited(p string, limit int64) ([]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	return readFile(f, limit)
}

func readFile(f *os.File, limit int64) ([]byte, error) {
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		clear(b)
		return nil, errLimit
	}
	return b, nil
}
