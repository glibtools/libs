package main

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"filippo.io/age/agessh"
)

const (
	gateRecoveryMissing = "recovery_missing"
	gateCASetMissing    = "ca_set_missing"
)

var (
	secretB64    = base64.RawURLEncoding.Strict()
	placeholders = []string{"change", "replace", "placeholder", "example", "dummy", "your", "<"}
	hostRe       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)
	destRe       = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]{0,255}$`)
)

// publicManifest is the only public RPC v3 key material: the permanent X25519 public
// key, the HPKE suite, the AAD path mapping and the open release gates.
type publicManifest struct {
	Version       int         `json:"version"`
	Suite         publicSuite `json:"suite"`
	X25519Public  string      `json:"x25519_public"`
	RPCAADPath    string      `json:"rpc_aad_path"`
	PublicURLPath string      `json:"public_url_path"`
	ReleaseStatus string      `json:"release_status"`
	ReleaseGates  []string    `json:"release_gates"`
}

type publicSuite struct {
	Mode string `json:"mode"`
	KEM  string `json:"kem"`
	KDF  string `json:"kdf"`
	AEAD string `json:"aead"`
}

// prepare restores from an existing bundle (never generating a different secret), or
// on a new project generates the X25519 secret once, packs it and writes the manifest.
func (a *app) prepare() error {
	if err := a.checkIgnored(); err != nil {
		return err
	}
	k, err := a.keys()
	if err != nil {
		return err
	}
	if a.bundleExists() {
		if err = a.restoreWith(k); err != nil {
			return err
		}
		return a.writeManifest(k)
	}
	if err = a.ensureSecret(); err != nil {
		return err
	}
	if _, err = a.pack(k); err != nil {
		return err
	}
	return a.writeManifest(k)
}

func (a *app) restore() error {
	if err := a.checkIgnored(); err != nil {
		return err
	}
	k, err := a.keys()
	if err != nil {
		return err
	}
	return a.restoreWith(k)
}

func (a *app) restoreWith(k *keyring) error {
	p, err := a.readBundle(k)
	if err != nil {
		return err
	}
	defer p.wipe()
	n, err := a.restoreFiles(p)
	if err != nil {
		return err
	}
	a.status("restored=" + strconv.Itoa(n))
	return nil
}

func (a *app) packCmd() error {
	if err := a.checkIgnored(); err != nil {
		return err
	}
	k, err := a.keys()
	if err != nil {
		return err
	}
	if _, err = a.pack(k); err != nil {
		return err
	}
	return a.writeManifest(k)
}

// pack encrypts the private files; an existing bundle is decrypted first and kept
// untouched when content and recipient set are unchanged or when it cannot be opened.
func (a *app) pack(k *keyring) (bool, error) {
	files, err := a.collect()
	if err != nil {
		return false, err
	}
	want := &payload{Format: payloadFormat, Recipients: k.names, Files: files}
	defer want.wipe()
	if a.bundleExists() {
		old, err := a.readBundle(k)
		if err != nil {
			return false, err
		}
		same := samePayload(old, want)
		old.wipe()
		if same && a.stanzaCount() == len(k.recipients) {
			a.status("pack=unchanged")
			return false, nil
		}
	}
	return true, a.seal(want, k)
}

func (a *app) seal(p *payload, k *keyring) error {
	ct, err := encrypt(p, k.recipients)
	if err != nil {
		return err
	}
	if !noPlaintext(ct, p) {
		return errPlaintext
	}
	if _, err = a.writePublic(a.policy.Bundle, ct); err != nil {
		return err
	}
	a.status("pack=written recipients=" + strconv.Itoa(len(k.recipients)))
	return nil
}

// verify checks bundle/private consistency, recipients, permissions, ciphertext
// markers and the public manifest. Release readiness is reported separately and only
// enforced with -release, so a consistent primary-only candidate never reads as ready.
func (a *app) verify() error {
	k, err := a.keys()
	if err != nil {
		return err
	}
	data, err := a.bundleBytes()
	if err != nil {
		return err
	}
	p, err := a.decrypt(data, k.identity)
	if err != nil {
		return err
	}
	defer p.wipe()
	local, err := a.collect()
	if err != nil {
		return err
	}
	same := samePayload(p, &payload{Format: payloadFormat, Recipients: p.Recipients, Files: local})
	(&payload{Files: local}).wipe()
	if !same {
		return errMismatch
	}
	if !slices.Equal(p.Recipients, k.names) || a.stanzaCount() != len(k.recipients) {
		return errRecipientsStale
	}
	if err = a.checkPerms(p); err != nil {
		return err
	}
	if !noPlaintext(data, p) {
		return errPlaintext
	}
	if err = a.checkManifest(k); err != nil {
		return err
	}
	gates := a.gates(k)
	a.status("content=ok public=ok perms=ok plaintext_markers=absent recipients=" + strconv.Itoa(len(k.recipients)) +
		" release_ready=" + strconv.FormatBool(len(gates) == 0) + " gates=" + strings.Join(gates, ","))
	if a.release && len(gates) > 0 {
		return errReleaseGate
	}
	return nil
}

// rekey re-encrypts the existing payload to the current recipient policy (optionally
// a new primary SSH public key) without touching or regenerating any secret.
func (a *app) rekey() error {
	k, err := a.keys()
	if err != nil {
		return err
	}
	p, err := a.readBundle(k)
	if err != nil {
		return err
	}
	defer p.wipe()
	if a.newPrimary != "" {
		b, err := readLimited(a.newPrimary, maxKeySize)
		if err != nil {
			return errRecipient
		}
		if _, err = agessh.ParseRecipient(strings.TrimSpace(string(b))); err != nil {
			return errRecipient
		}
		if err = k.setRecipients(string(b), a.policy.RecoveryRecipients); err != nil {
			return err
		}
	}
	if slices.Equal(p.Recipients, k.names) && a.stanzaCount() == len(k.recipients) {
		a.status("rekey=unchanged")
		return a.writeManifest(k)
	}
	p.Recipients = k.names
	if err = a.seal(p, k); err != nil {
		return err
	}
	return a.writeManifest(k)
}

// backup copies only the ciphertext bundle with a structured scp argv (no shell). It
// never runs without -allow-network and never sends plaintext.
func (a *app) backup() error {
	if !a.allowNetwork {
		return errNetwork
	}
	if !hostRe.MatchString(a.host) || !destRe.MatchString(a.dest) || strings.Contains(a.dest, "..") {
		return errHost
	}
	k, err := a.keys()
	if err != nil {
		return err
	}
	p, err := a.readBundle(k)
	if err != nil {
		return err
	}
	p.wipe()
	src, err := a.safeRel(a.policy.Bundle)
	if err != nil {
		return err
	}
	src = filepath.Join(a.root, src)
	argv := []string{"scp", "-q", "-o", "BatchMode=yes", "--", src, a.host + ":" + path.Join(a.dest, path.Base(a.policy.Bundle))}
	if runCommand(argv) != nil {
		return errBackup
	}
	a.status("backup=copied")
	return nil
}

// ensureSecret keeps an existing valid X25519 secret or generates one exactly once.
func (a *app) ensureSecret() error {
	if a.policy.RPCV3 == nil {
		return nil
	}
	rel := path.Join(a.policy.PrivateDir, a.policy.RPCV3.Secret)
	target, err := a.safeRel(rel)
	if err != nil {
		return err
	}
	if cur, err := a.readRel(rel, maxFileSize); err == nil {
		ok := validSecret(cur)
		clear(cur)
		if !ok {
			return errSecret
		}
		a.status("secret=kept")
		return nil
	}
	if _, err = a.fsys.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		return errPath
	}
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return errSecret
	}
	raw := key.Bytes()
	enc := []byte(secretB64.EncodeToString(raw))
	clear(raw)
	defer clear(enc)
	if !validSecret(enc) {
		return errSecret
	}
	if err = a.writePrivate(rel, enc); err != nil {
		return err
	}
	a.status("secret=generated")
	return nil
}

func (a *app) writeManifest(k *keyring) error {
	m, err := a.manifest(k)
	if err != nil || m == nil {
		return err
	}
	changed, err := a.writePublic(a.policy.RPCV3.PublicManifest, m)
	if err != nil {
		return err
	}
	state := "unchanged"
	if changed {
		state = "written"
	}
	a.status("manifest=" + state)
	return nil
}

func (a *app) checkManifest(k *keyring) error {
	want, err := a.manifest(k)
	if err != nil || want == nil {
		return err
	}
	cur, err := a.readRel(a.policy.RPCV3.PublicManifest, maxPolicySize)
	if err != nil || !bytes.Equal(cur, want) {
		return errPublicMismatch
	}
	return nil
}

func (a *app) manifest(k *keyring) ([]byte, error) {
	if a.policy.RPCV3 == nil {
		return nil, nil
	}
	b, err := a.readRel(path.Join(a.policy.PrivateDir, a.policy.RPCV3.Secret), maxFileSize)
	if err != nil {
		return nil, errMissing
	}
	defer clear(b)
	pub, err := publicKey(b)
	if err != nil {
		return nil, err
	}
	gates := a.gates(k)
	status := "release"
	if len(gates) > 0 {
		status = "candidate"
	}
	m := publicManifest{Version: 3, X25519Public: pub, RPCAADPath: a.policy.RPCV3.RPCAADPath,
		PublicURLPath: a.policy.RPCV3.PublicURLPath, ReleaseStatus: status, ReleaseGates: gates,
		Suite: publicSuite{Mode: "base", KEM: "DHKEM(X25519, HKDF-SHA256)", KDF: "HKDF-SHA256", AEAD: "AES-256-GCM"}}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, errWrite
	}
	return append(out, '\n'), nil
}

// gates lists open release gates. CA-set collection belongs to the client TLS
// subsystem and is never satisfied by this tool, so RPC v3 always reports it.
func (a *app) gates(k *keyring) []string {
	gates := []string{}
	if len(k.recipients) < 2 {
		gates = append(gates, gateRecoveryMissing)
	}
	if a.policy.RPCV3 != nil {
		gates = append(gates, gateCASetMissing)
	}
	return gates
}

func (a *app) checkPerms(p *payload) error {
	dir, err := a.safeRel(a.policy.PrivateDir)
	if err != nil {
		return err
	}
	if fi, err := a.fsys.Lstat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		return errPerms
	}
	for _, f := range p.Files {
		target, err := a.safeRel(path.Join(a.policy.PrivateDir, f.Path))
		if err != nil {
			return err
		}
		if fi, err := a.fsys.Lstat(target); err != nil || fi.Mode().Perm() != 0o600 {
			return errPerms
		}
	}
	return nil
}

func (a *app) bundleExists() bool {
	target, err := a.safeRel(a.policy.Bundle)
	if err != nil {
		return true // fail closed: never treat an unsafe path as "no bundle"
	}
	_, err = a.fsys.Lstat(target)
	return !errors.Is(err, os.ErrNotExist)
}

func (a *app) bundleBytes() ([]byte, error) {
	target, err := a.safeRel(a.policy.Bundle)
	if err != nil {
		return nil, err
	}
	fi, err := a.fsys.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errNoBundle
	}
	if err != nil || !fi.Mode().IsRegular() {
		return nil, errBundleRead
	}
	b, err := a.readRel(a.policy.Bundle, maxBundleSize)
	if err != nil {
		return nil, errBundleRead
	}
	return b, nil
}

func (a *app) readBundle(k *keyring) (*payload, error) {
	data, err := a.bundleBytes()
	if err != nil {
		return nil, err
	}
	return a.decrypt(data, k.identity)
}

func (a *app) stanzaCount() int {
	data, err := a.bundleBytes()
	if err != nil {
		return -1
	}
	n, err := stanzas(data)
	if err != nil {
		return -1
	}
	return n
}

// validSecret mirrors the AIT loader: 32 bytes, strict unpadded base64url, not a
// placeholder and not a single repeated byte. No trailing newline is written.
func validSecret(b []byte) bool {
	text := string(bytes.TrimSpace(b))
	for _, p := range placeholders {
		if strings.HasPrefix(strings.ToLower(text), p) {
			return false
		}
	}
	raw, err := secretB64.DecodeString(text)
	defer clear(raw)
	if err != nil || len(raw) != 32 || secretB64.EncodeToString(raw) != text {
		return false
	}
	return bytes.Count(raw, raw[:1]) != len(raw)
}

func publicKey(b []byte) (string, error) {
	if !validSecret(b) {
		return "", errSecret
	}
	raw, _ := secretB64.DecodeString(string(bytes.TrimSpace(b)))
	defer clear(raw)
	key, err := ecdh.X25519().NewPrivateKey(raw)
	if err != nil {
		return "", errSecret
	}
	return secretB64.EncodeToString(key.PublicKey().Bytes()), nil
}
