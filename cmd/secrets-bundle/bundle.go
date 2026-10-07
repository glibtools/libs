package main

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"filippo.io/age"
	"filippo.io/age/armor"
)

const (
	payloadFormat  = "secrets-bundle/v1"
	maxPayloadSize = 4 << 20
	maxBundleSize  = 8 << 20
)

// payload is the plaintext inside the age stream: a bounded JSON file table (no tar).
// Recipients records the public recipient set the bundle was encrypted to, so pack can
// tell whether the recipient policy changed.
type payload struct {
	Format     string        `json:"format"`
	Recipients []string      `json:"recipients"`
	Files      []payloadFile `json:"files"`
}

type payloadFile struct {
	Path string `json:"path"`
	Data []byte `json:"data"`
}

func (p *payload) wipe() {
	for i := range p.Files {
		clear(p.Files[i].Data)
	}
}

func samePayload(a, b *payload) bool {
	if a.Format != b.Format || !slices.Equal(a.Recipients, b.Recipients) || len(a.Files) != len(b.Files) {
		return false
	}
	for i := range a.Files {
		if a.Files[i].Path != b.Files[i].Path || subtle.ConstantTimeCompare(a.Files[i].Data, b.Files[i].Data) != 1 {
			return false
		}
	}
	return true
}

func encrypt(p *payload, rs []age.Recipient) ([]byte, error) {
	plain, err := json.Marshal(p)
	if err != nil {
		return nil, errPayload
	}
	defer clear(plain)
	var buf bytes.Buffer
	aw := armor.NewWriter(&buf)
	w, err := age.Encrypt(aw, rs...)
	if err != nil {
		return nil, errRecipient
	}
	if _, err = w.Write(plain); err != nil {
		return nil, errWrite
	}
	if w.Close() != nil || aw.Close() != nil {
		return nil, errWrite
	}
	return buf.Bytes(), nil
}

func (a *app) decrypt(data []byte, id age.Identity) (*payload, error) {
	r, err := age.Decrypt(armor.NewReader(bytes.NewReader(data)), id)
	if err != nil {
		return nil, errBundleDecrypt
	}
	plain, err := io.ReadAll(io.LimitReader(r, maxPayloadSize+1))
	defer clear(plain)
	if err != nil {
		return nil, errBundleDecrypt
	}
	if len(plain) > maxPayloadSize {
		return nil, errLimit
	}
	p := &payload{}
	if err = decodeStrict(plain, p); err == nil {
		err = a.validatePayload(p)
	}
	if err != nil {
		p.wipe()
		return nil, err
	}
	return p, nil
}

func (a *app) validatePayload(p *payload) error {
	if p.Format != payloadFormat || len(p.Recipients) == 0 || !slices.IsSorted(p.Recipients) || len(p.Files) > maxFiles {
		return errPayload
	}
	for i, f := range p.Files {
		if err := a.checkEntry(f.Path); err != nil {
			return err
		}
		if (i > 0 && p.Files[i-1].Path >= f.Path) || len(f.Data) > maxFileSize {
			return errPayload
		}
		if a.policy.RPCV3 != nil && f.Path == a.policy.RPCV3.Secret && !validSecret(f.Data) {
			return errSecret
		}
	}
	return nil
}

// checkEntry enforces path rules, never_pack and the allowed whitelist for one file
// path relative to the private directory.
func (a *app) checkEntry(rel string) error {
	if cleanRel(rel) != nil {
		return errPath
	}
	if a.policy.neverPack(path.Join(a.policy.PrivateDir, rel)) {
		return errNeverPack
	}
	if !slices.Contains(a.policy.Allowed, rel) {
		return errNotAllowed
	}
	return nil
}

// stanzas counts recipient stanzas in the bundle header (structure only, no secrets).
func stanzas(data []byte) (int, error) {
	hdr, err := age.ExtractHeader(armor.NewReader(bytes.NewReader(data)))
	if err != nil {
		return 0, errBundleDecrypt
	}
	return bytes.Count(hdr, []byte("\n-> ")), nil
}

// noPlaintext checks the ciphertext carries none of the payload markers.
func noPlaintext(ct []byte, p *payload) bool {
	markers := [][]byte{[]byte(payloadFormat), []byte(`"files"`), []byte(`"recipients"`)}
	for _, f := range p.Files {
		markers = append(markers, []byte(f.Path), f.Data)
	}
	for _, m := range markers {
		if len(m) > 0 && bytes.Contains(ct, m) {
			return false
		}
	}
	return true
}

// collect reads the private directory strictly: every regular file must be allowed,
// never_pack hits and symlinks are rejected, and every allowed file must exist.
func (a *app) collect() ([]payloadFile, error) {
	dir, err := a.safeRel(a.policy.PrivateDir)
	if err != nil {
		return nil, err
	}
	var files []payloadFile
	err = fs.WalkDir(a.fsys.FS(), filepath.ToSlash(dir), func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return errMissing
		}
		rel := strings.TrimPrefix(p, a.policy.PrivateDir+"/")
		if p == a.policy.PrivateDir {
			return nil
		}
		if a.policy.neverPack(p) {
			return errNeverPack
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errPath
		}
		if err := a.checkEntry(rel); err != nil {
			return err
		}
		if len(files) >= maxFiles {
			return errLimit
		}
		b, err := a.readRel(p, maxFileSize)
		if err != nil {
			return errLimit
		}
		files = append(files, payloadFile{Path: rel, Data: b})
		return nil
	})
	if err == nil && len(files) != len(a.policy.Allowed) {
		err = errMissing
	}
	if err != nil {
		(&payload{Files: files}).wipe()
		return nil, err
	}
	slices.SortFunc(files, func(x, y payloadFile) int { return strings.Compare(x.Path, y.Path) })
	return files, nil
}

// restoreFiles validates every target first (no symlink, identical or absent), then
// writes the absent ones atomically without ever replacing an existing file.
func (a *app) restoreFiles(p *payload) (int, error) {
	var todo []int
	for i, f := range p.Files {
		target, err := a.safeRel(path.Join(a.policy.PrivateDir, f.Path))
		if err != nil {
			return 0, err
		}
		fi, err := a.fsys.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			todo = append(todo, i)
			continue
		}
		if err != nil || !fi.Mode().IsRegular() {
			return 0, errPath
		}
		cur, err := a.readRel(path.Join(a.policy.PrivateDir, f.Path), maxFileSize)
		if err != nil {
			return 0, errConflict
		}
		same := subtle.ConstantTimeCompare(cur, f.Data) == 1
		clear(cur)
		if !same {
			return 0, errConflict
		}
	}
	for _, i := range todo {
		if err := a.writePrivate(path.Join(a.policy.PrivateDir, p.Files[i].Path), p.Files[i].Data); err != nil {
			return 0, err
		}
	}
	return len(todo), nil
}

// writePrivate creates a 0600 file under 0700 directories via an O_EXCL .partial file
// and a hard link, so an existing target is never overwritten.
func (a *app) writePrivate(rel string, data []byte) error {
	target, err := a.safeRel(rel)
	if err != nil {
		return err
	}
	if err = a.mkPrivateDirs(filepath.Dir(target)); err != nil {
		return err
	}
	partial := target + ".partial"
	_ = a.fsys.Remove(partial)
	if err = a.writeFile(partial, data, 0o600); err != nil {
		_ = a.fsys.Remove(partial)
		return errWrite
	}
	defer func() { _ = a.fsys.Remove(partial) }()
	if errors.Is(a.fsys.Link(partial, target), os.ErrExist) {
		return errConflict
	}
	if fi, err := a.fsys.Lstat(target); err != nil || !fi.Mode().IsRegular() {
		return errWrite
	}
	return nil
}

func (a *app) mkPrivateDirs(dir string) error {
	base := filepath.FromSlash(a.policy.PrivateDir)
	if a.fsys.MkdirAll(dir, 0o700) != nil {
		return errWrite
	}
	for d := dir; d == base || strings.HasPrefix(d, base+string(filepath.Separator)); d = filepath.Dir(d) {
		if a.fsys.Chmod(d, 0o700) != nil {
			return errWrite
		}
	}
	return nil
}

// writePublic atomically replaces a non-secret file (bundle ciphertext, public
// manifest) through a .partial rename; it reports whether content changed.
func (a *app) writePublic(rel string, data []byte) (bool, error) {
	target, err := a.safeRel(rel)
	if err != nil {
		return false, err
	}
	if cur, err := a.readRel(rel, maxBundleSize); err == nil && bytes.Equal(cur, data) {
		return false, nil
	}
	if err = a.fsys.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return false, errWrite
	}
	partial := target + ".partial"
	_ = a.fsys.Remove(partial)
	if err = a.writeFile(partial, data, 0o644); err != nil {
		_ = a.fsys.Remove(partial)
		return false, errWrite
	}
	if a.fsys.Rename(partial, target) != nil {
		_ = a.fsys.Remove(partial)
		return false, errWrite
	}
	return true, nil
}

func (a *app) writeFile(name string, data []byte, mode os.FileMode) error {
	f, err := a.fsys.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
