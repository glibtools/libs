// Command secrets-bundle manages a project's age-encrypted secrets bundle (ADR-017):
// plaintext lives under the Git-ignored private directory, ciphertext in the bundle
// file committed to Git. Commands: prepare, restore, pack, verify, rekey, backup.
//
// Output and log lines only carry fixed status words and fixed error codes: never
// secret material, public keys, hashes, paths of secrets or raw library errors.
// Secrets are never accepted as CLI arguments; the age identity is read from
// SECRETS_KEY (default ~/.ssh/ait.pem) inside the tool only.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type failure string

func (f failure) Error() string { return string(f) }

const (
	errUsage           failure = "usage"
	errPolicy          failure = "policy_invalid"
	errGitignore       failure = "private_not_gitignored"
	errLog             failure = "log_unavailable"
	errIdentity        failure = "identity_unavailable"
	errIdentityLocked  failure = "identity_passphrase_unsupported"
	errRecipient       failure = "recipient_invalid"
	errRecoveryMissing failure = "recovery_missing_not_declared"
	errNoBundle        failure = "bundle_missing"
	errBundleRead      failure = "bundle_unreadable"
	errBundleDecrypt   failure = "bundle_decrypt_failed"
	errPayload         failure = "payload_invalid"
	errPath            failure = "path_rejected"
	errNeverPack       failure = "never_pack_hit"
	errNotAllowed      failure = "file_not_allowed"
	errMissing         failure = "file_missing"
	errLimit           failure = "limit_exceeded"
	errSecret          failure = "secret_invalid"
	errConflict        failure = "existing_file_differs"
	errWrite           failure = "write_failed"
	errMismatch        failure = "content_mismatch"
	errRecipientsStale failure = "recipients_outdated"
	errPublicMismatch  failure = "public_manifest_mismatch"
	errPerms           failure = "permissions_invalid"
	errPlaintext       failure = "bundle_plaintext_marker"
	errReleaseGate     failure = "release_gate_open"
	errNetwork         failure = "network_not_confirmed"
	errHost            failure = "backup_target_invalid"
	errBackup          failure = "backup_failed"
)

// runCommand executes a structured argv without a shell; tests replace it.
var runCommand = func(argv []string) error {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	return cmd.Run()
}

type app struct {
	root         string
	fsys         *os.Root
	policy       *policy
	identityPath string
	out          io.Writer
	log          io.Writer
	cmd          string
	release      bool
	newPrimary   string
	host         string
	dest         string
	allowNetwork bool
}

func main() { os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr)) }

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("secrets-bundle", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	root := fs.String("project-root", "", "project root directory (required)")
	pol := fs.String("policy", "", "policy JSON relative to project root (required)")
	logPath := fs.String("log", "", "log file (default <project-root>/tmp/secrets-bundle.log)")
	release := fs.Bool("release", false, "verify: fail unless every release gate is closed")
	newPrimary := fs.String("new-primary", "", "rekey: SSH public key file of the new primary recipient")
	host := fs.String("host", "", "backup: SSH host alias")
	dest := fs.String("dest", "", "backup: remote directory")
	allowNetwork := fs.Bool("allow-network", false, "backup: explicit confirmation to copy ciphertext over the network")
	if err := fs.Parse(args); err != nil || fs.NArg() != 1 || *root == "" || *pol == "" {
		_, _ = fmt.Fprintln(stderr, "error="+string(errUsage))
		return 2
	}
	a := &app{out: stdout, cmd: fs.Arg(0), release: *release, newPrimary: *newPrimary,
		host: *host, dest: *dest, allowNetwork: *allowNetwork}
	err := a.init(*root, *pol, *logPath, getenv)
	if err == nil {
		err = a.dispatch()
	}
	if err == nil {
		a.logf("result=ok")
		return 0
	}
	code := errUsage
	if f, ok := err.(failure); ok {
		code = f
	}
	a.logf("result=error code=" + string(code))
	_, _ = fmt.Fprintln(stderr, "error="+string(code))
	return 1
}

func (a *app) init(root, pol, logPath string, getenv func(string) string) error {
	abs, err := filepath.Abs(root)
	if err != nil {
		return errUsage
	}
	if a.root, err = filepath.EvalSymlinks(abs); err != nil {
		return errUsage
	}
	if logPath == "" {
		logPath = filepath.Join(a.root, "tmp", "secrets-bundle.log")
	}
	if err = a.openLog(logPath); err != nil {
		return err
	}
	if a.fsys, err = os.OpenRoot(a.root); err != nil {
		return errUsage
	}
	if a.policy, err = a.loadPolicy(pol); err != nil {
		return err
	}
	a.identityPath = getenv("SECRETS_KEY")
	if a.identityPath != "" {
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return errIdentity
	}
	a.identityPath = filepath.Join(home, ".ssh", "ait.pem")
	return nil
}

func (a *app) dispatch() error {
	switch a.cmd {
	case "prepare":
		return a.prepare()
	case "restore":
		return a.restore()
	case "pack":
		return a.packCmd()
	case "verify":
		return a.verify()
	case "rekey":
		return a.rekey()
	case "backup":
		return a.backup()
	}
	return errUsage
}

func (a *app) openLog(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return errLog
	}
	if fi, err := os.Lstat(path); err == nil && !fi.Mode().IsRegular() {
		return errLog
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return errLog
	}
	if err = f.Chmod(0o600); err != nil {
		_ = f.Close()
		return errLog
	}
	a.log = f
	return nil
}

// logf appends one line; callers only pass fixed words and fixed codes.
func (a *app) logf(event string) {
	if a.log == nil {
		return
	}
	_, _ = fmt.Fprintf(a.log, "%s cmd=%s %s\n", time.Now().UTC().Format(time.RFC3339), safeWord(a.cmd), event)
}

// status prints a fixed status line to stdout and mirrors it to the log.
func (a *app) status(line string) {
	_, _ = fmt.Fprintln(a.out, line)
	a.logf("status=" + line)
}

func safeWord(s string) string {
	switch s {
	case "prepare", "restore", "pack", "verify", "rekey", "backup":
		return s
	}
	return "unknown"
}
