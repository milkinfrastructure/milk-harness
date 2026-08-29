package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCodexArgsExact(t *testing.T) {
	got := codexArgs("/worktree", "/runs/schema.json")
	want := []string{
		"--ask-for-approval", "never",
		"--sandbox", "workspace-write",
		"--strict-config",
		"-c", `shell_environment_policy.inherit="core"`,
		"-c", "shell_environment_policy.ignore_default_excludes=false",
		"exec",
		"--json",
		"--ephemeral",
		"--ignore-user-config",
		"--ignore-rules",
		"--output-schema", "/runs/schema.json",
		"-C", "/worktree",
		"-",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	joined := strings.Join(got, " ")
	for _, forbidden := range []string{"--search", "--dangerously-bypass-approvals-and-sandbox", "--full-auto"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("forbidden flag %q", forbidden)
		}
	}
}

func TestChildEnvironmentIsAllowlisted(t *testing.T) {
	t.Setenv("MILK_SECRET_CANARY", "must-not-pass")
	env := childEnvironment("/tmp/run", "test-key")
	wantKeys := []string{"PATH", "HOME", "TMPDIR", "LANG", "CODEX_HOME", "CODEX_API_KEY"}
	var gotKeys []string
	for _, entry := range env {
		gotKeys = append(gotKeys, strings.SplitN(entry, "=", 2)[0])
		if strings.Contains(entry, "must-not-pass") {
			t.Fatal("parent secret leaked into child environment")
		}
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("environment keys mismatch: got %v want %v", gotKeys, wantKeys)
	}
}

func TestParentCredentialEnvironmentFailsClosed(t *testing.T) {
	if err := rejectCredentialEnvironment([]string{"PATH=/bin", "CODEX_API_KEY=allowed"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "SSH_AUTH_SOCK", "KUBECONFIG"} {
		if err := rejectCredentialEnvironment([]string{name + "=canary"}); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestPromptBindsOnlyAllowlistedJobs(t *testing.T) {
	got := string(bindAllowedJobs([]byte("base\n"), []string{"offline-eval", "staged-proof"}))
	if !strings.Contains(got, `"offline-eval", "staged-proof"`) || strings.Contains(got, "provider argv") {
		t.Fatalf("unexpected bound prompt: %q", got)
	}
	if got := string(bindAllowedJobs([]byte("base"), nil)); !strings.HasSuffix(got, "requested_job must be null.\n") {
		t.Fatalf("null-only prompt not bound: %q", got)
	}
}

func TestValidateEventsFailsClosed(t *testing.T) {
	validDecision := `{"hypothesis":"small","changed_files":[],"local_checks":[],"requested_job":null,"stop_reason":"done"}`
	valid := eventStream(validDecision)
	tests := map[string]string{
		"valid":              valid,
		"malformed":          "{\n",
		"truncated":          strings.TrimSuffix(valid, "\n"),
		"duplicate terminal": valid + `{"type":"turn.completed"}` + "\n",
		"failed terminal": strings.Replace(valid,
			`{"type":"turn.completed"}`, `{"type":"turn.failed"}`, 1),
	}
	for name, content := range tests {
		t.Run(name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "events.jsonl")
			mustWrite(t, file, []byte(content), 0o600)
			_, err := validateEvents(file)
			if name == "valid" && err != nil {
				t.Fatalf("valid events rejected: %v", err)
			}
			if name != "valid" && err == nil {
				t.Fatal("invalid events accepted")
			}
		})
	}
}

func TestExecuteStreamsArtifactsAndDropsParentSecrets(t *testing.T) {
	fixture := newFixture(t, eventStream(`{"hypothesis":"small","changed_files":[],"local_checks":["go test"],"requested_job":null,"stop_reason":"done"}`))
	t.Setenv("MILK_SECRET_CANARY", "must-not-pass")
	fixture.cfg.apiKey = "test-key"
	r, err := execute(fixture.cfg)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if r.Outcome != "completed" || r.ExitCode != 0 || r.TimedOut {
		t.Fatalf("unexpected receipt: %+v", r)
	}
	runDir := filepath.Join(fixture.cfg.runsDir, fixture.cfg.runID)
	for _, name := range []string{"candidate.diff", "decision.json", "decision.schema.json", "events.jsonl", "meta.json", "prompt.txt", "receipt.json", "stderr.txt"} {
		if _, err := os.Stat(filepath.Join(runDir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	assertTreeDoesNotContain(t, runDir, "must-not-pass")
	assertTreeDoesNotContain(t, runDir, "test-key")
}

func TestExecuteBindsTrackedAndUntrackedPatch(t *testing.T) {
	decision := `{"hypothesis":"small","changed_files":["new.txt","tracked.txt"],"local_checks":[],"requested_job":null,"stop_reason":"done"}`
	script := "#!/bin/sh\nprintf 'changed\\n' > tracked.txt\nprintf 'new\\n' > new.txt\nprintf '%s' " + shellQuote(eventStream(decision)) + "\n"
	fixture := newFixtureAt(t, realTempDir(t), script)
	r, err := execute(fixture.cfg)
	if err != nil || r.Outcome != "completed" {
		t.Fatalf("execute: receipt=%+v err=%v", r, err)
	}
	patch, err := os.ReadFile(filepath.Join(fixture.cfg.runsDir, fixture.cfg.runID, "candidate.diff"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tracked.txt", "new.txt"} {
		if !strings.Contains(string(patch), name) {
			t.Fatalf("candidate patch does not bind %s:\n%s", name, patch)
		}
	}
}

func TestPreflightRejectsDirtySymlinkAndBaseMismatch(t *testing.T) {
	t.Run("dirty", func(t *testing.T) {
		fixture := newFixture(t, eventStream(nullDecision()))
		mustWrite(t, filepath.Join(fixture.cfg.worktree, "untracked"), []byte("x"), 0o600)
		if _, err := execute(fixture.cfg); err == nil || !strings.Contains(err.Error(), "clean") {
			t.Fatalf("dirty checkout not rejected: %v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		fixture := newFixture(t, eventStream(nullDecision()))
		if err := os.Symlink("tracked.txt", filepath.Join(fixture.cfg.worktree, "link")); err != nil {
			t.Fatal(err)
		}
		if _, err := execute(fixture.cfg); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("symlink not rejected: %v", err)
		}
	})
	t.Run("base mismatch", func(t *testing.T) {
		fixture := newFixture(t, eventStream(nullDecision()))
		fixture.cfg.baseCommit = strings.Repeat("0", 40)
		if _, err := execute(fixture.cfg); err == nil || !strings.Contains(err.Error(), "base commit mismatch") {
			t.Fatalf("base mismatch not rejected: %v", err)
		}
	})
	t.Run("created symlink", func(t *testing.T) {
		decision := `{"hypothesis":"small","changed_files":["link"],"local_checks":[],"requested_job":null,"stop_reason":"done"}`
		script := "#!/bin/sh\nln -s /etc/passwd link\nprintf '%s' " + shellQuote(eventStream(decision)) + "\n"
		fixture := newFixtureAt(t, realTempDir(t), script)
		r, err := execute(fixture.cfg)
		if err == nil || r.Outcome != "rejected" || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("created symlink not rejected: receipt=%+v err=%v", r, err)
		}
		patch, readErr := os.ReadFile(filepath.Join(fixture.cfg.runsDir, fixture.cfg.runID, "candidate.diff"))
		if readErr != nil || len(patch) != 0 {
			t.Fatalf("rejected symlink was read into patch: %q err=%v", patch, readErr)
		}
	})
}

func TestOutputLimitStopsOnceAndWritesAmbiguousReceipt(t *testing.T) {
	root := realTempDir(t)
	counter := filepath.Join(root, "starts")
	large := strings.Repeat("x", 4096)
	script := "#!/bin/sh\nprintf x >> " + shellQuote(counter) + "\nprintf '%s' " + shellQuote(large) + "\nsleep 30\n"
	fixture := newFixtureAt(t, root, script)
	fixture.cfg.maxEvents = 128
	r, err := execute(fixture.cfg)
	if err == nil {
		t.Fatal("output overflow unexpectedly succeeded")
	}
	if r.Outcome != "ambiguous" {
		t.Fatalf("outcome = %q", r.Outcome)
	}
	starts, readErr := os.ReadFile(counter)
	if readErr != nil || string(starts) != "x" {
		t.Fatalf("process was retried: starts=%q err=%v", starts, readErr)
	}
	info, statErr := os.Stat(filepath.Join(fixture.cfg.runsDir, fixture.cfg.runID, "events.jsonl"))
	if statErr != nil || info.Size() > fixture.cfg.maxEvents {
		t.Fatalf("events cap violated: size=%d err=%v", info.Size(), statErr)
	}
}

func TestCandidateDiffLimitRejectsCompletedTurn(t *testing.T) {
	decision := `{"hypothesis":"small","changed_files":["tracked.txt"],"local_checks":[],"requested_job":null,"stop_reason":"done"}`
	script := "#!/bin/sh\nprintf '%04096d' 0 > tracked.txt\nprintf '%s' " + shellQuote(eventStream(decision)) + "\n"
	fixture := newFixtureAt(t, realTempDir(t), script)
	fixture.cfg.maxDiff = 128
	r, err := execute(fixture.cfg)
	if err == nil || r.Outcome != "rejected" || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversized patch not rejected: receipt=%+v err=%v", r, err)
	}
}

func TestTimeoutKillsProcessGroupWithoutRetry(t *testing.T) {
	root := realTempDir(t)
	counter := filepath.Join(root, "starts")
	childMarker := filepath.Join(root, "child-finished")
	script := fmt.Sprintf("#!/bin/sh\nprintf x >> %s\n(sleep 1; printf child > %s) &\nsleep 30\n", shellQuote(counter), shellQuote(childMarker))
	fixture := newFixtureAt(t, root, script)
	fixture.cfg.timeout = 300 * time.Millisecond
	r, err := execute(fixture.cfg)
	if err == nil || !r.TimedOut || r.Outcome != "ambiguous" {
		t.Fatalf("timeout did not fail closed: receipt=%+v err=%v", r, err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(childMarker); !os.IsNotExist(err) {
		t.Fatalf("descendant survived process-group kill: %v", err)
	}
	starts, readErr := os.ReadFile(counter)
	if readErr != nil || string(starts) != "x" {
		t.Fatalf("process was retried: starts=%q err=%v", starts, readErr)
	}
}

func TestDecisionRejectsPathTraversalAndJobOverride(t *testing.T) {
	if hexID.MatchString("../run") || jobName.MatchString("../job") {
		t.Fatal("path traversal passed an identifier validator")
	}
	tests := []struct {
		name    string
		changed []string
		job     string
	}{
		{name: "parent path", changed: []string{"../secret"}},
		{name: "absolute path", changed: []string{"/secret"}},
		{name: "job path", job: "safe/job"},
		{name: "unknown job", job: "other"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := "null"
			if test.job != "" {
				encoded, _ := json.Marshal(test.job)
				job = string(encoded)
			}
			changed, _ := json.Marshal(test.changed)
			raw := fmt.Sprintf(`{"hypothesis":"small","changed_files":%s,"local_checks":[],"requested_job":%s,"stop_reason":"done"}`, changed, job)
			if _, err := validateDecision(raw, []string{"safe"}, nil); err == nil {
				t.Fatal("invalid decision accepted")
			}
		})
	}
	requested := "safe"
	raw := `{"hypothesis":"small","changed_files":[],"local_checks":[],"requested_job":"safe","stop_reason":"done"}`
	d, err := validateDecision(raw, []string{"safe"}, nil)
	if err != nil || d.RequestedJob == nil || *d.RequestedJob != requested {
		t.Fatalf("allowlisted job rejected: decision=%+v err=%v", d, err)
	}
}

func TestReceiptDeterminism(t *testing.T) {
	root := realTempDir(t)
	left := filepath.Join(root, "left")
	right := filepath.Join(root, "right")
	for _, dir := range []string{left, right} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, filepath.Join(dir, "b"), []byte("two"), 0o600)
		mustWrite(t, filepath.Join(dir, "a"), []byte("one"), 0o600)
	}
	base := receipt{Schema: "milk.planner-run-receipt.v1", RunID: strings.Repeat("a", 64), BaseCommit: strings.Repeat("b", 40), BaseTree: strings.Repeat("c", 40), CodexVersion: codexVersion, CodexSHA256: pinnedCodexSHA256, Outcome: "completed"}
	one, err := buildReceipt(left, base, []string{"b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	two, err := buildReceipt(right, base, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	oneJSON, _ := json.Marshal(one)
	twoJSON, _ := json.Marshal(two)
	if string(oneJSON) != string(twoJSON) {
		t.Fatalf("receipt is path- or order-dependent:\n%s\n%s", oneJSON, twoJSON)
	}
}

func TestDockerfilePinsCodexAndContainsNoProviderSurface(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile.planner"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(dockerfile)
	for _, required := range []string{
		codexVersion,
		"docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e",
		"golang:1.24.3-alpine3.22@sha256:b4f875e650466fa0fe62c6fd3f02517a392123eea85f1d7e69d85f780e4db1c1",
		"alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce",
		"ab308870bc7fc048c23dc49d03f6b8af9ce7fc99b9da882d6688be7a90155c7a",
		pinnedCodexSHA256,
		"apk add --no-cache --no-network /packages/*.apk",
		"USER 65532:65532",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("Dockerfile missing %q", required)
		}
	}
	for _, forbidden := range []string{"ghcr.io", "baseten", "modal", "cloudflare", "route-sign"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("Dockerfile contains provider surface %q", forbidden)
		}
	}
	ignore, err := os.ReadFile(filepath.Join(root, "Dockerfile.planner.dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(ignore), "**\n") || strings.Contains(string(ignore), "!deploy") || strings.Contains(text, "COPY . ") {
		t.Fatal("planner build context is not deny-by-default")
	}
	packages := map[string]string{
		"brotli-libs-1.1.0-r2.apk":        "a693524543421b3a90f163ccb48d5ad0f5fd773b5c3b640acc461eace2cb01b6",
		"bubblewrap-0.11.0-r1.apk":        "8fd45a340640da2a374e2238063a9b14c157c20ed033282cb71037e72972ea71",
		"c-ares-1.34.8-r0.apk":            "1397ec9682ff6153e5d037965c76408e570ae6535ce479cc2af37436fdea52ce",
		"libexpat-2.8.3-r0.apk":           "128338718fadfaaf9f926fe349347edce7645c8ffd1ae2878ca3d807b23fa5c3",
		"git-2.49.1-r0.apk":               "42f4573799ffce0c7dd4954d2247eb882eb87360dc18637930624484ecfd1c90",
		"git-init-template-2.49.1-r0.apk": "b8d9dc864aa8c68e93d1cc80d069d56f1ebbed9e8bd90a5fd9a6f2d183258fcc",
		"libcap2-2.78-r0.apk":             "9850759bbb16f1ff6d1a49dc99947ef1401e1c10e5ca24f8380e69ba19f077c9",
		"libcurl-8.14.1-r3.apk":           "249e5a3a50558cdb3ce1bab1619b15d88243a628d878700c8d05878c2e71ed71",
		"libidn2-2.3.7-r0.apk":            "515cfe061176c7456ea3548651d0014084d77dd3774b78e2c944404ae75a41ff",
		"libpsl-0.21.5-r3.apk":            "1e79da4fa1364b7153b8950a363a6de0bef1d1b48d46fcb3b850cd4f233727b2",
		"libunistring-1.3-r0.apk":         "989640e58f7646c0495c3950df36eb7b4df152d8150c2c888f8ab954fed8c908",
		"nghttp2-libs-1.69.0-r0.apk":      "d6ee515d30d703e94b55ef9c0a02aea053313f654acbd2c655d18e3907ecb66a",
		"pcre2-10.46-r0.apk":              "cfb8ad103a101fa6a31769e50e188dab9c60124705682d01b3de268795db58ad",
		"zstd-libs-1.5.7-r0.apk":          "1bdd6e57cfbfbfd6e8481cad37ddd5d199950715bec1879b3afb600272dbb09e",
	}
	if strings.Count(text, "ADD --checksum=sha256:") != len(packages) {
		t.Fatalf("Dockerfile package closure has %d remote APKs, want %d", strings.Count(text, "ADD --checksum=sha256:"), len(packages))
	}
	if strings.Count(text, "apk add ") != 1 || strings.Contains(text, "apk update") || strings.Contains(text, "apk upgrade") {
		t.Fatal("Dockerfile runtime package installation is not a single offline exact closure")
	}
	for name, digest := range packages {
		if !strings.Contains(text, "--checksum=sha256:"+digest) || strings.Count(text, name) != 2 {
			t.Fatalf("runtime package is not exact and single-source: %s", name)
		}
	}
	wantLicenses := map[string]string{
		"LICENSE": "d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc",
		"NOTICE":  "9d71575ecfd9a843fc1677b0efb08053c6ba9fd686a0de1a6f5382fd3c220915",
	}
	for name, want := range wantLicenses {
		got, err := fileSHA256(filepath.Join(root, "third_party", "openai-codex", name))
		if err != nil || got != want {
			t.Fatalf("%s digest = %s, want %s, err=%v", name, got, want, err)
		}
	}
}

type fixture struct {
	cfg config
}

func newFixture(t *testing.T, output string) fixture {
	return newFixtureAt(t, realTempDir(t), shellScript(output))
}

func newFixtureAt(t *testing.T, root, script string) fixture {
	t.Helper()
	worktree := filepath.Join(root, "worktree")
	runs := filepath.Join(root, "runs")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(runs, 0o700); err != nil {
		t.Fatal(err)
	}
	run(t, worktree, "git", "init", "-q")
	run(t, worktree, "git", "config", "user.name", "Milk Test")
	run(t, worktree, "git", "config", "user.email", "milk@example.invalid")
	mustWrite(t, filepath.Join(worktree, "tracked.txt"), []byte("base\n"), 0o600)
	run(t, worktree, "git", "add", "tracked.txt")
	run(t, worktree, "git", "commit", "-q", "-m", "base")
	base := strings.TrimSpace(run(t, worktree, "git", "rev-parse", "HEAD"))
	prompt := filepath.Join(root, "prompt.txt")
	schema := filepath.Join(root, "schema.json")
	codex := filepath.Join(root, "codex")
	mustWrite(t, prompt, []byte("bounded prompt\n"), 0o600)
	mustWrite(t, schema, []byte("{}\n"), 0o600)
	mustWrite(t, codex, []byte(script), 0o700)
	digest, err := fileSHA256(codex)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{cfg: config{
		worktree:      worktree,
		runsDir:       runs,
		promptPath:    prompt,
		schemaPath:    schema,
		codexPath:     codex,
		runID:         strings.Repeat("a", 64),
		baseCommit:    base,
		timeout:       5 * time.Second,
		maxEvents:     1 << 20,
		maxStderr:     1 << 20,
		maxDiff:       1 << 20,
		apiKey:        "test-key",
		codexSHA256:   digest,
		gitExecutable: "git",
	}}
}

func eventStream(decisionJSON string) string {
	lines := []any{
		map[string]any{"type": "thread.started", "thread_id": "thread-1"},
		map[string]any{"type": "turn.started"},
		map[string]any{"type": "item.completed", "item": map[string]any{"type": "agent_message", "text": decisionJSON}},
		map[string]any{"type": "turn.completed"},
	}
	var output strings.Builder
	for _, line := range lines {
		encoded, _ := json.Marshal(line)
		output.Write(encoded)
		output.WriteByte('\n')
	}
	return output.String()
}

func nullDecision() string {
	return `{"hypothesis":"small","changed_files":[],"local_checks":[],"requested_job":null,"stop_reason":"done"}`
}

func shellScript(output string) string {
	return "#!/bin/sh\nif [ \"${MILK_SECRET_CANARY+x}\" = x ]; then exit 88; fi\nprintf '%s' " + shellQuote(output) + "\n"
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func realTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func run(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v: %s", name, args, err, output)
	}
	return string(output)
}

func mustWrite(t *testing.T, file string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(file, data, mode); err != nil {
		t.Fatal(err)
	}
}

func assertTreeDoesNotContain(t *testing.T, root, needle string) {
	t.Helper()
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(data), needle) {
			return fmt.Errorf("%s contains secret canary", filePath)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPinnedSHAConstantIsHex(t *testing.T) {
	decoded, err := hex.DecodeString(pinnedCodexSHA256)
	if err != nil || len(decoded) != sha256.Size {
		t.Fatalf("invalid pinned SHA-256: %v", err)
	}
}
