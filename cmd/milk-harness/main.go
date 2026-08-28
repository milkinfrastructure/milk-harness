package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	codexVersion       = "0.150.1"
	pinnedCodexSHA256  = "abf1bb1643a79f73aa78ee627e111e02d4f8c98f25813a0cf6ce277709664386"
	maxStaticBytes     = int64(1 << 20)
	maxEventLineBytes  = 4 << 20
	maxAllowedArtifact = int64(64 << 20)
	gitTimeout         = 15 * time.Second
)

var (
	errSizeLimit = errors.New("size limit exceeded")
	hexID        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	gitObjectID  = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	jobName      = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
)

type config struct {
	worktree      string
	runsDir       string
	promptPath    string
	schemaPath    string
	codexPath     string
	runID         string
	baseCommit    string
	timeout       time.Duration
	maxEvents     int64
	maxStderr     int64
	maxDiff       int64
	allowedJobs   []string
	apiKey        string
	codexSHA256   string
	gitExecutable string
}

type repeatedFlag []string

func (f *repeatedFlag) String() string { return strings.Join(*f, ",") }
func (f *repeatedFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type runMeta struct {
	Schema               string   `json:"schema"`
	RunID                string   `json:"run_id"`
	BaseCommit           string   `json:"base_commit"`
	BaseTree             string   `json:"base_tree"`
	PromptSHA256         string   `json:"prompt_sha256"`
	DecisionSchemaSHA256 string   `json:"decision_schema_sha256"`
	CodexVersion         string   `json:"codex_version"`
	CodexSHA256          string   `json:"codex_sha256"`
	TimeoutSeconds       int64    `json:"timeout_seconds"`
	MaxEventsBytes       int64    `json:"max_events_bytes"`
	MaxStderrBytes       int64    `json:"max_stderr_bytes"`
	MaxDiffBytes         int64    `json:"max_diff_bytes"`
	AllowedJobs          []string `json:"allowed_jobs"`
	ChildEnvironmentKeys []string `json:"child_environment_keys"`
	Command              []string `json:"command"`
}

type decision struct {
	Hypothesis   string   `json:"hypothesis"`
	ChangedFiles []string `json:"changed_files"`
	LocalChecks  []string `json:"local_checks"`
	RequestedJob *string  `json:"requested_job"`
	StopReason   string   `json:"stop_reason"`
}

type artifactDigest struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type receipt struct {
	Schema       string           `json:"schema"`
	RunID        string           `json:"run_id"`
	BaseCommit   string           `json:"base_commit"`
	BaseTree     string           `json:"base_tree"`
	CodexVersion string           `json:"codex_version"`
	CodexSHA256  string           `json:"codex_sha256"`
	Outcome      string           `json:"outcome"`
	ExitCode     int              `json:"exit_code"`
	TimedOut     bool             `json:"timed_out"`
	Error        string           `json:"error,omitempty"`
	RequestedJob *string          `json:"requested_job"`
	Artifacts    []artifactDigest `json:"artifacts"`
}

type processResult struct {
	exitCode int
	timedOut bool
	err      error
}

type eventEnvelope struct {
	Type     string `json:"type"`
	ThreadID string `json:"thread_id,omitempty"`
	Item     *struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"item,omitempty"`
}

func main() {
	if err := rejectCredentialEnvironment(os.Environ()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, err := execute(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rejectCredentialEnvironment(environ []string) error {
	for _, entry := range environ {
		name := strings.ToUpper(strings.SplitN(entry, "=", 2)[0])
		if name == "CODEX_API_KEY" {
			continue
		}
		secretName := strings.Contains(name, "API_KEY") || strings.Contains(name, "SECRET") || strings.Contains(name, "TOKEN") || strings.Contains(name, "PASSWORD") || strings.Contains(name, "CREDENTIAL") || strings.HasSuffix(name, "_KEY")
		privilegedHandle := name == "SSH_AUTH_SOCK" || name == "GIT_ASKPASS" || name == "KUBECONFIG" || name == "DOCKER_HOST"
		if secretName || privilegedHandle {
			return fmt.Errorf("planner environment contains forbidden credential variable %s", name)
		}
	}
	return nil
}

func parseConfig(args []string, getenv func(string) string) (config, error) {
	var jobs repeatedFlag
	cfg := config{codexSHA256: pinnedCodexSHA256, gitExecutable: "git"}
	fs := flag.NewFlagSet("milk-harness", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.worktree, "worktree", "/worktree", "absolute clean Git checkout")
	fs.StringVar(&cfg.runsDir, "runs-dir", "/runs", "absolute artifact directory")
	fs.StringVar(&cfg.promptPath, "prompt", "/opt/milk/iterate.txt", "absolute prompt path")
	fs.StringVar(&cfg.schemaPath, "schema", "/opt/milk/decision.schema.json", "absolute output schema path")
	fs.StringVar(&cfg.codexPath, "codex", "/usr/local/bin/codex", "absolute pinned Codex path")
	fs.StringVar(&cfg.runID, "run-id", "", "64-character lowercase hexadecimal run ID")
	fs.StringVar(&cfg.baseCommit, "base-commit", "", "expected Git HEAD")
	fs.DurationVar(&cfg.timeout, "timeout", 30*time.Minute, "single Codex process deadline")
	fs.Int64Var(&cfg.maxEvents, "max-events-bytes", 32<<20, "events.jsonl byte limit")
	fs.Int64Var(&cfg.maxStderr, "max-stderr-bytes", 8<<20, "stderr.txt byte limit")
	fs.Int64Var(&cfg.maxDiff, "max-diff-bytes", 32<<20, "candidate.diff byte limit")
	fs.Var(&jobs, "allow-job", "permitted requested_job name; repeatable")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, errors.New("positional arguments are forbidden")
	}
	cfg.allowedJobs = append([]string(nil), jobs...)
	cfg.apiKey = getenv("CODEX_API_KEY")
	if err := validateConfig(&cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg *config) error {
	if !hexID.MatchString(cfg.runID) {
		return errors.New("run-id must be exactly 64 lowercase hexadecimal characters")
	}
	if !gitObjectID.MatchString(cfg.baseCommit) {
		return errors.New("base-commit must be a 40- or 64-character lowercase Git object ID")
	}
	if cfg.timeout < time.Second || cfg.timeout > time.Hour || cfg.timeout%time.Second != 0 {
		return errors.New("timeout must be an integral number of seconds from 1 through 3600")
	}
	for name, value := range map[string]int64{
		"max-events-bytes": cfg.maxEvents,
		"max-stderr-bytes": cfg.maxStderr,
		"max-diff-bytes":   cfg.maxDiff,
	} {
		if value <= 0 || value > maxAllowedArtifact {
			return fmt.Errorf("%s must be greater than zero and at most %d", name, maxAllowedArtifact)
		}
	}
	if cfg.apiKey == "" || len(cfg.apiKey) > 16<<10 || strings.IndexByte(cfg.apiKey, 0) >= 0 {
		return errors.New("CODEX_API_KEY must be nonempty and at most 16384 bytes")
	}
	for _, value := range []struct {
		name string
		path string
	}{
		{"worktree", cfg.worktree},
		{"runs-dir", cfg.runsDir},
		{"prompt", cfg.promptPath},
		{"schema", cfg.schemaPath},
		{"codex", cfg.codexPath},
	} {
		if !filepath.IsAbs(value.path) {
			return fmt.Errorf("%s must be absolute", value.name)
		}
		if filepath.Clean(value.path) != value.path {
			return fmt.Errorf("%s must be canonical", value.name)
		}
		if err := rejectSymlinkComponents(value.path); err != nil {
			return fmt.Errorf("%s: %w", value.name, err)
		}
	}
	if withinPath(cfg.runsDir, cfg.worktree) || withinPath(cfg.worktree, cfg.runsDir) {
		return errors.New("runs-dir and worktree must not contain one another")
	}
	if err := requireDirectory(cfg.worktree); err != nil {
		return fmt.Errorf("worktree: %w", err)
	}
	if err := requireDirectory(cfg.runsDir); err != nil {
		return fmt.Errorf("runs-dir: %w", err)
	}
	for name, value := range map[string]string{
		"prompt": cfg.promptPath,
		"schema": cfg.schemaPath,
		"codex":  cfg.codexPath,
	} {
		if err := requireRegular(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	seen := make(map[string]struct{}, len(cfg.allowedJobs))
	if len(cfg.allowedJobs) > 64 {
		return errors.New("at most 64 allow-job values are permitted")
	}
	for _, job := range cfg.allowedJobs {
		if !jobName.MatchString(job) {
			return fmt.Errorf("invalid allow-job %q", job)
		}
		if _, exists := seen[job]; exists {
			return fmt.Errorf("duplicate allow-job %q", job)
		}
		seen[job] = struct{}{}
	}
	sort.Strings(cfg.allowedJobs)
	return nil
}

func execute(cfg config) (receipt, error) {
	prompt, err := readBounded(cfg.promptPath, maxStaticBytes)
	if err != nil {
		return receipt{}, fmt.Errorf("read prompt: %w", err)
	}
	prompt = bindAllowedJobs(prompt, cfg.allowedJobs)
	if int64(len(prompt)) > maxStaticBytes {
		return receipt{}, errors.New("bound prompt exceeds 1 MiB")
	}
	schema, err := readBounded(cfg.schemaPath, maxStaticBytes)
	if err != nil {
		return receipt{}, fmt.Errorf("read schema: %w", err)
	}
	if !json.Valid(schema) {
		return receipt{}, errors.New("decision schema is not valid JSON")
	}
	actualCodexSHA, err := fileSHA256(cfg.codexPath)
	if err != nil {
		return receipt{}, fmt.Errorf("hash Codex: %w", err)
	}
	if actualCodexSHA != cfg.codexSHA256 {
		return receipt{}, fmt.Errorf("Codex SHA-256 mismatch: got %s", actualCodexSHA)
	}
	baseTree, err := preflight(cfg)
	if err != nil {
		return receipt{}, err
	}

	runDir := filepath.Join(cfg.runsDir, cfg.runID)
	if err := os.Mkdir(runDir, 0o700); err != nil {
		return receipt{}, fmt.Errorf("create immutable run directory: %w", err)
	}
	if err := writeExclusive(filepath.Join(runDir, "prompt.txt"), prompt); err != nil {
		return receipt{}, err
	}
	if err := writeExclusive(filepath.Join(runDir, "decision.schema.json"), schema); err != nil {
		return receipt{}, err
	}

	args := codexArgs(cfg.worktree, filepath.Join(runDir, "decision.schema.json"))
	meta := runMeta{
		Schema:               "milk.planner-run-meta.v1",
		RunID:                cfg.runID,
		BaseCommit:           cfg.baseCommit,
		BaseTree:             baseTree,
		PromptSHA256:         sumBytes(prompt),
		DecisionSchemaSHA256: sumBytes(schema),
		CodexVersion:         codexVersion,
		CodexSHA256:          cfg.codexSHA256,
		TimeoutSeconds:       int64(cfg.timeout / time.Second),
		MaxEventsBytes:       cfg.maxEvents,
		MaxStderrBytes:       cfg.maxStderr,
		MaxDiffBytes:         cfg.maxDiff,
		AllowedJobs:          append([]string(nil), cfg.allowedJobs...),
		ChildEnvironmentKeys: []string{"PATH", "HOME", "TMPDIR", "LANG", "CODEX_HOME", "CODEX_API_KEY"},
		Command:              append([]string{cfg.codexPath}, args...),
	}
	if err := writeExclusiveJSON(filepath.Join(runDir, "meta.json"), meta); err != nil {
		return receipt{}, err
	}

	process := runCodex(cfg, runDir, args, prompt)
	postErr := verifyPostflight(cfg)
	var diff []byte
	var changedFiles, untracked []string
	var changedErr, diffErr error
	if postErr == nil {
		changedFiles, untracked, changedErr = changedPaths(cfg)
		if changedErr == nil {
			diff, diffErr = candidateDiff(cfg, untracked)
		}
	}
	if err := writeExclusive(filepath.Join(runDir, "candidate.diff"), diff); err != nil {
		return receipt{}, err
	}

	var finalDecision decision
	eventDecision, eventErr := validateEvents(filepath.Join(runDir, "events.jsonl"))
	decisionErr := eventErr
	if decisionErr == nil {
		finalDecision, decisionErr = validateDecision(eventDecision, cfg.allowedJobs, changedFiles)
	}
	outcome := "completed"
	var runErr error
	switch {
	case process.err != nil:
		outcome, runErr = "ambiguous", process.err
	case eventErr != nil:
		outcome, runErr = "ambiguous", eventErr
	case postErr != nil:
		outcome, runErr = "rejected", postErr
	case changedErr != nil:
		outcome, runErr = "rejected", changedErr
	case diffErr != nil:
		outcome, runErr = "rejected", diffErr
	case decisionErr != nil:
		outcome, runErr = "rejected", decisionErr
	}
	if outcome == "completed" {
		if err := writeExclusiveJSON(filepath.Join(runDir, "decision.json"), finalDecision); err != nil {
			return receipt{}, err
		}
	}

	artifactNames := []string{
		"candidate.diff",
		"decision.schema.json",
		"events.jsonl",
		"meta.json",
		"prompt.txt",
		"stderr.txt",
	}
	if outcome == "completed" {
		artifactNames = append(artifactNames, "decision.json")
	}
	var requestedJob *string
	if outcome == "completed" {
		requestedJob = finalDecision.RequestedJob
	}
	r, err := buildReceipt(runDir, receipt{
		Schema:       "milk.planner-run-receipt.v1",
		RunID:        cfg.runID,
		BaseCommit:   cfg.baseCommit,
		BaseTree:     baseTree,
		CodexVersion: codexVersion,
		CodexSHA256:  cfg.codexSHA256,
		Outcome:      outcome,
		ExitCode:     process.exitCode,
		TimedOut:     process.timedOut,
		RequestedJob: requestedJob,
	}, artifactNames)
	if err != nil {
		return receipt{}, err
	}
	if runErr != nil {
		r.Error = runErr.Error()
	}
	if err := writeExclusiveJSON(filepath.Join(runDir, "receipt.json"), r); err != nil {
		return receipt{}, err
	}
	if err := syncDirectory(runDir); err != nil {
		return receipt{}, err
	}
	if runErr != nil {
		return r, runErr
	}
	return r, nil
}

func codexArgs(worktree, schemaPath string) []string {
	return []string{
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
		"--output-schema", schemaPath,
		"-C", worktree,
		"-",
	}
}

func bindAllowedJobs(prompt []byte, allowed []string) []byte {
	bound := append([]byte(nil), prompt...)
	if len(bound) > 0 && bound[len(bound)-1] != '\n' {
		bound = append(bound, '\n')
	}
	bound = append(bound, '\n')
	if len(allowed) == 0 {
		return append(bound, "For this run, requested_job must be null.\n"...)
	}
	bound = append(bound, "For this run, requested_job may be null or exactly one of: "...)
	for index, name := range allowed {
		if index > 0 {
			bound = append(bound, ", "...)
		}
		encoded, _ := json.Marshal(name)
		bound = append(bound, encoded...)
	}
	return append(bound, ".\n"...)
}

func preflight(cfg config) (string, error) {
	if err := scanNoSymlinks(cfg.worktree); err != nil {
		return "", err
	}
	top, err := gitOutput(cfg, maxStaticBytes, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(top)) != cfg.worktree {
		return "", errors.New("worktree must be the canonical Git root")
	}
	head, err := gitOutput(cfg, maxStaticBytes, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(head)) != cfg.baseCommit {
		return "", errors.New("base commit mismatch")
	}
	tree, err := gitOutput(cfg, maxStaticBytes, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return "", err
	}
	baseTree := strings.TrimSpace(string(tree))
	if !gitObjectID.MatchString(baseTree) {
		return "", errors.New("Git returned an invalid tree ID")
	}
	status, err := gitOutput(cfg, maxStaticBytes, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return "", err
	}
	if len(status) != 0 {
		return "", errors.New("worktree must be clean, including untracked files")
	}
	return baseTree, nil
}

func verifyPostflight(cfg config) error {
	head, err := gitOutput(cfg, maxStaticBytes, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head)) != cfg.baseCommit {
		return errors.New("Codex changed Git HEAD")
	}
	return scanNoSymlinks(cfg.worktree)
}

func changedPaths(cfg config) ([]string, []string, error) {
	trackedRaw, err := gitOutput(cfg, cfg.maxDiff, "diff", "--name-only", "-z", "--no-renames", "--no-ext-diff", "--no-textconv", "HEAD", "--")
	if err != nil {
		return nil, nil, err
	}
	untrackedRaw, err := gitOutput(cfg, cfg.maxDiff, "ls-files", "-z", "--others", "--exclude-standard")
	if err != nil {
		return nil, nil, err
	}
	tracked, err := parseNULPaths(trackedRaw)
	if err != nil {
		return nil, nil, err
	}
	untracked, err := parseNULPaths(untrackedRaw)
	if err != nil {
		return nil, nil, err
	}
	all := append(append([]string(nil), tracked...), untracked...)
	sort.Strings(all)
	all = uniqueStrings(all)
	sort.Strings(untracked)
	return all, uniqueStrings(untracked), nil
}

func candidateDiff(cfg config, untracked []string) ([]byte, error) {
	tracked, err := gitOutput(cfg, cfg.maxDiff, "diff", "--binary", "--no-renames", "--no-ext-diff", "--no-textconv", "HEAD", "--")
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if _, err := out.Write(tracked); err != nil {
		return nil, err
	}
	for _, name := range untracked {
		remaining := cfg.maxDiff - int64(out.Len())
		if remaining <= 0 {
			return nil, errors.New("candidate diff exceeds limit")
		}
		chunk, exitCode, err := gitOutputExit(cfg, remaining, "diff", "--no-index", "--binary", "--no-ext-diff", "--no-textconv", "--", "/dev/null", "./"+name)
		if err != nil && exitCode != 1 {
			return nil, err
		}
		if int64(len(chunk))+int64(out.Len()) > cfg.maxDiff {
			return nil, errors.New("candidate diff exceeds limit")
		}
		if _, err := out.Write(chunk); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func runCodex(cfg config, runDir string, args []string, prompt []byte) processResult {
	events, err := openExclusive(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		return processResult{exitCode: -1, err: err}
	}
	stderr, err := openExclusive(filepath.Join(runDir, "stderr.txt"))
	if err != nil {
		events.Close()
		return processResult{exitCode: -1, err: err}
	}
	defer events.Close()
	defer stderr.Close()

	tempRoot, err := os.MkdirTemp("", "milk-planner-")
	if err != nil {
		return processResult{exitCode: -1, err: err}
	}
	defer os.RemoveAll(tempRoot)
	for _, name := range []string{"home", "tmp", "codex"} {
		if err := os.Mkdir(filepath.Join(tempRoot, name), 0o700); err != nil {
			return processResult{exitCode: -1, err: err}
		}
	}

	cmd := exec.Command(cfg.codexPath, args...)
	cmd.Dir = cfg.worktree
	cmd.Env = childEnvironment(tempRoot, cfg.apiKey)
	cmd.Stdin = bytes.NewReader(prompt)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return processResult{exitCode: -1, err: err}
	}
	defer stdout.Close()
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return processResult{exitCode: -1, err: err}
	}
	defer stderrPipe.Close()
	if err := cmd.Start(); err != nil {
		return processResult{exitCode: -1, err: fmt.Errorf("start Codex: %w", err)}
	}
	limitHit := make(chan error, 2)
	copyDone := make(chan error, 2)
	for _, stream := range []struct {
		dst   io.Writer
		src   io.Reader
		limit int64
		name  string
	}{{events, stdout, cfg.maxEvents, "events"}, {stderr, stderrPipe, cfg.maxStderr, "stderr"}} {
		go func() {
			err := copyLimited(stream.dst, stream.src, stream.limit)
			if errors.Is(err, errSizeLimit) {
				select {
				case limitHit <- fmt.Errorf("Codex %s exceeded its byte limit; run is ambiguous and cannot be retried", stream.name):
				default:
				}
			}
			copyDone <- err
		}()
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(cfg.timeout)
	defer timer.Stop()

	var waitErr error
	result := processResult{exitCode: -1}
	select {
	case waitErr = <-done:
	case limitErr := <-limitHit:
		killProcessGroup(cmd.Process.Pid)
		waitErr = <-done
		result.err = limitErr
	case <-timer.C:
		killProcessGroup(cmd.Process.Pid)
		waitErr = <-done
		result.timedOut = true
		result.err = errors.New("Codex timed out; run is ambiguous and cannot be retried")
	}
	for range 2 {
		if copyErr := <-copyDone; copyErr != nil && result.err == nil {
			if errors.Is(copyErr, errSizeLimit) {
				result.err = errors.New("Codex output exceeded its byte limit; run is ambiguous and cannot be retried")
			} else {
				result.err = fmt.Errorf("capture Codex output: %w", copyErr)
			}
		}
	}
	if err := events.Sync(); err != nil && result.err == nil {
		result.err = err
	}
	if err := stderr.Sync(); err != nil && result.err == nil {
		result.err = err
	}
	result.exitCode = processExitCode(waitErr)
	if waitErr != nil && result.err == nil {
		result.err = fmt.Errorf("Codex exited unsuccessfully: %w", waitErr)
	}
	return result
}

func childEnvironment(tempRoot, apiKey string) []string {
	return []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=" + filepath.Join(tempRoot, "home"),
		"TMPDIR=" + filepath.Join(tempRoot, "tmp"),
		"LANG=C.UTF-8",
		"CODEX_HOME=" + filepath.Join(tempRoot, "codex"),
		"CODEX_API_KEY=" + apiKey,
	}
}

func validateEvents(eventsPath string) (string, error) {
	f, err := os.Open(eventsPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() == 0 {
		return "", errors.New("Codex emitted no JSONL events")
	}
	last := []byte{0}
	if _, err := f.ReadAt(last, info.Size()-1); err != nil {
		return "", err
	}
	if last[0] != '\n' {
		return "", errors.New("Codex JSONL is truncated: final newline missing")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), maxEventLineBytes)
	line := 0
	threadStarted := false
	turnStarted := false
	terminal := ""
	sawError := false
	lastAgentMessage := ""
	for scanner.Scan() {
		line++
		if len(scanner.Bytes()) == 0 {
			return "", fmt.Errorf("empty JSONL record at line %d", line)
		}
		if terminal != "" {
			return "", fmt.Errorf("event after terminal record at line %d", line)
		}
		var event eventEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil || event.Type == "" {
			return "", fmt.Errorf("malformed JSONL record at line %d", line)
		}
		switch event.Type {
		case "thread.started":
			if line != 1 || threadStarted || event.ThreadID == "" {
				return "", errors.New("invalid thread.started event")
			}
			threadStarted = true
		case "turn.started":
			if !threadStarted || turnStarted {
				return "", errors.New("invalid turn.started event")
			}
			turnStarted = true
		case "item.completed":
			if !turnStarted || event.Item == nil {
				return "", errors.New("invalid item.completed event")
			}
			if event.Item.Type == "agent_message" {
				lastAgentMessage = event.Item.Text
			}
		case "turn.completed", "turn.failed":
			if !turnStarted {
				return "", errors.New("terminal event before turn.started")
			}
			terminal = event.Type
		case "error":
			sawError = true
		default:
			if !threadStarted {
				return "", errors.New("event before thread.started")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read Codex JSONL: %w", err)
	}
	if terminal != "turn.completed" || sawError {
		return "", errors.New("Codex JSONL has no successful terminal event")
	}
	if lastAgentMessage == "" {
		return "", errors.New("Codex JSONL has no final agent message")
	}
	return lastAgentMessage, nil
}

func validateDecision(raw string, allowedJobs, actualChanged []string) (decision, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var d decision
	if err := decoder.Decode(&d); err != nil {
		return decision{}, fmt.Errorf("decode decision: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return decision{}, err
	}
	if d.Hypothesis == "" || len(d.Hypothesis) > 4096 || d.StopReason == "" || len(d.StopReason) > 4096 {
		return decision{}, errors.New("decision hypothesis and stop_reason must be 1..4096 bytes")
	}
	if len(d.ChangedFiles) > 256 || len(d.LocalChecks) > 256 {
		return decision{}, errors.New("decision arrays exceed 256 entries")
	}
	for _, check := range d.LocalChecks {
		if len(check) > 4096 {
			return decision{}, errors.New("local_checks entry exceeds 4096 bytes")
		}
	}
	for _, name := range d.ChangedFiles {
		if err := validateRepoPath(name); err != nil {
			return decision{}, fmt.Errorf("changed_files: %w", err)
		}
	}
	d.ChangedFiles = uniqueSorted(d.ChangedFiles)
	if d.ChangedFiles == nil {
		d.ChangedFiles = []string{}
	}
	actual := uniqueSorted(actualChanged)
	if !equalStrings(d.ChangedFiles, actual) {
		return decision{}, fmt.Errorf("decision changed_files does not match Git: decision=%v git=%v", d.ChangedFiles, actual)
	}
	if d.RequestedJob != nil {
		if !jobName.MatchString(*d.RequestedJob) {
			return decision{}, errors.New("requested_job is not a canonical job name")
		}
		index := sort.SearchStrings(allowedJobs, *d.RequestedJob)
		if index == len(allowedJobs) || allowedJobs[index] != *d.RequestedJob {
			return decision{}, fmt.Errorf("requested_job %q is not allowlisted", *d.RequestedJob)
		}
	}
	return d, nil
}

func buildReceipt(runDir string, r receipt, names []string) (receipt, error) {
	sort.Strings(names)
	for _, name := range names {
		filePath := filepath.Join(runDir, name)
		info, err := os.Stat(filePath)
		if err != nil {
			return receipt{}, err
		}
		digest, err := fileSHA256(filePath)
		if err != nil {
			return receipt{}, err
		}
		r.Artifacts = append(r.Artifacts, artifactDigest{Path: name, Bytes: info.Size(), SHA256: digest})
	}
	return r, nil
}

func copyLimited(dst io.Writer, src io.Reader, limit int64) error {
	buffer := make([]byte, 32<<10)
	var written int64
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			remaining := limit - written
			if remaining <= 0 {
				return errSizeLimit
			}
			write := buffer[:n]
			if int64(n) > remaining {
				write = write[:remaining]
			}
			count, err := dst.Write(write)
			written += int64(count)
			if err != nil {
				return err
			}
			if count != n {
				return errSizeLimit
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func gitOutput(cfg config, limit int64, args ...string) ([]byte, error) {
	out, _, err := gitOutputExit(cfg, limit, args...)
	return out, err
}

func gitOutputExit(cfg config, limit int64, args ...string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()
	gitArgs := append([]string{"-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false"}, args...)
	cmd := exec.CommandContext(ctx, cfg.gitExecutable, gitArgs...)
	cmd.Dir = cfg.worktree
	cmd.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=/nonexistent",
		"LANG=C.UTF-8",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_TERMINAL_PROMPT=0",
	}
	var stdout, stderr limitedBuffer
	stdout.limit = limit
	stderr.limit = maxStaticBytes
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := processExitCode(err)
	if ctx.Err() != nil {
		return nil, exitCode, errors.New("Git command timed out")
	}
	if errors.Is(stdout.err, errSizeLimit) || errors.Is(stderr.err, errSizeLimit) {
		return nil, exitCode, errors.New("Git output exceeded limit")
	}
	if err != nil {
		return stdout.buf.Bytes(), exitCode, fmt.Errorf("git %s failed: %w: %s", args[0], err, strings.TrimSpace(stderr.buf.String()))
	}
	return stdout.buf.Bytes(), exitCode, nil
}

type limitedBuffer struct {
	buf   bytes.Buffer
	limit int64
	err   error
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	remaining := b.limit - int64(b.buf.Len())
	if remaining <= 0 {
		b.err = errSizeLimit
		return 0, b.err
	}
	write := p
	if int64(len(write)) > remaining {
		write = write[:remaining]
	}
	n, err := b.buf.Write(write)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		b.err = errSizeLimit
		return n, b.err
	}
	return n, nil
}

func rejectSymlinkComponents(filePath string) error {
	clean := filepath.Clean(filePath)
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink component forbidden: %s", current)
		}
	}
	return nil
}

func scanNoSymlinks(root string) error {
	return filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filePath != root && entry.Name() == ".git" && entry.IsDir() {
			return filepath.SkipDir
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("worktree symlink forbidden: %s", filePath)
		}
		return nil
	})
}

func withinPath(candidate, root string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func requireDirectory(filePath string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("not a directory")
	}
	return nil
}

func requireRegular(filePath string) error {
	info, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	return nil
}

func readBounded(filePath string, limit int64) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errSizeLimit
	}
	return data, nil
}

func openExclusive(filePath string) (*os.File, error) {
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

func writeExclusive(filePath string, data []byte) error {
	f, err := openExclusive(filePath)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func writeExclusiveJSON(filePath string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeExclusive(filePath, data)
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func fileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func sumBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func killProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	if group, err := syscall.Getpgid(pid); err == nil {
		_ = syscall.Kill(-group, syscall.SIGKILL)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

func processExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func parseNULPaths(raw []byte) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if raw[len(raw)-1] != 0 {
		return nil, errors.New("truncated NUL-delimited Git path list")
	}
	parts := bytes.Split(raw[:len(raw)-1], []byte{0})
	paths := make([]string, 0, len(parts))
	for _, rawPath := range parts {
		name := string(rawPath)
		if err := validateRepoPath(name); err != nil {
			return nil, err
		}
		paths = append(paths, name)
	}
	return paths, nil
}

func validateRepoPath(name string) error {
	if name == "" || len(name) > 4096 || strings.Contains(name, `\`) || strings.IndexByte(name, 0) >= 0 || path.IsAbs(name) || path.Clean(name) != name || name == "." || name == ".." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("non-canonical repository path %q", name)
	}
	return nil
}

func uniqueSorted(values []string) []string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return uniqueStrings(copyValues)
}

func uniqueStrings(sortedValues []string) []string {
	if len(sortedValues) == 0 {
		return nil
	}
	out := sortedValues[:1]
	for _, value := range sortedValues[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("decision contains trailing JSON")
	}
	return nil
}
