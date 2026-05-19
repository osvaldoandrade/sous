# Contributor resources

Sous is developed in the open at `github.com/osvaldoandrade/sous`.
The repository is the source of truth for code, history, releases, and tracking.

This page collects the practical resources a contributor needs.
It covers where issues live, how pull requests flow, how CI gates merges, how releases ship, and the conventions that keep the history readable.

## Where work lives

Issues are filed on GitHub.

The `roadmap` label marks committed work that maps to a row in `roadmap/README.md`.
The `enhancement` label marks proposals that are not yet on a roadmap track.
The `epic` label flags issues that group a body of work spanning several tasks.

New contributors who want to find something self-contained should start with the filter `is:issue label:roadmap is:open`.
That filter surfaces every task that has been planned, mirrored from the roadmap folder, and is still open.

Pull requests come from either forks or topic branches on the main repo.
Branch from `main` with a descriptive name.
The project uses verbal names like `impl/issue-74-75-downpayments`, not numbered prefixes.
Push the branch and open the PR through `gh pr create` or the GitHub UI.
PRs target `main`.

There is no separate develop branch.
`main` is always shippable.
The release workflow tags every push to `main` with a fresh semantic version.

## The contribution flow

The typical end-to-end flow for a non-trivial change has six steps.
Each step exists to keep the review surface small and the history bisectable.

### 1. Open an issue first

Any change that touches the public CLI, the JSON Schemas under `spec/`, the manifest contract, the runtime registry, the audit event shape, or the wire types under `internal/api` should start with a written-down problem statement.

Drive-by typo fixes and docs corrections do not need an issue.
If you are unsure, file one.
The cost of an issue is a single GitHub form, and it gives reviewers something to point at.

### 2. Branch from `main` with a descriptive name

The project does not require a prefix, but human-readable names help reviewers.

If the change implements a roadmap task, include the epic and task identifier.
For example, a branch for E3.04 might be called `impl/e3-04-parity-harness`.

### 3. Write tests next to the code you change

Sous uses Go's table-driven test style.

Unit tests live alongside the implementation (`runner_test.go` next to `runner.go`).
Contract tests for the lifecycle API live at `cmd/cs-control/lifecycle_contract_test.go`.
Parity fixtures live under `test/parity/fixtures/`.
Build-tagged integration tests live under `test/integration/`.

Every new code path should have at least one test that locks its observable behaviour.
Reviewers will ask "where is the test that fails before your change?" — be ready to point at it.

### 4. Run the local validation gates before pushing

The three canonical commands are:

- `make test` runs `go test ./...` over the whole module.
  The default timeout is generous.
  The suite is fast enough that a clean machine completes it in well under a minute.

- `make lint` runs `go vet ./...`.
  The project relies on `go vet` plus the standard library's formatter.
  There is no separate linter to install.

- `make build` builds every binary under `cmd/` into `bin/`.
  Use this to catch dead-code build errors that `go test` would otherwise mask.

Two additional targets are worth knowing about.

`make test-contract` re-runs only the lifecycle contract suite under `cmd/cs-control`.
It is the same step CI runs as a separate guardrail.

`make test-parity` runs the cross-runtime parity matrix in `internal/runtime/parity`.
A new runtime adapter, a new fixture, or a new host API change should be re-run against this target.

### 5. Open the pull request

Push the branch, then `gh pr create` (or use the GitHub UI).

The PR body should describe the change in prose, link the originating issue, and include a short test plan.
Keep PRs focused.
One logical change per PR makes review tractable and rollback surgical.

### 6. Request review and iterate

A maintainer will respond on the PR.
CI must pass before merge (see below).

Squash-merge is the default.
The commit message that lands on `main` is the PR title plus any body the maintainer keeps.
Avoid force-pushing once review starts.
Incremental commits make re-review fast.

## CI and required checks

CI is defined in `.github/workflows/ci.yml`.

The workflow runs on `push` to `main` and `master` and on every pull request.
Each run executes four steps in order.

1. `go test ./...` exercises the full module.
2. `make test-contract` re-runs the lifecycle CRUD contract subset under `cmd/cs-control`.
3. `go vet ./...` performs the static-analysis pass.
4. `go build ./cmd/...` compiles every service binary.

All four must pass before a PR is mergeable.

The workflow uses `go-version-file: go.mod` to pin the Go toolchain to whatever the module declares.
Bumping Go is therefore a `go.mod` edit, not a workflow edit.

Static analysis beyond `go vet` is intentionally absent.
The project favours a small required-checks surface and reviewer judgement over a long list of bot opinions.

Two adjacent workflows complete the automation picture.

`.github/workflows/static.yml` builds and deploys the docs site (this wiki) when the documentation tree changes.

`.github/workflows/release.yml` runs on every push to `main` and handles the release dance described below.

## Commit message conventions

The git log on `main` follows two styles, applied consistently across recent history.
Inspect the log to confirm before opening a PR.

### Roadmap-task commits

Roadmap-task commits use `{epic-id}.{task-id}: {short summary} (#PR)`.

Examples from recent history:

- `E3.01: cs-python subprocess MVP runtime adapter (#77)`
- `E8.01: DecisionTask MVP — schedule-activity workflows (#76)`
- `E5.02: ed25519 signed bundles with tenant keys (#72)`

The PR number suffix is appended automatically by GitHub's squash-merge.

### Out-of-band commits

Out-of-band changes use `{type}: {short summary} (#PR)`.

Examples:

- `docs: contributors`
- `hotfix: remove .claude/worktrees/ embedded git refs from main (#78)`
- `chore: bump go.mod to 1.22`

The leading verb (`docs`, `hotfix`, `chore`, `fix`, `refactor`) signals intent without imposing a full Conventional Commits grammar.

### Style rules

Subject lines stay imperative and under roughly 72 characters.

The body, when present, explains the why.
What changed is already in the diff.

Co-author trailers are added by tooling when an agent collaborates on the change.
Hand-edited co-author trailers should follow the same `Co-Authored-By: Name <email>` format.

## How releases ship

`.github/workflows/release.yml` runs on every push to `main`.

The workflow resolves the next semantic-version tag.
It reads the highest existing `vX.Y.Z` tag and increments the patch component.
It then re-runs the full test suite with that tag in scope.

Once tests pass, the workflow cross-compiles the `cs` CLI under `cmd/cs-cli` for `{linux, darwin, windows} × {amd64, arm64}`.
Each of the six artifacts is named `cs-{GOOS}-{GOARCH}` (with `.exe` on windows) and uploaded to a fresh GitHub Release.
Server-side binaries (`cs-control`, `cs-http-gateway`, and the other services) are built from source by the `sous-deploy` workflow into container images, not shipped as release artifacts.

Release notes are generated automatically from the merged-PR list since the last tag.
The release title is the version tag.

The npm wrapper under `npm/cs/` is published in the same workflow when an `NPM_TOKEN` secret is configured.
The wrapper version is rewritten to match the new tag via `npm version`.
`npm publish --access public` ships the package to the public registry as `@osvaldoandrade/cs`.

The published package is a thin postinstall shim.
The actual binary is downloaded from the GitHub Release artifacts on `npm install`.
If `NPM_TOKEN` is not set, the workflow logs a skip message and continues.

Contributors do not cut releases manually.
Merging to `main` is the release trigger.
The version bump and tag are deterministic from the previous tag and the merge time.

## Where the CLI ends up

Each release produces three distribution channels for the `cs` CLI.

GitHub Release artifacts — the cross-compiled `cs-{os}-{arch}` files described above.
This is the canonical download path for an operator who curls a binary onto a server.

`install.sh` — the one-line installer at the repo root.
It detects the host platform, downloads the matching binary from the latest GitHub Release, and drops it onto `PATH`.

`npm/cs/` — the npm wrapper.
Useful for Node-first development environments and CI pipelines that already have npm in the toolchain.

All three channels are kept in lockstep by the release workflow.
A given `cs` binary version is the same byte-for-byte regardless of how it was installed.

## Where to ask questions

Discussions on individual PRs and issues are the canonical record.

If a change spans several issues or needs design before implementation, open an `epic`-labelled issue and link the related tasks.
Roadmap planning happens through `roadmap/README.md` and the per-epic folders under `roadmap/`.
See [Contributors: What to Contribute](Contributors-What-to-Contribute) for a tour of entry points and good-first-contribution shapes.

For operational topics — running Sous locally, debugging activations, reproducing CI failures — the relevant deep-dives live in [Get Started](Get-Started), [Testing](Testing), and [Runbooks](Runbooks).

For the layout of the codebase itself, see [Contributors: Project Structure](Contributors-Project-Structure).
