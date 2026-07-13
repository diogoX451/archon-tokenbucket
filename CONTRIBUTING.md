# Contributing to archon-tokenbucket

Part of the **Archon open-source toolkit** by [diogoX451](https://github.com/diogoX451).

## Scope

Keep PRs focused on this package. Out of scope: multi-tenant SaaS product
features (billing, CRM, vertical domain logic).

## Workflow

1. Open an issue (unless trivial).
2. Branch from `main`: `feat/...` or `fix/...`.
3. `go test ./...` and `go vet ./...` must pass.
4. PR describes **why**, **what**, and **test plan**.

## API stability

Go module semver. Breaking changes require `/v2`. Prefer dual-read for wire formats.

## Branding

Credit **Archon open-source toolkit** and link this repository.

## License

Contributions are accepted under Apache-2.0.
