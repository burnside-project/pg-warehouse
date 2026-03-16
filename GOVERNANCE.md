# Governance

This document describes the governance model for pg-warehouse.

## Maintainers

pg-warehouse is maintained by the Burnside Project team. Current maintainers are listed in the repository's GitHub team settings and have merge access to the `main` branch.

## Decision-Making

- **Architecture decisions** are documented as ADRs in [`docs/adr/`](docs/adr/). Significant changes to the system's structure, dependencies, or boundaries require a new ADR that is reviewed and accepted before implementation.
- **Feature requests and bugs** are tracked as GitHub Issues. Discussion happens in the issue before work begins.
- **Pull requests** require at least one maintainer approval. CI must pass before merging.

## Release Process

- pg-warehouse follows [Semantic Versioning](https://semver.org/).
- Each release includes an updated `CHANGELOG.md` entry describing additions, changes, and fixes.
- Releases are built and published via GoReleaser, producing platform-specific binaries.
- Release candidates may be published for testing before a stable release.

## Becoming a Maintainer

Contributors who demonstrate sustained, high-quality contributions may be invited to become maintainers. The criteria include:

- Meaningful contributions over multiple months (code, documentation, reviews, issue triage).
- Understanding of the hexagonal architecture and open-core boundary.
- Alignment with the project's design principles and community standards.

Existing maintainers propose and approve new maintainers by consensus.

## Code of Conduct

All participants are expected to follow the project's Code of Conduct. Maintainers are responsible for enforcement.
