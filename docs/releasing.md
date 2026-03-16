# Releasing pg-warehouse

## Versioning

pg-warehouse follows [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`.

| Bump  | When                                                        | Example         |
|-------|-------------------------------------------------------------|-----------------|
| MAJOR | Breaking changes to CLI flags, config format, or public API | `v2.0.0`        |
| MINOR | New commands, features, or non-breaking enhancements        | `v1.1.0`        |
| PATCH | Bug fixes, performance improvements, doc corrections        | `v1.0.1`        |

Pre-release versions use suffixes: `-rc.1`, `-beta.1`, `-alpha.1`.

## Version Injection

The version is injected at build time via `-ldflags`:

```
-X github.com/burnside-project/pg-warehouse/pkg/version.Version={{.Version}}
```

During development the version defaults to `dev`. Tagged builds receive the Git tag value automatically through GoReleaser.

## Release Steps

1. **Update CHANGELOG.md** — move entries under `Unreleased` to a new version heading with the release date.

2. **Create an annotated tag:**
   ```bash
   git tag -a v1.0.0 -m "Release v1.0.0"
   ```

3. **Push the tag:**
   ```bash
   git push origin v1.0.0
   ```

4. **GoReleaser runs automatically** via the GitHub Actions `release` workflow. It builds binaries for linux/darwin (amd64/arm64), publishes a GitHub Release, and updates the Homebrew tap at `burnside-project/homebrew-tap`.

5. **Verify the Homebrew tap** was updated:
   ```bash
   brew update && brew info burnside-project/tap/pg-warehouse
   ```

6. **Edit the GitHub Release** to add polished release notes (see template below).

## Dry Run

Test the release pipeline locally without publishing:

```bash
make release-dry-run
```

## Release Notes Template

```markdown
## Highlights

- Brief summary of the most important changes.

## Breaking Changes

- Description of any breaking change and migration steps.

## Migration Guide

If applicable, describe the steps users need to take to upgrade.

## What's Changed

- Feature A (#PR)
- Bug fix B (#PR)

## Contributors

Thanks to @contributor for their work on this release.

**Full Changelog**: https://github.com/burnside-project/pg-warehouse/compare/vPREV...vCURRENT
```
