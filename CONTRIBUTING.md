# Contributing to pg-warehouse

Thank you for your interest in contributing to pg-warehouse!

## How to Contribute

### Reporting Issues

- Use [GitHub Issues](https://github.com/burnside-project/pg-warehouse/issues) to report bugs or suggest features
- Search existing issues before creating a new one
- Include steps to reproduce for bug reports

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests: `make test`
5. Run linting: `make lint`
6. Commit with a clear message
7. Push and open a Pull Request

### Development Setup

```bash
# Clone
git clone https://github.com/burnside-project/pg-warehouse.git
cd pg-warehouse

# Install dev dependencies
./scripts/dev.sh

# Build
make build

# Test
make test

# Lint
make lint
```

### Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Keep functions small and focused
- Write tests for new functionality
- Use the hexagonal architecture patterns established in the codebase

### Commit Messages

Use clear, descriptive commit messages:
- `feat: add S3 export adapter`
- `fix: handle nil watermark in incremental sync`
- `docs: update CDC quickstart guide`
- `test: add integration test for full sync workflow`

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
