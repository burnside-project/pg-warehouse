# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in pg-warehouse, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, email: [security@burnsideproject.ai](mailto:security@burnsideproject.ai)

We will acknowledge receipt within 48 hours and provide an estimated timeline for a fix.

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest  | Yes       |

## Security Best Practices

When using pg-warehouse:

- Never commit PostgreSQL credentials in config files
- Use environment variables or secrets management for connection URLs
- Restrict PostgreSQL user permissions to minimum required (SELECT for sync, REPLICATION for CDC)
- Keep pg-warehouse updated to the latest version
