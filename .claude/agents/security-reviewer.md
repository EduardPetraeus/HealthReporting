---
name: security-reviewer
description: Review code for security vulnerabilities, hardcoded secrets, and unsafe patterns. Use before merging PRs with new connectors, API integrations, or config changes.
---

# Security Reviewer

Review code for security issues specific to this platform.

## Checks

### Secrets and credentials
- No API keys, tokens, passwords, or OAuth secrets in code or config files
- No hardcoded Databricks host URLs or workspace IDs
- `.env` files are in `.gitignore` and never staged
- Oura OAuth tokens are read from environment variables only

### Data handling
- Raw health data files (`*.xml`, `*.parquet`, `*.csv`) are in `.gitignore`
- No PII logged to console or written to non-encrypted storage
- DuckDB files (`*.db`) are gitignored

### Code safety
- No `eval()`, `exec()`, or `subprocess` with unsanitized user input
- SQL queries use parameterized queries or safe string formatting — no f-string SQL with external input
- File paths validated before use — no path traversal (`../`) risk

### Dependencies
- No `pip install` of unverified packages in scripts
- `requirements.txt` pins exact versions

## Output format
- `[CRITICAL]` — must fix before merge
- `[WARN]` — should fix, explain if not
- `[NOTE]` — informational, no action required
- End with: SECURE / REVIEW NEEDED
