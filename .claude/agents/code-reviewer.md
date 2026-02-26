---
name: code-reviewer
description: Review code or a PR against project conventions and medallion architecture standards. Use when asked to review, check, or audit code changes.
---

# Code Reviewer

Review the specified code or diff against these standards:

## Conventions
- snake_case on all names (tables, columns, variables, files)
- English only — variable names, comments, docstrings, print statements
- Bronze tables prefixed `stg_`, always include `_ingested_at` and `source_system`
- Silver uses MERGE INTO with `_ingested_at` watermark — never INSERT OVERWRITE
- Gold is views only — no raw data, always includes `date`, `user_id`, `source_system`
- No hardcoded paths, user IDs, credentials, or env values in code
- Individual files over monolithic scripts

## Quality checks
1. Does the code follow all conventions above?
2. Are there hardcoded values that should be in YAML config?
3. Is there unnecessary complexity or premature abstraction?
4. Are there missing error handlers at system boundaries (file I/O, API calls)?
5. Are there any security concerns (secrets in code, SQL injection risk)?

## PoC mindset
6. Is there a comment noting the enterprise-grade scale-up path where a simpler approach was chosen?
7. Is the code multi-tenant ready (`source_system` isolation, `user_id` dimension)?

## Output format
- List issues as: `[CRITICAL|WARN|NOTE] file:line — description`
- End with a summary: APPROVED / APPROVED WITH NOTES / CHANGES REQUESTED
