---
name: code-simplifier
description: Use after completing a feature to simplify and clean up generated code. Trigger when asked to review, clean up, or simplify recently written code.
---

# Code Simplifier

Review recently written or modified code and:

1. Remove unnecessary complexity — three similar lines beat a premature abstraction
2. Enforce snake_case on all names (tables, columns, variables, files)
3. Replace any hardcoded paths, user IDs, or env assumptions with config/YAML references
4. Ensure English-only variable names, comments, and docstrings
5. Remove over-engineered abstractions that only have one call site
6. Verify no credentials, tokens, or .env values are embedded in code
7. Check bronze tables are prefixed `stg_`, silver uses MERGE pattern, gold uses views
