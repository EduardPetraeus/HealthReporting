Run the security-reviewer agent on all changes in the current branch.

Use the security-reviewer agent to check:
1. Run `git diff main...HEAD --name-only` to get changed files
2. Read each changed file and apply all checks from the security-reviewer agent
3. Output findings using [CRITICAL] / [WARN] / [NOTE] format
4. End with SECURE or REVIEW NEEDED verdict
