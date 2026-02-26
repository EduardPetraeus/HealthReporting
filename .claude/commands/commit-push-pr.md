Commit, push, and open a PR for the current branch.

1. Run `git status` and `git diff --stat` to review changes
2. Stage relevant files (never `.env`, never `*.db`)
3. Write a concise commit message in imperative form
4. Commit with Co-Authored-By trailer
5. Push to remote
6. Open PR with `gh pr create` — short title, bullet summary, test plan
7. Run `.venv/bin/python update_productivity.py`
