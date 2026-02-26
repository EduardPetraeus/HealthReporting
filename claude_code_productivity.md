# Claude Code Productivity Tracker

Methodology and context for `claude_code_productivity.json`.

## What We Measure

### Metric 1 — Files per commit (scope proxy)
How many files does a typical commit touch?

- **Baseline (pre-Claude):** 10.0 avg files/commit
- **Claude Code era:** 16.7 avg files/commit
- **Factor: 1.67x**

Interpretation: Claude Code touches more files per commit — indicating broader, more coordinated changes per task.

### Metric 2 — Commits per active week (velocity)
How many commits happen in a week where development is actually taking place?

- **Baseline (pre-Claude):** 4.1 commits/active week (37 commits over 9 active weeks)
- **Claude Code era:** 29.0 commits/active week (29 commits over 1 active week)
- **Factor: 7x**

Interpretation: With Claude Code, development is continuous during active weeks. Before Claude Code, it was sporadic — resulting in fewer, larger sessions.

## Why Active Weeks (Not Calendar Weeks)

Using calendar weeks would penalize periods of inactivity unrelated to productivity (holidays, other projects, day job). Active weeks — weeks with at least one commit — give a fair comparison of what happens *when* you sit down to work.

## Baseline Period

`2024-02-02` → `2026-02-15` (solo development, no Claude Code)

- 37 commits over 106 calendar weeks
- Only 9 of those weeks had any activity
- Large initial commits (bulk imports) slightly inflate the avg files metric

## Claude Code Start Date

`2026-02-16`

## Task Types

| Type | Description |
|------|-------------|
| `new_feature` | New functionality added |
| `fix` | Bug or data quality fix |
| `refactor` | Restructuring without new functionality |
| `config` | Configuration, environment, tooling |
| `docs` | Documentation and markdown files |
| `setup` | One-time project scaffolding |
| `archive` | Moving legacy code — excluded from factor calculations |

## What Else Can Be Tracked

The following metrics can be added to entries over time:

| Metric | How | What it shows |
|--------|-----|---------------|
| `refactor_ratio` | lines_removed / (lines_added + lines_removed) | How much cleanup accompanies new code |
| `code_density` | lines_added / files_changed | How substantial each file change is |
| `feature_ratio` | new_feature commits / total commits | How proactive vs reactive development is |
| `session_depth` | max files in a single commit | Breadth of coordinated thinking per session |
| `streak_days` | consecutive active days | Momentum indicator |
| `products_shipped` | manual | Business output per period |
| `claude_api_cost_usd` | manual / API logs | Cost per commit or per session |
| `time_to_first_commit` | manual (minutes) | How quickly you go from idea to code |
| `bug_regression` | manual flag | Did you have to fix something you just built? |

## Limitations

- Claude Code era has only 1 active week of data — factors will stabilize over time
- Files-per-commit factor is influenced by task type (setup/refactor commits inflate numbers)
- Time estimates are not tracked — velocity is the primary proxy
- Does not capture code quality beyond scope (files/lines)

## How to Update

Add a new entry to `claude_code_productivity.json` after each meaningful commit:

```json
{
  "commit_sha": "abc1234",
  "date": "YYYY-MM-DD",
  "message": "commit message",
  "task_type": "new_feature",
  "files_changed": 8,
  "lines_added": 240,
  "lines_removed": 10,
  "files_factor": 0.8
}
```

Recalculate `claude_code_summary` and `factors` periodically as the dataset grows.
