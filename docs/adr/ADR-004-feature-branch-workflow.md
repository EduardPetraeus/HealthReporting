# ADR-004: Feature Branch Workflow + PR-Based Deployment

## Status
Accepted

## Date
2026-02-28

## Context
The platform uses GitHub Actions for CI/CD and Databricks Asset Bundles for deployment. We need a branching strategy that is safe, auditable, and compatible with automated deployment.

## Decision
All changes go through feature branches and PRs. Never commit directly to main.

Branch naming:
- `feature/` — new functionality
- `fix/` — bug fixes
- `docs/` — documentation only
- `refactor/` — restructuring without new functionality

Deployment gates:
- Feature branch push → auto-deploy to `dev` (bundle deploy --target dev)
- PR merge to main → auto-deploy to `prd`
- PR must pass: bundle validation + AI code review + governance check

## Consequences
- `main` is always production-ready
- Every change has a PR as audit trail
- Agents must always create feature branches — never commit to main
- After PR merge: `git checkout main && git pull && git branch -d <branch>`
- GitHub deletes remote head branch automatically after merge

## Alternatives Considered
- **Trunk-based development**: Faster, but higher risk. Not appropriate when PRs trigger automated deployments.
- **GitFlow (develop/release branches)**: Too complex for current team size (1 person + AI agents)
- **Direct commits to main**: No audit trail, no CI gate, incompatible with production deployment safety

## Agent Rule
Agents must never commit directly to main. If an agent finds itself on main, it must create a feature branch before making any changes. The `/commit-push-pr` command handles the full workflow.
