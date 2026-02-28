# COST_LOG.md — AI Usage & Cost Tracking

> Layer 4: Observability — cost visibility across sessions
> Updated manually at end of each session (or automatically via tooling).
> Purpose: ensure AI costs remain proportional to value delivered.

---

## Monthly Summary

| Month | Sessions | Total tokens (est.) | Est. cost | Notes |
|-------|----------|---------------------|-----------|-------|
| Feb 2026 | 5 | ~500,000 | ~$3.50 | Claude Sonnet 4.5/4.6 + Haiku for PR review |

---

## Session Log

| Date | Session | Model | Task type | Est. input tokens | Est. output tokens | Est. cost | Verdict |
|------|---------|-------|-----------|-------------------|--------------------|-----------|---------|
| 2026-02-28 | 001 | claude-sonnet-4-6 | Architecture + bronze setup | 80,000 | 25,000 | $0.65 | High value |
| 2026-02-28 | 002 | claude-sonnet-4-6 | Silver transforms (9 entities) | 120,000 | 40,000 | $0.98 | High value |
| 2026-02-28 | 003 | claude-sonnet-4-6 | Audit logging framework | 90,000 | 30,000 | $0.74 | High value |
| 2026-02-28 | 004 | claude-sonnet-4-6 | AI Governance Framework v1 | 60,000 | 20,000 | $0.49 | High value |
| 2026-02-28 | 005 | claude-sonnet-4-6 | AI Governance enforcement layer | 150,000 | 50,000 | $1.23 | High value |

> **Note:** Token counts are estimates based on context window usage and output length.
> Pricing reference: Sonnet 4.6 — $3/M input, $15/M output. Haiku 4.5 — $0.25/M input, $1.25/M output.

---

## PR Review Costs (AI gate, per PR)

| Date | PR | Model | Input tokens | Output tokens | Est. cost |
|------|----|-------|-------------|--------------|-----------|
| 2026-02-28 | #37 | claude-haiku-4-5-20251001 | ~8,000 | ~600 | $0.003 |

> PR review uses Claude Haiku — cost per review is negligible (<$0.01).
> Enable by adding `ANTHROPIC_API_KEY` to GitHub Secrets.

---

## Cost Guardrails

| Guardrail | Threshold | Action |
|-----------|-----------|--------|
| Monthly AI cost | > $20 | Review and optimize prompt sizes |
| Single session | > $2 | Split into smaller sessions |
| PR review cost | > $0.05 per PR | Switch to lighter model or smaller diff |

---

## Notes

- `claude_code_productivity.json` tracks commit-level metrics (not token-level)
- Token estimates based on typical context window usage — not measured precisely
- Future: instrument `ai_pr_review.py` to log actual token usage from API response

<!-- smoke test 2026-02-28 — validates ANTHROPIC_API_KEY + ai-review workflow -->
