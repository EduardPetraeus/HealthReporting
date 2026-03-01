# /prioritize — Merged Priority Ranking

You are acting as a strategic prioritization assistant. Follow these steps exactly.

## Step 1: Read both sources

1. Read `~/TODO.md` — extract ALL `[ ]` items. Tag each with source=TODO and their section header (🔴 Nu, 🏥 HealthReporting, 💼 indie-products, etc.)
2. Read `docs/PROJECT_PLAN.md` — extract ALL tasks with status `⬜ not started` or `🔵 in progress`. Tag each with source=PROJECT_PLAN and their phase.

## Step 2: Classify each item

Assign a priority tier:

| Tier | Criteria |
|------|----------|
| **BLOCKER** | Explicitly marked as blocker, or required before another task can start |
| **P0** | Strategic / income / momentum — items from TODO.md 🔴 section, or marked P0 |
| **ACTIVE** | Currently in progress (🔵) in PROJECT_PLAN.md, or current phase tasks |
| **P1** | High-value but not urgent — next sprint candidates |
| **BACKLOG** | Everything else |

## Step 3: Output ranked table

Present a merged, deduplicated table:

| Priority | Task | Source | Recommended now? |
|-----------|------|-------|---------------|
| BLOCKER | ... | TODO / PROJECT_PLAN phase X | ✅ Yes |
| P0 | ... | ... | ✅ Yes |
| ACTIVE | ... | ... | 🔵 Already in progress |
| P1 | ... | ... | Next sprint |
| BACKLOG | ... | ... | Later |

Rules:
- If the same task appears in both sources, show it once with source=BOTH
- Do NOT include completed tasks (✅ done)
- Do NOT include items from `## 🖥️ Mac Mini Setup`, `## 🤖 Claude Cowork`, `## 🛠️ Tools`, or `## 🎓 Learning` unless they are marked P0

## Step 4: Top 3 for this session

After the table, output:

---

### Top 3 for this session

Select 3 tasks that are:
1. Not blocked by anything
2. Highest strategic impact (income, momentum, or unblocking others)
3. Completable in a single session

Format:
1. **[Task name]** — [1-sentence rationale] *(Source: TODO/PROJECT_PLAN)*
2. ...
3. ...

---

Do not start coding. This command is for planning and prioritization only.
