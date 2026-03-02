# Dogfood Report: Agentic Engineering OS applied to HealthReporting

**Date:** 2026-03-02
**Repo:** HealthReporting (personal health data platform)
**Framework repos tested:**
- ai-governance-framework (health score calculator, CI templates)
- ai-engineering-standards (validator CLI)
- ai-project-management (YAML task schema, task templates)
- ai-project-templates (scaffold.py)

---

## Summary

The Agentic Engineering OS was applied to a real, pre-existing project (HealthReporting) that was built organically before the framework existed. This is the first dogfood test on a project that was not scaffolded from the templates.

**Verdict:** The framework works, but it shows clear bias toward greenfield projects. Retrofitting an existing project requires significant manual work that the tools do not assist with.

---

## What Worked

### 1. Health score calculator gives actionable feedback
The governance health score (55%, Level 2 "Structured") provided a clear, itemized list of what is present and what is missing. The output is immediately actionable — each missing item has a point value, so you can prioritize improvements by impact.

### 2. ai-standards validator is useful
Running `ai-standards validate` against HealthReporting produced a clear 4/11 compliance report. The validator correctly identified: missing ruff.toml, missing pyproject.toml, missing tests/ directory, missing LICENSE, CLAUDE.md missing Identity/Scope/Boundaries sections, naming convention violations, and missing docstrings. Each check has a clear pass/fail with explanation.

### 3. YAML task format is clean and practical
The one-file-per-task approach in ai-project-management is genuinely good for agentic workflows. Each task is self-contained, has clear fields (status, priority, depends_on, definition_of_done), and the schema is well-defined. Creating 6 new task files was straightforward using the template.

### 4. CLAUDE.md template sections are well-chosen
The Identity/Scope/Boundaries structure from the framework makes the CLAUDE.md more useful for agents. Before dogfooding, HealthReporting's CLAUDE.md had extensive session protocols but no clear statement of "what this project is" and "what agents must not do." Adding these sections is a genuine improvement.

### 5. CI template is a good starting point
The governance-gate workflow template from ai-governance-framework provides a reasonable CI skeleton. The secret scanning patterns and .gitignore checks are practical.

---

## What Was Friction

### 1. scaffold.py only works for new projects
The scaffolder creates a new directory from scratch. There is no `scaffold.py --retrofit` or `scaffold.py --audit` mode that analyzes an existing project and suggests what to add. For the most common real-world scenario (an existing project wanting to adopt the framework), the scaffolder provides zero value.

**Recommendation:** Add a `scaffold.py --audit <path>` command that compares an existing project against the expected structure and outputs a gap report.

### 2. Two task directories: backlog/ vs .tasks/
HealthReporting already had a `backlog/` directory with TASK-001 through TASK-003. The ai-project-management template uses `backlog/` as the default directory name, but the dogfood instructions specified `.tasks/`. This creates confusion: which is the canonical location? The framework should have a single, opinionated default.

**Recommendation:** Pick one directory name and document it. If projects want to customize, support a config option, not an implicit "either works" approach.

### 3. Health score checks for files that do not make sense for all projects
The health score checks for `PROJECT_PLAN.md` and `ARCHITECTURE.md` at the repo root, but HealthReporting keeps these in `docs/`. The scorer does not know about alternative locations. Similarly, it checks for `patterns/` and `automation/` directories, which are governance-framework-specific concepts that do not map to a data engineering project.

**Recommendation:** Allow a `.governance.yaml` config file that maps file locations, or make the scorer scan common alternative paths (e.g., `docs/PROJECT_PLAN.md`, `docs/architecture.md`).

### 4. CLAUDE.md is already 460+ lines — framework wants max 200
The framework template header says "Maximum 200 lines. Agents under context pressure miss rules at the bottom." HealthReporting's CLAUDE.md is over 460 lines because it includes domain-specific rules (health data handling, Databricks conventions, model routing, security protocol). Trimming to 200 lines would lose critical domain context.

**Recommendation:** Either increase the guideline or provide a clear inheritance mechanism (e.g., CLAUDE.md for core rules, CLAUDE.domain.md for domain-specific extensions) that agents know to load.

### 5. ai-standards validator checks are shallow
The validator checks file existence and AST-based naming, but does not validate content quality. A CLAUDE.md with just "# Identity\n# Scope\n# Boundaries" and no content would pass. The naming convention check also flags legitimate Python HTTP handler methods like `do_GET` (standard library convention) as violations.

**Recommendation:** Add content-quality checks (minimum section length, required fields). Add an allow-list mechanism for known naming exceptions.

### 6. No unified CLI
Each tool has its own invocation pattern:
- `python -m ai_standards validate <path>` (but actually requires the entry point `ai-standards validate <path>`)
- `python automation/health_score_calculator.py <path>`
- `python scaffold.py --name <name> --stack <stack> --mode <mode>`

There is no single `agentic-os check <path>` command that runs all validators. For a framework that positions itself as an "operating system," this fragmentation is notable.

**Recommendation:** Create a unified CLI entry point or at minimum a shell script that chains the three validators.

---

## What Is Missing from the Framework

### 1. Retrofit / migration path
No tool or guide exists for "I have an existing project, how do I adopt the framework?" The implicit assumption is that every project starts from scaffold.py. This is the single biggest gap.

### 2. Data engineering conventions
The framework is implicitly web/API focused. No templates or conventions exist for:
- Medallion architecture (bronze/silver/gold)
- SQL transform organization
- Data pipeline testing patterns
- YAML-driven config schemas for data sources
- DuckDB/Databricks-specific patterns

### 3. CLAUDE.md section detection is fragile
The health score calculator uses regex to find sections like `mandatory_session_protocol`. HealthReporting's CLAUDE.md uses a heading `## Mandatory Session Protocol` (with spaces and capitalization), which does not match the regex looking for `mandatory_session_protocol`. The section IS there but is not detected.

### 4. No task migration tool
HealthReporting has an extensive TODO.md (200+ lines) with prioritized backlog items. There is no tool to convert a markdown TODO list into individual YAML task files. This conversion was done manually.

### 5. Cross-repo references are fragile
The framework assumes the governance repos are at specific paths (`~/Github repos/ai-governance-framework`). If a user clones them elsewhere, nothing works. There is no discovery mechanism or environment variable fallback.

---

## Scores

### ai-standards validate
| Check | Status |
|---|---|
| Ruff config | FAIL |
| pyproject.toml [project] | FAIL |
| Pre-commit config | PASS |
| CLAUDE.md | PASS |
| CLAUDE.md sections (Identity/Scope/Boundaries) | PASS (after update) |
| Tests directory | FAIL |
| CI/CD workflows | PASS |
| README.md | PASS |
| LICENSE | FAIL |
| Naming conventions | FAIL (do_GET flagged) |
| Docstring presence | FAIL (44 missing) |

**Score: 5/11** (after adding Identity/Scope/Boundaries to CLAUDE.md)

### Governance health score
| Check | Points | Status |
|---|---|---|
| CLAUDE.md exists | 10 | PASS |
| CLAUDE.md section: project_context | 2 | FAIL |
| CLAUDE.md section: conventions | 2 | PASS |
| CLAUDE.md section: mandatory_session_protocol | 2 | FAIL |
| CLAUDE.md section: security_protocol | 2 | PASS |
| CLAUDE.md section: mandatory_task_reporting | 2 | PASS |
| PROJECT_PLAN.md exists | 5 | FAIL |
| CHANGELOG.md with 3+ entries | 10 | FAIL |
| ARCHITECTURE.md exists | 5 | FAIL |
| MEMORY.md exists | 5 | FAIL |
| At least 1 ADR in docs/adr/ | 5 | PASS |
| .pre-commit-config.yaml exists | 10 | PASS |
| GitHub Actions workflow exists | 5 | PASS |
| AI review workflow (anthropic/claude) | 10 | PASS |
| Agent definition exists | 5 | PASS |
| Command definition exists | 5 | PASS |
| patterns/ directory with files | 5 | FAIL |
| automation/ directory exists | 5 | FAIL |
| .gitignore includes .env | 5 | PASS |
| AGENTS.md portable governance bridge | 5 | FAIL |
| Self-validation checklist | 5 | FAIL |

**Score: 55% (61/110 points) — Level 2 (Structured)**

### scaffold.py compatibility
The scaffold expects: `src/bronze/`, `src/silver/`, `src/gold/`, `tests/`, `.engineering/`, `CONTEXT.md`, `ARCHITECTURE.md`, `AGENTS.md`.
HealthReporting has: `health_unified_platform/`, `docs/architecture.md`, `.claude/agents/`, `backlog/`.

**Compatibility: Low.** The project structure does not match the scaffold output. This is expected — HealthReporting was built before the framework existed.

---

## Conclusion

The Agentic Engineering OS is a solid governance framework for greenfield projects. The individual tools (health score, standards validator, task schema) each provide real value. The biggest gaps are:

1. **No retrofit path** — the framework assumes you start from scratch
2. **Shallow validation** — checks file existence, not content quality
3. **No unified CLI** — three separate tools with different invocation patterns
4. **Web-biased conventions** — no data engineering templates or patterns
5. **Fragile section detection** — regex matching breaks on natural heading styles

The framework is ready for early adopters who start new projects. It is not yet ready for adoption by existing projects without significant manual effort.
