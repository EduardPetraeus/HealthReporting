#!/usr/bin/env python3
"""
Auto-update claude_code_productivity.json with git-derived metrics.

Usage:
    python update_productivity.py

Run after each commit. Preserves any manually added fields (e.g. attempted_solo).
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).parent
JSON_PATH = REPO_ROOT / "claude_code_productivity.json"


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def git(args: list[str]) -> str:
    result = subprocess.run(
        ["git"] + args, capture_output=True, text=True, cwd=REPO_ROOT
    )
    return result.stdout.strip()


def get_commits(since_date: str) -> list[dict]:
    """Return all commits after since_date, oldest first."""
    log = git(["log", f"--after={since_date}", '--format=%H|%ad|%s', '--date=format:%Y-%m-%dT%H:%M:%S', "--reverse"])
    commits = []
    for line in log.splitlines():
        if not line:
            continue
        sha, timestamp, message = line.split("|", 2)
        dt = datetime.fromisoformat(timestamp)
        commits.append({
            "sha": sha[:7],
            "sha_full": sha,
            "dt": dt,
            "date": dt.strftime("%Y-%m-%d"),
            "hour": dt.hour,
            "message": message,
        })
    return commits


def get_stats(sha_full: str) -> dict:
    """Return files_changed, lines_added, lines_removed for a commit."""
    output = git(["diff-tree", "--no-commit-id", "-r", "--stat", sha_full])
    summary = output.splitlines()[-1] if output else ""
    files = lines_added = lines_removed = 0
    for part in summary.split(","):
        part = part.strip()
        if "file" in part:
            files = int(part.split()[0])
        elif "insertion" in part:
            lines_added = int(part.split()[0])
        elif "deletion" in part:
            lines_removed = int(part.split()[0])
    return {
        "files_changed": files,
        "lines_added": lines_added,
        "lines_removed": lines_removed,
    }


# ---------------------------------------------------------------------------
# Derived metrics
# ---------------------------------------------------------------------------

TASK_TYPE_RULES = [
    (["fix", "bug", "hotfix", "correct", "patch"],           "starts", "fix"),
    (["refactor", "restructure", "clean", "reorgani"],        "contains", "refactor"),
    (["readme", " md ", ".md", "docs", "documentation"],      "contains", "docs"),
    (["gitignore", "config", "yaml", "yml", "settings"],      "contains", "config"),
    (["setup", "init", "initial", "scaffold"],                "contains", "setup"),
    (["archive", "legacy", "migrate"],                        "contains", "archive"),
    (["add", "new", "create", "implement", "build"],          "starts", "new_feature"),
    (["update", "change", "improve"],                          "starts", "fix"),
]


def infer_task_type(message: str) -> str:
    msg = message.lower()
    for keywords, mode, task_type in TASK_TYPE_RULES:
        if mode == "starts" and any(msg.startswith(k) for k in keywords):
            return task_type
        if mode == "contains" and any(k in msg for k in keywords):
            return task_type
    return "other"


def refactor_ratio(lines_added: int, lines_removed: int) -> Optional[float]:
    total = lines_added + lines_removed
    if total == 0:
        return None
    return round(lines_removed / total, 3)


def code_density(lines_added: int, files_changed: int) -> Optional[float]:
    if files_changed == 0:
        return None
    return round(lines_added / files_changed, 1)


def compute_streak(commits: list[dict], up_to_index: int) -> int:
    """Consecutive active days ending at commits[up_to_index]."""
    from datetime import timedelta
    dates = sorted(set(
        datetime.strptime(c["date"], "%Y-%m-%d").date()
        for c in commits[:up_to_index + 1]
    ))
    if not dates:
        return 1
    streak = 1
    for i in range(len(dates) - 1, 0, -1):
        if (dates[i] - dates[i - 1]).days == 1:
            streak += 1
        else:
            break
    return streak


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_entry(
    commit: dict,
    stats: dict,
    baseline_avg_files: float,
    time_since_prev_minutes: Optional[int],
    streak: int,
    existing: Optional[dict],
) -> dict:
    files = stats["files_changed"]
    added = stats["lines_added"]
    removed = stats["lines_removed"]

    auto = {
        "commit_sha": commit["sha"],
        "date": commit["date"],
        "message": commit["message"],
        "task_type": infer_task_type(commit["message"]),
        "commit_hour": commit["hour"],
        "files_changed": files,
        "lines_added": added,
        "lines_removed": removed,
        "files_factor": round(files / baseline_avg_files, 2) if files else 0,
        "refactor_ratio": refactor_ratio(added, removed),
        "code_density": code_density(added, files),
        "time_since_prev_commit_minutes": time_since_prev_minutes,
        "streak_days": streak,
    }

    if existing:
        # Preserve manual fields, overwrite auto fields
        merged = {**existing, **auto}
        return merged

    return auto


def update():
    data = json.loads(JSON_PATH.read_text())
    baseline_avg_files = data["baseline"]["avg_files_per_commit"]
    since_date = data["meta"]["claude_code_start_date"]

    # Index existing entries by SHA for merge
    existing_by_sha = {e["commit_sha"]: e for e in data.get("entries", [])}

    commits = get_commits(since_date)
    if not commits:
        print("No commits found since", since_date)
        return

    entries = []
    prev_dt = None

    for i, commit in enumerate(commits):
        stats = get_stats(commit["sha_full"])
        streak = compute_streak(commits, i)

        if prev_dt is not None:
            delta = int((commit["dt"] - prev_dt).total_seconds() / 60)
        else:
            delta = None
        prev_dt = commit["dt"]

        entry = build_entry(
            commit=commit,
            stats=stats,
            baseline_avg_files=baseline_avg_files,
            time_since_prev_minutes=delta,
            streak=streak,
            existing=existing_by_sha.get(commit["sha"]),
        )
        entries.append(entry)

    # Recompute summary (exclude outliers: files_factor is None)
    valid = [e for e in entries if e.get("files_factor") is not None]
    avg_files = round(sum(e["files_changed"] for e in valid) / len(valid), 1) if valid else 0

    from collections import Counter
    active_weeks = len(set(
        datetime.strptime(c["date"], "%Y-%m-%d").strftime("%Y-%W")
        for c in commits
    ))
    commits_per_week = round(len(commits) / active_weeks, 1) if active_weeks else 0

    data["claude_code_summary"] = {
        "period": data["claude_code_summary"]["period"],
        "total_commits": len(commits),
        "active_weeks": active_weeks,
        "avg_commits_per_active_week": commits_per_week,
        "avg_files_per_commit": avg_files,
    }

    baseline_velocity = data["baseline"]["avg_commits_per_active_week"]
    data["factors"] = {
        "velocity_factor": round(commits_per_week / baseline_velocity, 2),
        "files_per_commit_factor": round(avg_files / baseline_avg_files, 2),
        "description": data["factors"]["description"],
    }

    data["meta"]["last_updated"] = datetime.today().strftime("%Y-%m-%d")
    data["entries"] = entries

    JSON_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    # Summary print
    print(f"\n{'='*50}")
    print(f"  Claude Code Productivity — {data['meta']['last_updated']}")
    print(f"{'='*50}")
    print(f"  Commits tracked   : {len(commits)}")
    print(f"  Active weeks      : {active_weeks}")
    print(f"  Velocity factor   : {data['factors']['velocity_factor']}x  (commits/active week)")
    print(f"  Files factor      : {data['factors']['files_per_commit_factor']}x  (files/commit)")

    task_counts = Counter(e["task_type"] for e in entries)
    print(f"\n  Task breakdown:")
    for task_type, count in sorted(task_counts.items(), key=lambda x: -x[1]):
        print(f"    {task_type:<20} {count}")
    print()


if __name__ == "__main__":
    update()
