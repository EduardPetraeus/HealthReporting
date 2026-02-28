#!/usr/bin/env python3
"""
AI PR Review — Layer 3 enforcement (Tier 3, probabilistic).

Feeds PR diff + constitution files to Claude, posts a structured review
comment on the PR with verdict: PASS / WARN / FAIL.

FAIL blocks merge (exit code 1).
WARN allows merge with human awareness (exit code 0).
PASS is clean (exit code 0).

Required env vars:
  ANTHROPIC_API_KEY  — Claude API key (stored in GitHub Secrets)
  GITHUB_TOKEN       — GitHub token (auto-provided in Actions)
  PR_NUMBER          — PR number (from github.event.pull_request.number)
  REPO               — owner/repo (from github.repository)

Usage: python scripts/ai_pr_review.py
"""

import json
import os
import subprocess
import sys
import urllib.request

import anthropic

MAX_DIFF_CHARS = 12000
MAX_FILE_CHARS = 6000
MAX_FILES = 25
MODEL = 'claude-haiku-4-5-20251001'


def get_pr_diff():
    result = subprocess.run(
        ['git', 'diff', 'origin/main...HEAD', '--name-only'],
        capture_output=True, text=True
    )
    changed_files = [f for f in result.stdout.strip().split('\n') if f]

    diffs = []
    total_chars = 0
    for filepath in changed_files[:MAX_FILES]:
        diff = subprocess.run(
            ['git', 'diff', 'origin/main...HEAD', '--', filepath],
            capture_output=True, text=True
        )
        chunk = f"### {filepath}\n{diff.stdout}"
        truncated = chunk[:3000] + ('\n[...truncated]' if len(chunk) > 3000 else '')
        diffs.append(truncated)
        total_chars += len(truncated)
        if total_chars > MAX_DIFF_CHARS:
            diffs.append(f"\n[Review truncated — {len(changed_files) - MAX_FILES} files not shown]")
            break

    return '\n\n'.join(diffs), changed_files


def read_constitution():
    files = {}
    for path in ['CLAUDE.md', 'docs/ARCHITECTURE.md', 'docs/adr']:
        if os.path.isfile(path):
            with open(path) as fh:
                files[path] = fh.read()[:MAX_FILE_CHARS]
        elif os.path.isdir(path):
            adr_content = []
            for adr_file in sorted(os.listdir(path))[:4]:
                adr_path = os.path.join(path, adr_file)
                if adr_file.endswith('.md'):
                    with open(adr_path) as fh:
                        adr_content.append(fh.read()[:1500])
            files['docs/adr/'] = '\n\n---\n\n'.join(adr_content)
    return files


def post_pr_comment(body):
    repo = os.environ['REPO']
    pr_number = os.environ['PR_NUMBER']
    token = os.environ['GITHUB_TOKEN']

    url = f'https://api.github.com/repos/{repo}/issues/{pr_number}/comments'
    data = json.dumps({'body': body}).encode()
    req = urllib.request.Request(url, data=data, headers={
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
    })
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        print(f"Failed to post PR comment: {e}")


def run_review(diff, changed_files, constitution):
    client = anthropic.Anthropic()

    constitution_text = '\n\n'.join(
        f'## {k}\n{v}' for k, v in constitution.items()
    )

    prompt = f"""You are an AI code reviewer for a personal health data platform (HealthReporting).
Review this PR diff against the project constitution and architecture decisions.

{constitution_text}

## PR Diff ({len(changed_files)} files changed)
{diff}

## Review Instructions
Respond with exactly this structure:

**VERDICT**: [PASS|WARN|FAIL]
- PASS: Follows all conventions, governance updated, no security issues
- WARN: Minor issues — can merge but developer should be aware
- FAIL: Critical issue — security violation, hardcoded secret, contradicts accepted ADR, architecture breach

**Summary** (2-3 sentences max)

**Issues** (bullet list, or "None" if clean):
- file.py:line — description

**Governance** (one line):
- CHANGELOG updated: yes/no
- ARCHITECTURE updated: yes/no (if relevant)

Be concise and specific. Focus on violations, not style preferences. If the PR is documentation-only, be lenient."""

    message = client.messages.create(
        model=MODEL,
        max_tokens=800,
        messages=[{'role': 'user', 'content': prompt}]
    )
    return message.content[0].text


def parse_verdict(review_text):
    first_200 = review_text[:200].upper()
    if 'FAIL' in first_200:
        return 'FAIL'
    if 'WARN' in first_200:
        return 'WARN'
    return 'PASS'


def main():
    print("Running AI PR review...")

    diff, changed_files = get_pr_diff()
    if not changed_files or changed_files == ['']:
        print("No changed files — skipping AI review.")
        sys.exit(0)

    constitution = read_constitution()
    review_text = run_review(diff, changed_files, constitution)
    verdict = parse_verdict(review_text)

    emoji = {'PASS': '✅', 'WARN': '⚠️', 'FAIL': '❌'}[verdict]

    comment = f"""## {emoji} AI Code Review — {verdict}

{review_text}

---
*Automated review · Model: `{MODEL}` · Files reviewed: {len(changed_files)}*
*Constitution: `CLAUDE.md` + `docs/ARCHITECTURE.md` + `docs/adr/`*"""

    if os.environ.get('GITHUB_TOKEN') and os.environ.get('PR_NUMBER'):
        post_pr_comment(comment)
        print(f"Posted PR comment — verdict: {verdict}")
    else:
        print("--- AI Review Output ---")
        print(comment)
        print("(GITHUB_TOKEN or PR_NUMBER not set — running in local mode)")

    if verdict == 'FAIL':
        print("AI Review returned FAIL — see PR comment for details.")
        sys.exit(1)


if __name__ == '__main__':
    main()
