---
name: git-commit
description: Safely commit and push changes to the dbt-icebreaker repository using the tysondoberneck GitHub account.
---

# Git Commit Skill for dbt-icebreaker

## Purpose
Commit staged or unstaged changes to the `dbt-icebreaker` repository, ensuring the correct GitHub identity (`tysondoberneck`) and remote (`https://github.com/tysondoberneck/dbt-icebreaker.git`) are used.

## Pre-flight Checks (MANDATORY)

Before making any commit, you **MUST** verify all of the following. If any check fails, **STOP** and inform the user.

1. **Working directory** must be `/Users/tysondoberneck/codebase/dbt-icebreaker`.
2. **Current branch** – Run `git branch --show-current` and confirm the branch belongs to the `dbt-icebreaker` project. Do **NOT** commit on `main` or `develop` unless the user explicitly approves.
3. **Git identity** – Run `git config user.name` and `git config user.email`. They must be:
   - `user.name` = `tysondoberneck`
   - `user.email` = `tysondoberneck@users.noreply.github.com`
   If they differ, set them for this repo before committing:
   ```bash
   git config user.name "tysondoberneck"
   git config user.email "tysondoberneck@users.noreply.github.com"
   ```
4. **Remote** – Run `git remote -v` and confirm origin points to `https://github.com/tysondoberneck/dbt-icebreaker.git`.

## Commit Workflow

1. **Show status** – Run `git status` and present the summary to the user.
2. **Stage files** – Ask the user which files to stage, or stage all with `git add -A` if they approve.
3. **Compose commit message** – Suggest a conventional-commit-style message (e.g., `feat:`, `fix:`, `chore:`, `docs:`). Let the user confirm or edit the message.
4. **Commit** – Run:
   ```bash
   git commit -m "<approved message>"
   ```
5. **Push** – Ask the user if they want to push. If yes:
   ```bash
   git push origin <current-branch>
   ```
   If the branch has no upstream yet:
   ```bash
   git push -u origin <current-branch>
   ```

## Rules

> [!CAUTION]
> **NEVER** create a Pull Request or merge into any branch without explicit user permission (per user rule #5).

- Always show the user the exact `git` commands you intend to run **before** executing them.
- If there are merge conflicts or push rejections, stop and inform the user rather than attempting an automatic resolution.
- Never force-push (`--force` or `--force-with-lease`) without explicit approval.
