---
description: "Stage all changes and commit with a descriptive message"
---

Follow these steps in order:

1. Run `git status` to see what files have changed
2. Run `git diff` to review the changes
3. Stage all relevant files with `git add`
4. Create a commit with a clear, descriptive message following conventional commits format:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `refactor:` for code refactoring
   - `docs:` for documentation changes
   - `test:` for test additions/changes
   - `chore:` for maintenance tasks

Example commit messages:
- `feat(extraction): add support for 2025 London data schema`
- `fix(dbt): correct date filtering in staging models`
- `refactor(orchestrator): improve error handling in pipeline stages`

If there are any issues, stop and report them.
