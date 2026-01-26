---
description: "Commit, push, and open a PR"
---

Follow these steps in order:

1. Run `git status` to see what files have changed
2. Run `git diff` to review the changes
3. Stage the appropriate files with `git add`
4. Create a commit with a clear, descriptive message following conventional commits format
5. Push to the remote branch (create remote branch if needed with `-u origin <branch>`)
6. Create a Pull Request using `gh pr create` with:
   - A clear title summarizing the changes
   - A description with:
     - Summary of what changed and why
     - Any testing done (pytest results, pipeline validation, etc.)
     - Any data model or schema changes
     - Any notes for reviewers

Example PR description format:
```
## Summary
- Added support for 2025 NYC CitiBike data schema
- Updated data models and dbt staging layer

## Testing
- ✅ All pytest tests passing
- ✅ Schema validation tests added
- ✅ Tested with sample 2025 data files

## Notes
- New schema adds 'ride_id' column
- Backwards compatible with existing data
```

If there are any issues at any step, stop and report them.
