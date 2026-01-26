---
description: "Review uncommitted changes and suggest improvements"
---

Perform a comprehensive code review of uncommitted changes:

## 1. Identify Changes

```bash
# See what files changed
git status

# Review the actual changes
git diff
```

## 2. Review Checklist

Analyze changes for:

### Code Quality
- ✓ Clear, descriptive variable names
- ✓ Functions are focused and single-purpose
- ✓ Proper error handling (no silent failures)
- ✓ Type hints for function signatures
- ✓ Docstrings for non-obvious functions

### Data Engineering Best Practices
- ✓ Schema validation before processing data
- ✓ Idempotent operations (safe to re-run)
- ✓ Memory management for large files (pandas chunking)
- ✓ Proper logging with context
- ✓ Environment variables for configuration (not hardcoded)

### Testing
- ✓ Tests added for new functionality
- ✓ Tests updated for modified code
- ✓ Edge cases covered

### Documentation
- ✓ README updated if needed
- ✓ Comments explain "why" not "what"
- ✓ CLAUDE.md updated with learnings

### Security
- ✓ No credentials committed (.env, AWS keys)
- ✓ No sensitive data in code
- ✓ .gitignore properly configured

## 3. Suggest Improvements

Provide specific, actionable feedback:
- Highlight potential issues
- Suggest refactoring opportunities
- Recommend additional tests
- Note any anti-patterns

## 4. Validation Recommendations

Suggest verification steps:
- Which tests to run
- Manual validation steps
- Pipeline stages to test

Format suggestions clearly with file paths and line numbers.
