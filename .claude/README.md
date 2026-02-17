# Claude Code Setup for City Cycles

This directory contains Claude Code configuration following Boris Cherny's best practices for power users.

## 📁 Directory Structure

```
.claude/
├── commands/           # Slash commands for common workflows
│   ├── run-pipeline.md
│   ├── validate-data.md
│   └── review-changes.md
├── agents/            # Specialized subagents for complex tasks
│   ├── verify-app.md
│   ├── code-simplifier.md
│   ├── data-quality-validator.md
│   ├── pipeline-troubleshooter.md
│   └── code-architect.md
└── settings.json      # Pre-allowed permissions & hooks
```

> **Note:** `quick-commit`, `commit-push-pr`, and `test-and-fix` are available as global skills (installed in `~/.claude/skills/`) rather than project-level commands.

## 🚀 Quick Start

### Using Slash Commands

In Claude Code, type `/` to see available commands:

| Command | Description |
|---------|-------------|
| `/run-pipeline` | Execute orchestrator pipeline with validation |
| `/validate-data` | Run comprehensive data quality checks |
| `/review-changes` | Review uncommitted changes and suggest improvements |

**Global skills** (available across all projects):
| Skill | Description |
|-------|-------------|
| `/quick-commit` | Fast commit with conventional commits format |
| `/commit-push-pr` | Full git workflow: commit, push, and create PR |

**Example:**
```
User: "Run the tests and fix any issues"
Claude: [Executes /test-and-fix command]
```

### Using Subagents

Ask Claude to use a specialized agent:

| Agent | Purpose |
|-------|---------|
| `verify-app` | Test pipeline end-to-end after changes |
| `code-simplifier` | Simplify code without changing functionality |
| `data-quality-validator` | Validate data quality and detect anomalies |
| `pipeline-troubleshooter` | Debug pipeline failures and performance issues |
| `code-architect` | Design reviews and architectural decisions |

**Example:**
```
User: "Use the verify-app agent to test everything"
Claude: [Launches verify-app subagent]
```

## ⚙️ Settings & Permissions

### Pre-Allowed Commands

The following commands won't prompt for permission (see `settings.json`):

**Python & Testing:**
- `python -m pytest*`
- `python -m orchestrator*`
- `python -m extraction*`
- `python -m mypy*`
- All Python module executions

**dbt:**
- `dbt run*`, `dbt test*`, `dbt compile*`, `dbt docs*`

**Git & GitHub:**
- All `git` commands (status, diff, commit, push, etc.)
- `gh pr*`, `gh issue*` (GitHub CLI)

**Package Management:**
- `pip install*`, `pip list*`, `pip show*`

**AWS (Read-only):**
- `aws s3 ls*`
- `aws s3 sync --dryrun*`

### Denied Commands (Safety)

These commands require explicit approval:
- `aws s3 rm*` (S3 deletion)
- `aws s3 rb*` (S3 bucket removal)
- `rm -rf*` (recursive deletion)
- `DROP TABLE*`, `TRUNCATE TABLE*` (destructive SQL)

### PostToolUse Hook

Python files are automatically formatted after Write/Edit operations using:
- `black` (preferred)
- `autopep8` (fallback)

This prevents formatting errors in CI and keeps code consistent.

## 📖 Project Documentation

See `../CLAUDE.md` for comprehensive project-specific guidance:
- Project architecture overview
- Development workflow and verification loops
- Code style conventions
- Common commands reference
- Things Claude should NOT do
- Project-specific patterns
- Common issues & solutions

## 💡 Best Practices (from Boris)

1. **Give Claude verification loops** - Run tests, typecheck, lint after changes for 2-3x quality improvement

2. **Use Plan mode** (shift+tab twice) - Iterate on plan before implementation

3. **Update CLAUDE.md continuously** - Every mistake is a learning opportunity

4. **Automate repetitive workflows** - Use slash commands for common tasks

5. **Use subagents for specialized work** - Let experts handle their domain

6. **Pre-allow safe commands** - Avoid permission prompts for known-safe operations

## 🎯 City Cycles Specific Tips

### Before Making Changes
```bash
# Always run tests first
python -m pytest tests/ -v

# Check current pipeline status
python -m orchestrator.cli status
```

### After Making Changes
```bash
# Run tests
python -m pytest tests/ -v

# Validate data quality
/validate-data  # Use the slash command

# Test specific pipeline stage
python -m orchestrator.cli stage <stage_name>
```

### Common Workflows

**Feature Development:**
1. Make changes
2. `/test-and-fix` - Run tests and fix issues
3. `/validate-data` - Check data quality
4. Use `verify-app` agent for end-to-end testing
5. `/commit-push-pr` - Create PR

**Bug Fixing:**
1. Use `pipeline-troubleshooter` agent to diagnose
2. Fix the issue
3. `/test-and-fix` - Verify fix
4. `/quick-commit` - Commit the fix

**Refactoring:**
1. `/test-and-fix` - Ensure tests pass before refactoring
2. Make changes
3. Use `code-simplifier` agent to clean up
4. `/test-and-fix` - Verify behavior unchanged
5. `/review-changes` - Review quality

**Architecture Decisions:**
1. Use `code-architect` agent for design review
2. Discuss trade-offs and options
3. Implement chosen approach
4. Document decision in CLAUDE.md

## 🔧 Customization

### Adding New Commands

Create a new file in `.claude/commands/`:

```markdown
---
description: "Brief description (shows in command list)"
---

Instructions for Claude to follow when this command is invoked.

Can include:
- Step-by-step procedures
- Bash commands to run
- Validation checks
- Error handling guidance
```

### Adding New Agents

Create a new file in `.claude/agents/`:

```markdown
# Agent Name

You are a [role] specialist. Your job is to [mission].

## Your Task
[Detailed instructions for the agent]

## Process
[Step-by-step workflow]

## Guidelines
[Rules and best practices]

## Reporting
[How to report results]
```

### Modifying Permissions

Edit `settings.json`:

```json
{
  "permissions": {
    "allow": [
      "Bash(your-command*)"
    ],
    "deny": [
      "Bash(dangerous-command*)"
    ]
  }
}
```

## 📚 Resources

- [Claude Code Documentation](https://code.claude.com/docs)
- [Boris Cherny's Setup Thread](https://x.com/bcherny/status/2007179832300581177)
- [City Cycles README](../README.md)
- [Project CLAUDE.md](../CLAUDE.md)

---

**Setup Date:** 2025-01-26
**Based on:** Boris Cherny's Claude Code best practices
**Customized for:** City Cycles Analytics Pipeline (Python + DuckDB + dbt + AWS)
