# Prelight Documentation

Welcome to the Prelight developer documentation. Start here and pick the document that matches what you need.

| Document | Audience | What it covers |
|----------|----------|----------------|
| [Onboarding Guide](ONBOARDING.md) | New developers | Environment setup, repo tour, running tests, your first contribution |
| [Architecture (High-Level)](ARCHITECTURE.md) | Everyone | What Prelight is, system design, the 7-step migration flow, safety model, key design decisions |
| [Low-Level Design](LOW_LEVEL_DESIGN.md) | Contributors | Module-by-module reference: every MCP tool, function, class, guard rule, and SQL template |

## Suggested reading order for a new developer

1. Read the project [README](../README.md) for the product pitch and the end-user experience.
2. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system shape and why it is built this way.
3. Follow [ONBOARDING.md](ONBOARDING.md) to get a working dev environment and run the test suite.
4. Keep [LOW_LEVEL_DESIGN.md](LOW_LEVEL_DESIGN.md) open as a reference while reading or changing code.

## What is Prelight in one paragraph

Prelight is an [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that lets AI clients (Claude Desktop, Claude Code) transform production data **safely**. Every change goes into a sandbox copy of the table first, gets validated by SQL quality checks, and ships as a reviewable GitHub pull request. Production tables are never written to — that invariant is enforced by SQL AST inspection (and, on Databricks, by read-only credentials).
