# Changelog

<!--
  Contributors: add a bullet point describing your change under [Unreleased].
  You don't need to add a PR reference or your name — CI does that automatically.
-->

## [Unreleased]

- SQLite over SSH — browse and query remote SQLite files via SSH exec channels ([#2](https://github.com/emanuele-em/cove/pull/2) by [@eznix86](https://github.com/eznix86))
- DuckDB over SSH — same remote CLI execution pattern as SQLite
- Shared `FileBackendExecution` abstraction for file-based backend SSH support
- Extracted SSH connection primitives into shared `SSHSupport` module
- Replaced `isFileBased` boolean with `BackendCapabilities` struct

## [0.1.0] - 2026-03-20

- PostgreSQL backend with full schema browsing, TLS support, and type decoders
- MySQL backend with multi-database support and TLS fallback
- MariaDB backend
- SQLite backend (file-based, no external dependency)
- MongoDB backend with shell-style commands and document schema inference
- Redis backend with command-based execution and dynamic type discovery
- ScyllaDB backend (CQL)
- Cassandra backend (CQL)
- Elasticsearch backend with REST-style query execution
- Oracle backend with schema-based navigation and connection pool pattern
- SQL Server backend with multi-database + schema support
- DuckDB backend (file-based analytical DB, C API)
- ClickHouse backend with column-oriented OLAP support
- Sidebar tree for browsing databases, schemas, tables, views, indexes, and keys
- Inline row editing with SQL/CQL preview before commit
- Query editor with syntax highlighting for keywords, strings, numbers, and comments
- SQL/CQL autocomplete engine with schema-aware completions
- Multiple tabs with independent connections (Cmd+T)
- Connection environments (local, dev, staging, production)
- SSH tunneling with password and private key authentication
- Session persistence and restore across app relaunches
- Encrypted credential storage via macOS Keychain
- Color-coded connection indicators
- Context menu actions for creating and dropping database objects
- Data pagination with sorting
- Table structure tab showing columns, indexes, and triggers
- GitHub Actions CI (build + test on macOS 15)
- GitHub Actions release workflow (DMG + ZIP on tag push)
