import Foundation

final class SQLiteBackend: DatabaseBackend, @unchecked Sendable {
    static let sqliteKeywords: Set<String> = [
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE", "SET",
        "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "DATABASE",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON", "AS",
        "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "EXISTS",
        "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "DISTINCT",
        "UNION", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "BEGIN",
        "COMMIT", "ROLLBACK", "TRANSACTION", "VALUES", "DEFAULT", "PRIMARY",
        "KEY", "FOREIGN", "REFERENCES", "CASCADE", "CONSTRAINT", "CHECK",
        "UNIQUE", "ASC", "DESC", "WITH", "RECURSIVE", "RETURNING",
        "TRIGGER", "IF", "REPLACE", "ABORT", "FAIL", "IGNORE",
        "BOOLEAN", "INTEGER", "BIGINT", "SMALLINT", "TEXT", "VARCHAR",
        "CHAR", "NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE", "DATE",
        "TIME", "TIMESTAMP", "BLOB", "JSON",
        "TRUE", "FALSE", "COUNT", "SUM", "AVG", "MIN", "MAX",
        "COALESCE", "CAST", "OVER", "PARTITION", "ROW_NUMBER", "RANK",
        "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
        "AUTOINCREMENT", "VACUUM", "REINDEX", "ATTACH", "DETACH",
        "PRAGMA", "EXPLAIN", "GLOB", "REGEXP", "COLLATE", "NOCASE",
        "ROWID", "WITHOUT",
    ]

    let name = "SQLite"
    let syntaxKeywords = SQLiteBackend.sqliteKeywords

    private let execution: any FileBackendExecution

    private init(execution: any FileBackendExecution) {
        self.execution = execution
    }

    static func connect(config: ConnectionConfig) async throws -> SQLiteBackend {
        let execution: any FileBackendExecution
        if let sshConfig = config.sshTunnel {
            execution = try await RemoteCLIExecution.connect(binaryName: "sqlite3", path: config.database, sshConfig: sshConfig)
        } else {
            execution = try SQLiteLocalExecution.connect(path: config.database)
        }
        return SQLiteBackend(execution: execution)
    }

    var isReadOnly: Bool {
        execution.isReadOnly
    }

    func runQuery(_ sql: String) async throws -> QueryResult {
        try await execution.query(sql)
    }

    func runExec(_ sql: String) async throws -> UInt64? {
        try await execution.execute(sql)
    }

    func quoteIdentifier(_ name: String) -> String {
        let escaped = name.replacingOccurrences(of: "\"", with: "\"\"")
        return "\"\(escaped)\""
    }

    func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }
}
