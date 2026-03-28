import Foundation
import ClickHouseNIO
import NIOCore
import NIOPosix
import Logging

final class ClickHouseBackend: DatabaseBackend, @unchecked Sendable {
    let name = "ClickHouse"
    private let config: ConnectionConfig
    private let lock = NSLock()
    private var connections: [ClickHouseConnection] = []

    let syntaxKeywords: Set<String> = [
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE", "SET",
        "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "DATABASE",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON", "AS",
        "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "EXISTS",
        "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "DISTINCT",
        "UNION", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END",
        "VALUES", "DEFAULT", "PRIMARY", "KEY", "ASC", "DESC", "WITH",
        "GRANT", "REVOKE", "TRUNCATE",
        "IF", "FUNCTION",
        "BOOLEAN", "INT8", "INT16", "INT32", "INT64",
        "UINT8", "UINT16", "UINT32", "UINT64",
        "FLOAT32", "FLOAT64", "DECIMAL",
        "STRING", "FIXEDSTRING", "UUID", "DATE", "DATE32",
        "DATETIME", "DATETIME64",
        "NULLABLE", "ARRAY", "MAP", "TUPLE", "ENUM8", "ENUM16",
        "TRUE", "FALSE", "COUNT", "SUM", "AVG", "MIN", "MAX",
        "COALESCE", "CAST", "OVER", "PARTITION", "ROW_NUMBER", "RANK",
        "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
        "ENGINE", "MERGETREE", "REPLACINGMERGETREE", "SUMMINGMERGETREE",
        "AGGREGATINGMERGETREE", "COLLAPSINGMERGETREE",
        "LOG", "TINYLOG", "STRIPELOG", "MEMORY",
        "MATERIALIZED", "POPULATE", "SETTINGS", "FORMAT",
        "FINAL", "SAMPLE", "PREWHERE", "GLOBAL",
        "ATTACH", "DETACH", "OPTIMIZE", "MUTATIONS",
        "TTL", "CODEC", "GRANULARITY",
    ]

    private init(config: ConnectionConfig) {
        self.config = config
    }

    deinit {
        let conns = lock.withLock { () -> [ClickHouseConnection] in
            let values = connections
            connections.removeAll()
            return values
        }
        for conn in conns {
            _ = conn.close()
        }
    }

    static func connect(config: ConnectionConfig) async throws -> ClickHouseBackend {
        let backend = ClickHouseBackend(config: config)
        // Validate connectivity
        let conn = try await backend.newConnection(database: config.database)
        backend.lock.withLock { backend.connections.append(conn) }
        return backend
    }

    // MARK: - Connection management

    // ClickHouseNIO does not support concurrent queries on a single connection.
    // Each caller gets its own connection to avoid ChannelError conflicts.
    func connectionFor(database: String) async throws -> ClickHouseConnection {
        let conn = try await newConnection(database: database)
        lock.withLock { connections.append(conn) }
        return conn
    }

    func getAnyConnection() async throws -> ClickHouseConnection {
        try await connectionFor(database: config.database)
    }

    private func newConnection(database: String) async throws -> ClickHouseConnection {
        let port = Int(config.port) ?? 9000
        let db = database.isEmpty ? "default" : database
        let chConfig: ClickHouseConfiguration
        do {
            chConfig = try ClickHouseConfiguration(
                hostname: config.host,
                port: port,
                user: config.user.isEmpty ? "default" : config.user,
                password: config.password,
                database: db
            )
        } catch {
            throw DbError.connection(Self.describeError(error))
        }

        let el = MultiThreadedEventLoopGroup.singleton.any()
        do {
            let conn = try await ClickHouseConnection.connect(
                configuration: chConfig, on: el
            ).get()
            try await conn.ping().get()
            return conn
        } catch {
            throw DbError.connection(Self.describeError(error))
        }
    }

    func quoteIdentifier(_ name: String) -> String {
        let escaped = name.replacingOccurrences(of: "`", with: "``")
        return "`\(escaped)`"
    }

    static func describeError(_ error: any Error) -> String {
        if let ex = error as? ExceptionMessage {
            return ex.displayText
        }
        return error.localizedDescription
    }
}
