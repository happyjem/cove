import Foundation
import PostgresNIO
import Logging

final class PostgresBackend: DatabaseBackend, @unchecked Sendable {
    let name = "PostgreSQL"
    private let config: ConnectionConfig
    private let lock = NSLock()
    private var clients: [String: PostgresClient] = [:]
    private var runningTasks: [String: Task<Void, any Error>] = [:]

    let syntaxKeywords: Set<String> = [
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "UPDATE", "DELETE", "SET",
        "CREATE", "DROP", "ALTER", "TABLE", "INDEX", "VIEW", "DATABASE", "SCHEMA",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "ON", "AS",
        "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "EXISTS",
        "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "DISTINCT",
        "UNION", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "BEGIN",
        "COMMIT", "ROLLBACK", "TRANSACTION", "VALUES", "DEFAULT", "PRIMARY",
        "KEY", "FOREIGN", "REFERENCES", "CASCADE", "CONSTRAINT", "CHECK",
        "UNIQUE", "ASC", "DESC", "WITH", "RECURSIVE", "RETURNING", "GRANT",
        "REVOKE", "TRUNCATE", "EXPLAIN", "ANALYZE", "VACUUM", "TRIGGER",
        "FUNCTION", "PROCEDURE", "IF", "THEN", "ELSIF", "LOOP", "WHILE",
        "FOR", "FETCH", "CURSOR", "DECLARE", "EXECUTE", "PERFORM",
        "BOOLEAN", "INTEGER", "BIGINT", "SMALLINT", "TEXT", "VARCHAR",
        "CHAR", "NUMERIC", "DECIMAL", "REAL", "FLOAT", "DOUBLE", "DATE",
        "TIME", "TIMESTAMP", "INTERVAL", "UUID", "JSON", "JSONB", "SERIAL",
        "BIGSERIAL", "TRUE", "FALSE", "COUNT", "SUM", "AVG", "MIN", "MAX",
        "COALESCE", "CAST", "OVER", "PARTITION", "ROW_NUMBER", "RANK",
        "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
    ]

    private init(config: ConnectionConfig) {
        self.config = config
    }

    static func connect(config: ConnectionConfig) async throws -> PostgresBackend {
        let backend = PostgresBackend(config: config)

        let client = backend.makeClient(database: config.database)
        backend.addClient(client, key: "__default__")

        let rows = try await client.query("SELECT current_database()")
        var dbName = "postgres"
        for try await row in rows {
            dbName = try row.decode(String.self, context: .default)
        }

        backend.lock.withLock {
            backend.clients[dbName] = client
            if let task = backend.runningTasks.removeValue(forKey: "__default__") {
                backend.runningTasks[dbName] = task
            }
            backend.clients.removeValue(forKey: "__default__")
        }

        return backend
    }

    // MARK: - Node styles

    private static let tintDatabase = NodeTint(r: 0.357, g: 0.608, b: 0.835)
    private static let tintSchema   = NodeTint(r: 0.773, g: 0.525, b: 0.753)
    private static let tintTable    = NodeTint(r: 0.420, g: 0.624, b: 0.800)
    private static let tintView     = NodeTint(r: 0.863, g: 0.863, b: 0.667)
    private static let tintGroup    = NodeTint(r: 0.60, g: 0.60, b: 0.60)
    private static let tintMatView  = NodeTint(r: 0.749, g: 0.824, b: 0.600)
    private static let tintIndex    = NodeTint(r: 0.400, g: 0.694, b: 0.659)
    private static let tintSequence = NodeTint(r: 0.529, g: 0.753, b: 0.518)
    private static let tintFunction = NodeTint(r: 0.694, g: 0.506, b: 0.804)
    private static let tintType     = NodeTint(r: 0.878, g: 0.647, b: 0.412)
    private static let tintColumn   = NodeTint(r: 0.545, g: 0.659, b: 0.780)
    private static let tintKey      = NodeTint(r: 0.835, g: 0.718, b: 0.392)
    private static let tintTrigger  = NodeTint(r: 0.835, g: 0.490, b: 0.392)

    private static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()

    private static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSSXXX"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()

    // MARK: - DatabaseBackend

    func listChildren(path: [String]) async throws -> [HierarchyNode] {
        switch path.count {
        case 0:
            let client = try getAnyClient()
            let rows = try await client.query("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            var nodes: [HierarchyNode] = []
            for try await row in rows {
                let name = try row.decode(String.self, context: .default)
                nodes.append(HierarchyNode(name: name, icon: "cylinder.split.1x2", tint: Self.tintDatabase, isExpandable: true))
            }
            return nodes

        case 1:
            let client = try await clientFor(database: path[0])
            let sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema') ORDER BY schema_name"
            let rows = try await client.query(PostgresQuery(stringLiteral: sql))
            var nodes: [HierarchyNode] = []
            for try await row in rows {
                let name = try row.decode(String.self, context: .default)
                nodes.append(HierarchyNode(name: name, icon: "square.grid.2x2", tint: Self.tintSchema, isExpandable: true))
            }
            return nodes

        case 2:
            return [
                HierarchyNode(name: "Tables", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Views", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Materialized Views", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Sequences", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Functions", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Types", icon: "folder", tint: Self.tintGroup, isExpandable: true),
            ]

        case 3:
            let client = try await clientFor(database: path[0])
            let schema = path[1]
            switch path[2] {
            case "Tables":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT table_name FROM information_schema.tables WHERE table_schema = '\(schema)' AND table_type = 'BASE TABLE' ORDER BY table_name",
                    icon: "tablecells", tint: Self.tintTable, expandable: true
                )
            case "Views":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT table_name FROM information_schema.tables WHERE table_schema = '\(schema)' AND table_type = 'VIEW' ORDER BY table_name",
                    icon: "eye", tint: Self.tintView, expandable: true
                )
            case "Materialized Views":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT matviewname FROM pg_matviews WHERE schemaname = '\(schema)' ORDER BY matviewname",
                    icon: "eye.fill", tint: Self.tintMatView, expandable: true
                )
            case "Sequences":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = '\(schema)' ORDER BY sequence_name",
                    icon: "number", tint: Self.tintSequence, expandable: false
                )
            case "Functions":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '\(schema)' AND p.prokind IN ('f', 'p') ORDER BY 1",
                    icon: "function", tint: Self.tintFunction, expandable: false
                )
            case "Types":
                return try await queryNodeList(
                    client: client,
                    sql: """
                        SELECT t.typname FROM pg_type t \
                        JOIN pg_namespace n ON t.typnamespace = n.oid \
                        WHERE n.nspname = '\(schema)' \
                        AND t.typtype IN ('e', 'c', 'd') \
                        AND NOT EXISTS ( \
                            SELECT 1 FROM pg_class c \
                            WHERE c.reltype = t.oid AND c.relkind IN ('r', 'v', 'm') \
                        ) \
                        ORDER BY t.typname
                        """,
                    icon: "textformat", tint: Self.tintType, expandable: false
                )
            default:
                throw DbError.other("unknown group: \(path[2])")
            }

        case 4:
            switch path[2] {
            case "Tables":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Indexes", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Constraints", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Triggers", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            case "Views":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Triggers", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            case "Materialized Views":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Indexes", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            default:
                return []
            }

        case 5:
            let client = try await clientFor(database: path[0])
            let schema = path[1]
            let relation = path[3]
            switch path[4] {
            case "Columns":
                return try await fetchTreeColumns(client: client, schema: schema, relation: relation)
            case "Indexes":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT indexname FROM pg_indexes WHERE schemaname = '\(schema)' AND tablename = '\(relation)' ORDER BY indexname",
                    icon: "arrow.up.arrow.down", tint: Self.tintIndex, expandable: false
                )
            case "Constraints":
                return try await fetchTreeConstraints(client: client, schema: schema, relation: relation)
            case "Triggers":
                return try await queryNodeList(
                    client: client,
                    sql: "SELECT DISTINCT trigger_name FROM information_schema.triggers WHERE event_object_schema = '\(schema)' AND event_object_table = '\(relation)' ORDER BY trigger_name",
                    icon: "bolt.fill", tint: Self.tintTrigger, expandable: false
                )
            default:
                return []
            }

        default:
            return []
        }
    }

    func fetchTableData(
        path: [String],
        limit: UInt32,
        offset: UInt32,
        sort: (column: String, direction: SortDirection)?
    ) async throws -> QueryResult {
        guard path.count == 4 else {
            throw DbError.invalidPath(expected: 4, got: path.count)
        }

        let client = try await clientFor(database: path[0])
        let schema = path[1]
        let table = path[3]
        let fqn = "\"\(schema)\".\"\(table)\""

        let columns = try await fetchColumnInfo(client: client, schema: schema, table: table)

        var orderClause = ""
        if let sort {
            let dir = sort.direction == .asc ? "ASC" : "DESC"
            orderClause = " ORDER BY \"\(sort.column)\" \(dir)"
        }

        let dataSql = "SELECT * FROM \(fqn)\(orderClause) LIMIT \(limit) OFFSET \(offset)"
        var rows: [[String?]] = []
        do {
            let dataRows = try await client.query(PostgresQuery(stringLiteral: dataSql))
            for try await row in dataRows {
                rows.append(decodeRowCells(row))
            }
        } catch let error as PSQLError {
            throw DbError.query(error.serverMessage)
        }

        let countSql = "SELECT COUNT(*) FROM \(fqn)"
        var totalCount: Int64 = 0
        do {
            let countRows = try await client.query(PostgresQuery(stringLiteral: countSql))
            for try await row in countRows {
                totalCount = try row.decode(Int64.self, context: .default)
            }
        } catch let error as PSQLError {
            throw DbError.query(error.serverMessage)
        }

        return QueryResult(
            columns: columns,
            rows: rows,
            rowsAffected: nil,
            totalCount: UInt64(totalCount)
        )
    }

    func fetchNodeDetails(path: [String]) async throws -> QueryResult {
        guard path.count >= 3 else {
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        let client = try await clientFor(database: path[0])
        let schema = path[1]

        let sql: String
        switch path.count {
        case 3, 4:
            sql = groupDetailSQL(schema: schema, group: path[2])
        case 5, 6:
            sql = subGroupDetailSQL(schema: schema, relation: path[3], subGroup: path[4])
        default:
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        return try await runQuery(client: client, sql: sql)
    }

    func executeQuery(database: String, sql: String) async throws -> QueryResult {
        let client = try await clientFor(database: database)
        return try await runQuery(client: client, sql: sql)
    }

    func generateUpdateSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) -> String {
        let fqn = fqnFrom(tablePath)

        let setClause: String
        if let newValue {
            let escaped = newValue.replacingOccurrences(of: "'", with: "''")
            setClause = "\"\(column)\" = '\(escaped)'"
        } else {
            setClause = "\"\(column)\" = NULL"
        }

        return "UPDATE \(fqn) SET \(setClause) WHERE \(whereClause(primaryKey))"
    }

    func generateInsertSQL(
        tablePath: [String],
        columns: [String],
        values: [String?]
    ) -> String {
        let fqn = fqnFrom(tablePath)
        let colList = columns.map { "\"\($0)\"" }.joined(separator: ", ")
        let valList = values.map { val -> String in
            guard let val else { return "NULL" }
            let escaped = val.replacingOccurrences(of: "'", with: "''")
            return "'\(escaped)'"
        }.joined(separator: ", ")
        return "INSERT INTO \(fqn) (\(colList)) VALUES (\(valList))"
    }

    func generateDeleteSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)]
    ) -> String {
        return "DELETE FROM \(fqnFrom(tablePath)) WHERE \(whereClause(primaryKey))"
    }

    func generateDropElementSQL(path: [String], elementName: String) -> String {
        let schema = path[1]
        let table = path[3]
        let escaped = elementName.replacingOccurrences(of: "\"", with: "\"\"")
        switch path[4] {
        case "Indexes":
            return "DROP INDEX \"\(schema)\".\"\(escaped)\""
        case "Constraints":
            return "ALTER TABLE \"\(schema)\".\"\(table)\" DROP CONSTRAINT \"\(escaped)\""
        case "Triggers":
            return "DROP TRIGGER \"\(escaped)\" ON \"\(schema)\".\"\(table)\""
        default:
            return "-- unsupported element type: \(path[4])"
        }
    }

    private func fqnFrom(_ tablePath: [String]) -> String {
        precondition(tablePath.count >= 4, "tablePath must have at least 4 elements")
        return "\"\(tablePath[1])\".\"\(tablePath[3])\""
    }

    private func whereClause(_ primaryKey: [(column: String, value: String)]) -> String {
        primaryKey.map { pk in
            let escaped = pk.value.replacingOccurrences(of: "'", with: "''")
            return "\"\(pk.column)\" = '\(escaped)'"
        }.joined(separator: " AND ")
    }

    func updateCell(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) async throws {
        guard tablePath.count == 4 else {
            throw DbError.invalidPath(expected: 4, got: tablePath.count)
        }

        let client = try await clientFor(database: tablePath[0])
        let sql = generateUpdateSQL(tablePath: tablePath, primaryKey: primaryKey, column: column, newValue: newValue)
        do {
            _ = try await client.query(PostgresQuery(stringLiteral: sql))
        } catch let error as PSQLError {
            throw DbError.query(error.serverMessage)
        }
    }

    // MARK: - Private

    private func makeClient(database: String) -> PostgresClient {
        let port = Int(config.port) ?? 5432
        let pgConfig = PostgresClient.Configuration(
            host: config.host,
            port: port,
            username: config.user,
            password: config.password.isEmpty ? nil : config.password,
            database: database.isEmpty ? nil : database,
            tls: .disable
        )
        return PostgresClient(configuration: pgConfig)
    }

    private func addClient(_ client: PostgresClient, key: String) {
        lock.withLock {
            clients[key] = client
            if runningTasks[key] == nil {
                let task = Task { try await client.run() }
                runningTasks[key] = task
            }
        }
    }

    private func getAnyClient() throws -> PostgresClient {
        try lock.withLock {
            clients.values.first
        }.orThrow(DbError.connection("no connection available"))
    }

    private func clientFor(database: String) async throws -> PostgresClient {
        let existing: PostgresClient? = lock.withLock { clients[database] }
        if let existing { return existing }

        let client = makeClient(database: database)
        addClient(client, key: database)
        return client
    }

    private func fetchColumnInfo(
        client: PostgresClient,
        schema: String,
        table: String
    ) async throws -> [ColumnInfo] {
        let sql = """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
            COALESCE(( \
                SELECT true FROM pg_constraint pc \
                WHERE pc.conrelid = c.oid \
                AND pc.contype = 'p' \
                AND a.attnum = ANY(pc.conkey) \
            ), false) as is_pk \
            FROM pg_attribute a \
            JOIN pg_class c ON a.attrelid = c.oid \
            JOIN pg_namespace n ON c.relnamespace = n.oid \
            WHERE n.nspname = '\(schema)' AND c.relname = '\(table)' \
            AND a.attnum > 0 AND NOT a.attisdropped \
            ORDER BY a.attnum
            """
        let rows = try await client.query(PostgresQuery(stringLiteral: sql))
        var columns: [ColumnInfo] = []
        for try await row in rows {
            let (name, typeName, isPK) = try row.decode((String, String, Bool).self, context: .default)
            columns.append(ColumnInfo(name: name, typeName: typeName, isPrimaryKey: isPK))
        }
        return columns
    }

    private func queryNodeList(
        client: PostgresClient,
        sql: String,
        icon: String,
        tint: NodeTint,
        expandable: Bool
    ) async throws -> [HierarchyNode] {
        let rows = try await client.query(PostgresQuery(stringLiteral: sql))
        var nodes: [HierarchyNode] = []
        for try await row in rows {
            let name = try row.decode(String.self, context: .default)
            nodes.append(HierarchyNode(name: name, icon: icon, tint: tint, isExpandable: expandable))
        }
        return nodes
    }

    private func fetchTreeColumns(
        client: PostgresClient,
        schema: String,
        relation: String
    ) async throws -> [HierarchyNode] {
        let columns = try await fetchColumnInfo(client: client, schema: schema, table: relation)
        return columns.map { col in
            HierarchyNode(
                name: "\(col.name) : \(col.typeName)",
                icon: col.isPrimaryKey ? "key.fill" : "circle.fill",
                tint: col.isPrimaryKey ? Self.tintKey : Self.tintColumn,
                isExpandable: false
            )
        }
    }

    private func fetchTreeConstraints(
        client: PostgresClient,
        schema: String,
        relation: String
    ) async throws -> [HierarchyNode] {
        let sql = """
            SELECT conname, contype::text \
            FROM pg_constraint c \
            JOIN pg_class t ON c.conrelid = t.oid \
            JOIN pg_namespace n ON t.relnamespace = n.oid \
            WHERE n.nspname = '\(schema)' AND t.relname = '\(relation)' \
            ORDER BY conname
            """
        let rows = try await client.query(PostgresQuery(stringLiteral: sql))
        var nodes: [HierarchyNode] = []
        for try await row in rows {
            let (name, contype) = try row.decode((String, String).self, context: .default)
            let typeLabel = switch contype {
            case "p": "primary key"
            case "f": "foreign key"
            case "u": "unique"
            case "c": "check"
            case "x": "exclusion"
            default: contype
            }
            let icon = switch contype {
            case "p": "key.fill"
            case "f": "link"
            default: "checkmark.circle"
            }
            nodes.append(HierarchyNode(
                name: "\(name) (\(typeLabel))",
                icon: icon,
                tint: Self.tintKey,
                isExpandable: false
            ))
        }
        return nodes
    }

    private func runQuery(client: PostgresClient, sql: String) async throws -> QueryResult {
        let stream: PostgresRowSequence
        do {
            stream = try await client.query(PostgresQuery(stringLiteral: sql))
        } catch let error as PSQLError {
            throw DbError.query(error.serverMessage)
        }

        var columnInfos: [ColumnInfo] = []
        var allRows: [[String?]] = []
        var columnsExtracted = false

        do {
            for try await row in stream {
                if !columnsExtracted {
                    for cell in row {
                        columnInfos.append(ColumnInfo(
                            name: cell.columnName,
                            typeName: String(describing: cell.dataType),
                            isPrimaryKey: false
                        ))
                    }
                    columnsExtracted = true
                }
                allRows.append(decodeRowCells(row))
            }
        } catch let error as PSQLError {
            throw DbError.query(error.serverMessage)
        }

        if columnInfos.isEmpty {
            return QueryResult(columns: [], rows: [], rowsAffected: 0, totalCount: nil)
        }

        return QueryResult(columns: columnInfos, rows: allRows, rowsAffected: nil, totalCount: nil)
    }

    private func groupDetailSQL(schema: String, group: String) -> String {
        switch group {
        case "Tables":
            return """
                SELECT t.tablename AS "Table", \
                pg_size_pretty(pg_total_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))) AS "Size", \
                s.n_live_tup AS "Rows (est.)", \
                obj_description((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass) AS "Comment" \
                FROM pg_tables t \
                LEFT JOIN pg_stat_user_tables s ON t.schemaname = s.schemaname AND t.tablename = s.relname \
                WHERE t.schemaname = '\(schema)' \
                ORDER BY t.tablename
                """
        case "Views":
            return """
                SELECT v.viewname AS "View", \
                obj_description((quote_ident(v.schemaname) || '.' || quote_ident(v.viewname))::regclass) AS "Comment" \
                FROM pg_views v \
                WHERE v.schemaname = '\(schema)' \
                ORDER BY v.viewname
                """
        case "Materialized Views":
            return """
                SELECT m.matviewname AS "Materialized View", \
                pg_size_pretty(pg_total_relation_size(quote_ident(m.schemaname) || '.' || quote_ident(m.matviewname))) AS "Size", \
                obj_description((quote_ident(m.schemaname) || '.' || quote_ident(m.matviewname))::regclass) AS "Comment" \
                FROM pg_matviews m \
                WHERE m.schemaname = '\(schema)' \
                ORDER BY m.matviewname
                """
        case "Sequences":
            return """
                SELECT s.sequencename AS "Sequence", \
                s.data_type AS "Type", \
                s.start_value AS "Start", \
                s.min_value AS "Min", \
                s.max_value AS "Max", \
                s.increment_by AS "Increment" \
                FROM pg_sequences s \
                WHERE s.schemaname = '\(schema)' \
                ORDER BY s.sequencename
                """
        case "Functions":
            return """
                SELECT p.proname AS "Name", \
                pg_get_function_identity_arguments(p.oid) AS "Arguments", \
                pg_get_function_result(p.oid) AS "Returns", \
                l.lanname AS "Language", \
                CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' END AS "Kind" \
                FROM pg_proc p \
                JOIN pg_namespace n ON p.pronamespace = n.oid \
                JOIN pg_language l ON p.prolang = l.oid \
                WHERE n.nspname = '\(schema)' AND p.prokind IN ('f', 'p') \
                ORDER BY p.proname
                """
        case "Types":
            return """
                SELECT t.typname AS "Name", \
                CASE t.typtype WHEN 'e' THEN 'enum' WHEN 'c' THEN 'composite' WHEN 'd' THEN 'domain' END AS "Kind", \
                CASE \
                    WHEN t.typtype = 'e' THEN ( \
                        SELECT string_agg(e.enumlabel, ', ' ORDER BY e.enumsortorder) \
                        FROM pg_enum e WHERE e.enumtypid = t.oid \
                    ) \
                    WHEN t.typtype = 'd' THEN pg_catalog.format_type(t.typbasetype, t.typtypmod) \
                    ELSE NULL \
                END AS "Details" \
                FROM pg_type t \
                JOIN pg_namespace n ON t.typnamespace = n.oid \
                WHERE n.nspname = '\(schema)' \
                AND t.typtype IN ('e', 'c', 'd') \
                AND NOT EXISTS ( \
                    SELECT 1 FROM pg_class c \
                    WHERE c.reltype = t.oid AND c.relkind IN ('r', 'v', 'm') \
                ) \
                ORDER BY t.typname
                """
        default:
            return "SELECT 1 AS \"Info\" WHERE false"
        }
    }

    private func subGroupDetailSQL(schema: String, relation: String, subGroup: String) -> String {
        switch subGroup {
        case "Columns":
            return """
                SELECT a.attname AS "Column", \
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Type", \
                CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS "Nullable", \
                pg_get_expr(d.adbin, d.adrelid) AS "Default", \
                COALESCE((SELECT 'YES' FROM pg_constraint pc \
                    WHERE pc.conrelid = c.oid AND pc.contype = 'p' \
                    AND a.attnum = ANY(pc.conkey)), 'NO') AS "PK" \
                FROM pg_attribute a \
                JOIN pg_class c ON a.attrelid = c.oid \
                JOIN pg_namespace n ON c.relnamespace = n.oid \
                LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum \
                WHERE n.nspname = '\(schema)' AND c.relname = '\(relation)' \
                AND a.attnum > 0 AND NOT a.attisdropped \
                ORDER BY a.attnum
                """
        case "Indexes":
            return """
                SELECT i.indexname AS "Index", \
                CASE WHEN ix.indisunique THEN 'YES' ELSE 'NO' END AS "Unique", \
                CASE WHEN ix.indisprimary THEN 'YES' ELSE 'NO' END AS "Primary", \
                am.amname AS "Method", \
                pg_get_indexdef(ix.indexrelid) AS "Definition" \
                FROM pg_indexes i \
                JOIN pg_class ci ON ci.relname = i.indexname \
                JOIN pg_namespace ni ON ci.relnamespace = ni.oid AND ni.nspname = i.schemaname \
                JOIN pg_index ix ON ix.indexrelid = ci.oid \
                JOIN pg_am am ON ci.relam = am.oid \
                WHERE i.schemaname = '\(schema)' AND i.tablename = '\(relation)' \
                ORDER BY i.indexname
                """
        case "Constraints":
            return """
                SELECT con.conname AS "Constraint", \
                CASE con.contype \
                    WHEN 'p' THEN 'PRIMARY KEY' \
                    WHEN 'f' THEN 'FOREIGN KEY' \
                    WHEN 'u' THEN 'UNIQUE' \
                    WHEN 'c' THEN 'CHECK' \
                    WHEN 'x' THEN 'EXCLUSION' \
                END AS "Type", \
                pg_get_constraintdef(con.oid) AS "Definition" \
                FROM pg_constraint con \
                JOIN pg_class t ON con.conrelid = t.oid \
                JOIN pg_namespace n ON t.relnamespace = n.oid \
                WHERE n.nspname = '\(schema)' AND t.relname = '\(relation)' \
                ORDER BY con.conname
                """
        case "Triggers":
            return """
                SELECT trigger_name AS "Trigger", \
                action_timing AS "Timing", \
                event_manipulation AS "Event", \
                action_statement AS "Action" \
                FROM information_schema.triggers \
                WHERE event_object_schema = '\(schema)' \
                AND event_object_table = '\(relation)' \
                ORDER BY trigger_name, event_manipulation
                """
        default:
            return "SELECT 1 AS \"Info\" WHERE false"
        }
    }

    private func decodeRowCells(_ row: PostgresRow) -> [String?] {
        row.map { cell in
            if cell.bytes == nil { return nil }
            return decodeCell(cell)
        }
    }

    private func decodeCell(_ cell: PostgresCell) -> String {
        do {
            switch cell.dataType {
            // Boolean
            case .bool:
                return String(try cell.decode(Bool.self))

            // Integers
            case .int2:
                return String(try cell.decode(Int16.self))
            case .int4, .oid:
                return String(try cell.decode(Int32.self))
            case .int8:
                return String(try cell.decode(Int64.self))

            // Floating point
            case .float4:
                return String(try cell.decode(Float.self))
            case .float8:
                return String(try cell.decode(Double.self))

            // Numeric (binary format: ndigits, weight, sign, dscale, digit groups)
            case .numeric:
                return decodeNumeric(cell) ?? "[numeric]"

            // UUID
            case .uuid:
                return try cell.decode(UUID.self).uuidString

            // Date/Time (binary formats, String decode gives garbled output)
            case .date:
                if let date = try? cell.decode(Date.self) {
                    return Self.dateFormatter.string(from: date)
                }
                return "[date]"
            case .timestamp, .timestamptz:
                if let date = try? cell.decode(Date.self) {
                    return Self.timestampFormatter.string(from: date)
                }
                return "[timestamp]"
            case .time:
                return decodeTime(cell) ?? "[time]"
            case .timetz:
                return decodeTimeTz(cell) ?? "[timetz]"
            case .interval:
                return decodeInterval(cell) ?? "[interval]"

            // JSON
            case .jsonb:
                return decodeJsonb(cell) ?? "[jsonb]"

            // Binary data
            case .bytea:
                return decodeBytea(cell)

            // Money (Int64 cents)
            case .money:
                return decodeMoney(cell) ?? "[money]"

            // Network types (binary format)
            case .inet, .cidr:
                return decodeInet(cell) ?? "[inet]"
            case .macaddr:
                return decodeMacaddr(cell) ?? "[macaddr]"

            // Text-safe types + everything else
            default:
                return try cell.decode(String.self)
            }
        } catch {
            return "[\(cell.dataType)]"
        }
    }

    // MARK: - Binary type decoders

    private func decodeNumeric(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes else { return nil }
        guard let ndigits = buf.readInteger(as: UInt16.self),
              let weight = buf.readInteger(as: Int16.self),
              let sign = buf.readInteger(as: UInt16.self),
              let dscale = buf.readInteger(as: UInt16.self) else { return nil }

        if sign == 0xC000 { return "NaN" }
        if ndigits == 0 {
            return dscale > 0 ? "0." + String(repeating: "0", count: Int(dscale)) : "0"
        }

        var groups: [UInt16] = []
        for _ in 0..<ndigits {
            guard let d = buf.readInteger(as: UInt16.self) else { return nil }
            groups.append(d)
        }

        let prefix = sign == 0x4000 ? "-" : ""
        let intGroups = Int(weight) + 1

        var intStr = ""
        if intGroups > 0 {
            for i in 0..<intGroups {
                let d = i < groups.count ? groups[i] : 0
                intStr += i == 0 ? "\(d)" : String(format: "%04d", d)
            }
        } else {
            intStr = "0"
        }

        if dscale == 0 { return prefix + intStr }

        var fracStr = ""
        if weight < -1 {
            fracStr += String(repeating: "0", count: (-Int(weight) - 1) * 4)
        }
        let fracStart = max(intGroups, 0)
        for i in fracStart..<groups.count {
            fracStr += String(format: "%04d", groups[i])
        }
        if fracStr.count > Int(dscale) {
            fracStr = String(fracStr.prefix(Int(dscale)))
        } else {
            fracStr += String(repeating: "0", count: Int(dscale) - fracStr.count)
        }

        return prefix + intStr + "." + fracStr
    }

    private func decodeTime(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes,
              let us = buf.readInteger(as: Int64.self) else { return nil }
        return formatMicroseconds(us)
    }

    private func decodeTimeTz(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes,
              let us = buf.readInteger(as: Int64.self),
              let tzOffset = buf.readInteger(as: Int32.self) else { return nil }
        let time = formatMicroseconds(us)
        let offsetSec = -Int(tzOffset)
        let sign = offsetSec >= 0 ? "+" : "-"
        let abs = abs(offsetSec)
        return "\(time)\(sign)\(String(format: "%02d:%02d", abs / 3600, abs % 3600 / 60))"
    }

    private func decodeInterval(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes,
              let us = buf.readInteger(as: Int64.self),
              let days = buf.readInteger(as: Int32.self),
              let months = buf.readInteger(as: Int32.self) else { return nil }

        var parts: [String] = []
        let years = months / 12
        let mons = months % 12
        if years != 0 { parts.append("\(years) year\(abs(years) == 1 ? "" : "s")") }
        if mons != 0 { parts.append("\(mons) mon\(abs(mons) == 1 ? "" : "s")") }
        if days != 0 { parts.append("\(days) day\(abs(days) == 1 ? "" : "s")") }
        if us != 0 || parts.isEmpty {
            let sign = us < 0 ? "-" : ""
            parts.append(sign + formatMicroseconds(Swift.abs(us)))
        }
        return parts.joined(separator: " ")
    }

    private func formatMicroseconds(_ us: Int64) -> String {
        let totalSec = us / 1_000_000
        let h = totalSec / 3600
        let m = (totalSec % 3600) / 60
        let s = totalSec % 60
        let frac = us % 1_000_000
        let base = String(format: "%02d:%02d:%02d", h, m, s)
        if frac == 0 { return base }
        let fracStr = String(format: "%06d", frac)
            .replacingOccurrences(of: "0+$", with: "", options: .regularExpression)
        return base + "." + fracStr
    }

    private func decodeJsonb(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes, buf.readableBytes > 1 else { return nil }
        buf.moveReaderIndex(forwardBy: 1)
        return buf.readString(length: buf.readableBytes)
    }

    private func decodeBytea(_ cell: PostgresCell) -> String {
        guard let buf = cell.bytes else { return "" }
        return "\\x" + buf.readableBytesView.map { String(format: "%02x", $0) }.joined()
    }

    private func decodeMoney(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes,
              let cents = buf.readInteger(as: Int64.self) else { return nil }
        let sign = cents < 0 ? "-" : ""
        let abs = Swift.abs(cents)
        return "\(sign)\(abs / 100).\(String(format: "%02d", abs % 100))"
    }

    private func decodeInet(_ cell: PostgresCell) -> String? {
        guard var buf = cell.bytes,
              let family = buf.readInteger(as: UInt8.self),
              let mask = buf.readInteger(as: UInt8.self),
              let isCidr = buf.readInteger(as: UInt8.self),
              let addrLen = buf.readInteger(as: UInt8.self) else { return nil }

        if family == 2, addrLen == 4 {
            guard let a = buf.readInteger(as: UInt8.self),
                  let b = buf.readInteger(as: UInt8.self),
                  let c = buf.readInteger(as: UInt8.self),
                  let d = buf.readInteger(as: UInt8.self) else { return nil }
            let addr = "\(a).\(b).\(c).\(d)"
            return (isCidr == 1 || mask < 32) ? "\(addr)/\(mask)" : addr
        }
        if family == 3, addrLen == 16 {
            var groups: [String] = []
            for _ in 0..<8 {
                guard let g = buf.readInteger(as: UInt16.self) else { return nil }
                groups.append(String(format: "%x", g))
            }
            let addr = groups.joined(separator: ":")
            return (isCidr == 1 || mask < 128) ? "\(addr)/\(mask)" : addr
        }
        return nil
    }

    private func decodeMacaddr(_ cell: PostgresCell) -> String? {
        guard let buf = cell.bytes, buf.readableBytes == 6 else { return nil }
        return buf.readableBytesView.map { String(format: "%02x", $0) }.joined(separator: ":")
    }
}

// MARK: - Helpers

extension PSQLError {
    var serverMessage: String {
        if let msg = serverInfo?[.message] {
            return msg
        }
        return String(reflecting: self)
    }
}

extension Optional {
    func orThrow(_ error: @autoclosure () -> some Error) throws -> Wrapped {
        guard let self else { throw error() }
        return self
    }
}
