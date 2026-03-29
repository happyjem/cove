import Foundation

extension SQLiteBackend {

    func fetchTableData(
        path: [String],
        limit: UInt32,
        offset: UInt32,
        sort: (column: String, direction: SortDirection)?
    ) async throws -> QueryResult {
        guard path.count == 3 else {
            throw DbError.invalidPath(expected: 3, got: path.count)
        }

        let table = path[2]
        let quotedTable = quoteIdentifier(table)
        let columns = try await fetchColumnInfo(table: table)

        var orderClause = ""
        if let sort {
            let dir = sort.direction == .asc ? "ASC" : "DESC"
            orderClause = " ORDER BY \(quoteIdentifier(sort.column)) \(dir)"
        }

        let dataSql = "SELECT * FROM \(quotedTable)\(orderClause) LIMIT \(limit) OFFSET \(offset)"
        let dataResult = try await runQuery(dataSql)

        let countSql = "SELECT COUNT(*) AS count FROM \(quotedTable)"
        let countResult = try await runQuery(countSql)
        let totalCount = countResult.rows.first?.first.flatMap { $0.flatMap(UInt64.init) } ?? 0

        return QueryResult(
            columns: columns,
            rows: dataResult.rows,
            rowsAffected: nil,
            totalCount: totalCount
        )
    }

    func executeQuery(database: String, sql: String) async throws -> QueryResult {
        try await runQuery(sql)
    }

    func updateCell(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) async throws {
        guard tablePath.count == 3 else {
            throw DbError.invalidPath(expected: 3, got: tablePath.count)
        }
        guard !isReadOnly else {
            throw DbError.other("SQLite over SSH is read-only for now")
        }

        let sql = generateUpdateSQL(tablePath: tablePath, primaryKey: primaryKey, column: column, newValue: newValue)
        _ = try await runExec(sql)
    }

    func fetchColumnInfo(table: String) async throws -> [ColumnInfo] {
        let result = try await runQuery("PRAGMA table_info(\(quoteIdentifier(table)))")
        return result.rows.compactMap { row in
            guard row.count >= 6,
                  let name = row[1],
                  let typeName = row[2] else { return nil }
            let isPK = row[5] == "1"
            return ColumnInfo(name: name, typeName: typeName, isPrimaryKey: isPK)
        }
    }

    func fetchCompletionSchema(database: String) async throws -> CompletionSchema {
        let tablesResult = try await runQuery(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )

        var tableMap: [String: [CompletionColumn]] = [:]
        for row in tablesResult.rows {
            guard let tableName = row.first ?? nil else { continue }
            let cols = try await fetchColumnInfo(table: tableName)
            tableMap[tableName] = cols.map { CompletionColumn(name: $0.name, typeName: $0.typeName) }
        }

        let tables: [String: [CompletionTable]] = [
            "main": tableMap.map { CompletionTable(name: $0.key, columns: $0.value) }
                .sorted { $0.name < $1.name }
        ]

        return CompletionSchema(schemas: ["main"], tables: tables, functions: [], types: [])
    }
}
