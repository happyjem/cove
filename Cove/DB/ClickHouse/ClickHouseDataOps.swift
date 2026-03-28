import Foundation
import ClickHouseNIO

extension ClickHouseBackend {

    func fetchTableData(
        path: [String],
        limit: UInt32,
        offset: UInt32,
        sort: (column: String, direction: SortDirection)?
    ) async throws -> QueryResult {
        guard path.count == 3 else {
            throw DbError.invalidPath(expected: 3, got: path.count)
        }

        let conn = try await connectionFor(database: path[0])
        let fqn = fqnFrom(path)

        let columns = try await fetchColumnInfo(conn: conn, database: path[0], table: path[2])

        var orderClause = ""
        if let sort {
            let dir = sort.direction == .asc ? "ASC" : "DESC"
            orderClause = " ORDER BY \(quoteIdentifier(sort.column)) \(dir)"
        }

        let dataSql = "SELECT * FROM \(fqn)\(orderClause) LIMIT \(limit) OFFSET \(offset)"
        let result: ClickHouseQueryResult
        do {
            result = try await conn.query(sql: dataSql).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }
        let (_, rows) = transposeResult(result, columnInfos: columns)

        let countSql = "SELECT count() AS cnt FROM \(fqn)"
        var totalCount: UInt64 = 0
        do {
            let countResult = try await conn.query(sql: countSql).get()
            if let col = countResult.columns.first, col.count > 0 {
                if let arr = col.values as? [UInt64], let v = arr.first { totalCount = v }
                else if let arr = col.values as? [Int64], let v = arr.first { totalCount = UInt64(v) }
            }
        } catch {
            throw DbError.query(Self.describeError(error))
        }

        return QueryResult(
            columns: columns,
            rows: rows,
            rowsAffected: nil,
            totalCount: totalCount
        )
    }

    func executeQuery(database: String, sql: String) async throws -> QueryResult {
        let conn = try await connectionFor(database: database)
        return try await runQuery(conn: conn, sql: sql)
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

        let conn = try await connectionFor(database: tablePath[0])
        let sql = generateUpdateSQL(tablePath: tablePath, primaryKey: primaryKey, column: column, newValue: newValue)
        do {
            try await conn.command(sql: sql).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }
    }

    // MARK: - Shared helpers

    func runQuery(conn: ClickHouseConnection, sql: String) async throws -> QueryResult {
        do {
            let result = try await conn.query(sql: sql).get()
            let (columns, rows) = transposeResult(result)
            if columns.isEmpty {
                return QueryResult(columns: [], rows: [], rowsAffected: 0, totalCount: nil)
            }
            return QueryResult(columns: columns, rows: rows, rowsAffected: nil, totalCount: nil)
        } catch {
            // query() throws when the SQL returns no data (DDL/DML) — fall back to command()
            let desc = String(describing: error)
            if desc.contains("queryDidNotReturnAnyData") {
                do {
                    try await conn.command(sql: sql).get()
                } catch {
                    throw DbError.query(Self.describeError(error))
                }
                return QueryResult(columns: [], rows: [], rowsAffected: 0, totalCount: nil)
            }
            throw DbError.query(Self.describeError(error))
        }
    }

    func fetchCompletionSchema(database: String) async throws -> CompletionSchema {
        let conn = try await connectionFor(database: database)

        let dbSQL = """
            SELECT name FROM system.databases \
            WHERE name NOT IN ('system','INFORMATION_SCHEMA','information_schema') \
            ORDER BY name
            """
        let dbResult: ClickHouseQueryResult
        do {
            dbResult = try await conn.query(sql: dbSQL).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }
        let schemas = (dbResult.columns.first?.values as? [String]) ?? []

        let colSQL = """
            SELECT database, table, name, type \
            FROM system.columns \
            WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') \
            ORDER BY database, table, position
            """
        let colResult: ClickHouseQueryResult
        do {
            colResult = try await conn.query(sql: colSQL).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }

        var tableMap: [String: [String: [CompletionColumn]]] = [:]
        let cols = colResult.columns
        if cols.count >= 4 {
            let databases = cols[0].values as? [String] ?? []
            let tableNames = cols[1].values as? [String] ?? []
            let colNames = cols[2].values as? [String] ?? []
            let colTypes = cols[3].values as? [String] ?? []
            for i in 0..<databases.count {
                tableMap[databases[i], default: [:]][tableNames[i], default: []].append(
                    CompletionColumn(name: colNames[i], typeName: colTypes[i])
                )
            }
        }

        var tables: [String: [CompletionTable]] = [:]
        for (schema, tblMap) in tableMap {
            tables[schema] = tblMap.map { CompletionTable(name: $0.key, columns: $0.value) }
                .sorted { $0.name < $1.name }
        }

        let funcSQL = """
            SELECT DISTINCT name FROM system.functions \
            WHERE origin = 'SQLUserDefined' \
            ORDER BY name
            """
        var functions: [String] = []
        do {
            let funcResult = try await conn.query(sql: funcSQL).get()
            functions = (funcResult.columns.first?.values as? [String]) ?? []
        } catch {
            // system.functions may not have origin column in older versions
        }

        return CompletionSchema(schemas: schemas, tables: tables, functions: functions, types: [])
    }

    func fetchColumnInfo(
        conn: ClickHouseConnection,
        database: String,
        table: String
    ) async throws -> [ColumnInfo] {
        let db = escapeSQLString(database)
        let tbl = escapeSQLString(table)

        // Get sorting key to determine "primary key" columns
        let keySQL = """
            SELECT sorting_key FROM system.tables \
            WHERE database = '\(db)' AND name = '\(tbl)'
            """
        var sortingKeyCols: Set<String> = []
        do {
            let keyResult = try await conn.query(sql: keySQL).get()
            if let col = keyResult.columns.first, let keys = col.values as? [String], let key = keys.first {
                for part in key.split(separator: ",") {
                    sortingKeyCols.insert(part.trimmingCharacters(in: .whitespaces))
                }
            }
        } catch {}

        let sql = """
            SELECT name, type FROM system.columns \
            WHERE database = '\(db)' AND table = '\(tbl)' \
            ORDER BY position
            """
        let result: ClickHouseQueryResult
        do {
            result = try await conn.query(sql: sql).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }

        guard result.columns.count >= 2 else { return [] }
        let names = result.columns[0].values as? [String] ?? []
        let types = result.columns[1].values as? [String] ?? []

        return zip(names, types).map { name, type in
            ColumnInfo(name: name, typeName: type, isPrimaryKey: sortingKeyCols.contains(name))
        }
    }

    func escapeSQLString(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "\\'")
    }
}
