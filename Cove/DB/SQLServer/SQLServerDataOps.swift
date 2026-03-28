import CosmoMSSQL
import CosmoSQLCore

extension SQLServerBackend {

    func fetchTableData(
        path: [String],
        limit: UInt32,
        offset: UInt32,
        sort: (column: String, direction: SortDirection)?
    ) async throws -> QueryResult {
        guard path.count == 4 else {
            throw DbError.invalidPath(expected: 4, got: path.count)
        }

        let pool = poolFor(database: path[0])
        let schema = path[1]
        let table = path[3]
        let fqn = "\(quoteIdentifier(schema)).\(quoteIdentifier(table))"

        let columns = try await fetchColumnInfo(pool: pool, schema: schema, table: table)

        var orderClause = ""
        if let sort {
            let dir = sort.direction == .asc ? "ASC" : "DESC"
            orderClause = " ORDER BY \(quoteIdentifier(sort.column)) \(dir)"
        } else if let pkCol = columns.first(where: { $0.isPrimaryKey }) {
            orderClause = " ORDER BY \(quoteIdentifier(pkCol.name)) ASC"
        } else if let firstCol = columns.first {
            orderClause = " ORDER BY \(quoteIdentifier(firstCol.name)) ASC"
        }

        let dataSql = "SELECT * FROM \(fqn)\(orderClause) OFFSET \(offset) ROWS FETCH NEXT \(limit) ROWS ONLY"
        var rows: [[String?]] = []
        do {
            let dataRows = try await pool.query(dataSql, [])
            for row in dataRows {
                rows.append(decodeRowValues(row))
            }
        } catch let error as SQLError {
            throw DbError.query(error.description)
        }

        let countSql = "SELECT COUNT(*) FROM \(fqn)"
        var totalCount: Int64 = 0
        do {
            let countRows = try await pool.query(countSql, [])
            if let first = countRows.first {
                totalCount = Int64(first.values[0].asInt() ?? 0)
            }
        } catch let error as SQLError {
            throw DbError.query(error.description)
        }

        return QueryResult(
            columns: columns,
            rows: rows,
            rowsAffected: nil,
            totalCount: UInt64(totalCount)
        )
    }

    func executeQuery(database: String, sql: String) async throws -> QueryResult {
        let pool = poolFor(database: database.isEmpty ? "master" : database)
        return try await runQuery(pool: pool, sql: sql)
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

        let pool = poolFor(database: tablePath[0])
        let sql = generateUpdateSQL(
            tablePath: tablePath, primaryKey: primaryKey,
            column: column, newValue: newValue
        )
        do {
            _ = try await pool.execute(sql, [])
        } catch let error as SQLError {
            throw DbError.query(error.description)
        } catch let error as DbError {
            throw error
        } catch {
            throw DbError.query(error.localizedDescription)
        }
    }

    // MARK: - Shared helpers

    func runQuery(pool: MSSQLConnectionPool, sql: String) async throws -> QueryResult {
        let rows: [SQLRow]
        do {
            rows = try await pool.query(sql, [])
        } catch let error as SQLError {
            throw DbError.query(error.description)
        } catch let error as DbError {
            throw error
        } catch {
            throw DbError.query(error.localizedDescription)
        }

        guard let first = rows.first else {
            return QueryResult(columns: [], rows: [], rowsAffected: 0, totalCount: nil)
        }

        let columnInfos = first.columns.map {
            ColumnInfo(name: $0.name, typeName: String(describing: $0.dataTypeID ?? 0), isPrimaryKey: false)
        }

        let allRows = rows.map { decodeRowValues($0) }

        return QueryResult(columns: columnInfos, rows: allRows, rowsAffected: nil, totalCount: nil)
    }

    func fetchCompletionSchema(database: String) async throws -> CompletionSchema {
        let pool = poolFor(database: database.isEmpty ? "master" : database)

        let schemaSQL = """
            SELECT s.name FROM sys.schemas s \
            WHERE s.schema_id < 16384 \
            AND s.name NOT IN ('guest','INFORMATION_SCHEMA','sys', \
            'db_owner','db_accessadmin','db_securityadmin','db_ddladmin', \
            'db_backupoperator','db_datareader','db_datawriter', \
            'db_denydatareader','db_denydatawriter') \
            ORDER BY s.name
            """
        let schemaRows = try await pool.query(schemaSQL, [])
        let schemas = schemaRows.compactMap { $0.values[0].asString() }

        let colSQL = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE \
            FROM INFORMATION_SCHEMA.COLUMNS \
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
        let colRows = try await pool.query(colSQL, [])
        var tableMap: [String: [String: [CompletionColumn]]] = [:]
        for row in colRows {
            guard let schema = row["TABLE_SCHEMA"].asString(),
                  let tableName = row["TABLE_NAME"].asString(),
                  let colName = row["COLUMN_NAME"].asString(),
                  let dataType = row["DATA_TYPE"].asString() else { continue }
            tableMap[schema, default: [:]][tableName, default: []].append(
                CompletionColumn(name: colName, typeName: dataType)
            )
        }

        var tables: [String: [CompletionTable]] = [:]
        for (schema, tblMap) in tableMap {
            tables[schema] = tblMap.map { CompletionTable(name: $0.key, columns: $0.value) }
                .sorted { $0.name < $1.name }
        }

        let funcSQL = """
            SELECT DISTINCT name FROM sys.objects \
            WHERE type IN ('FN','IF','TF','P') \
            AND schema_id IN (SELECT schema_id FROM sys.schemas WHERE name NOT IN ('sys','INFORMATION_SCHEMA')) \
            ORDER BY name
            """
        let funcRows = try await pool.query(funcSQL, [])
        let functions = funcRows.compactMap { $0.values[0].asString() }

        let typeSQL = """
            SELECT name FROM sys.types WHERE is_user_defined = 1 ORDER BY name
            """
        let typeRows = try await pool.query(typeSQL, [])
        let types = typeRows.compactMap { $0.values[0].asString() }

        return CompletionSchema(schemas: schemas, tables: tables, functions: functions, types: types)
    }

    func fetchColumnInfo(
        pool: MSSQLConnectionPool,
        schema: String,
        table: String
    ) async throws -> [ColumnInfo] {
        let sql = """
            SELECT c.COLUMN_NAME, \
            c.DATA_TYPE + \
            CASE \
                WHEN c.CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + \
                    CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' \
                    ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' \
                WHEN c.NUMERIC_PRECISION IS NOT NULL THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + \
                    CASE WHEN c.NUMERIC_SCALE > 0 THEN ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) ELSE '' END + ')' \
                ELSE '' \
            END AS full_type, \
            CASE WHEN EXISTS ( \
                SELECT 1 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA \
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
                AND kcu.TABLE_SCHEMA = '\(schema)' AND kcu.TABLE_NAME = '\(table)' \
                AND kcu.COLUMN_NAME = c.COLUMN_NAME \
            ) THEN 1 ELSE 0 END AS is_pk \
            FROM INFORMATION_SCHEMA.COLUMNS c \
            WHERE c.TABLE_SCHEMA = '\(schema)' AND c.TABLE_NAME = '\(table)' \
            ORDER BY c.ORDINAL_POSITION
            """
        let rows = try await pool.query(sql, [])
        return rows.compactMap { row in
            guard let name = row["COLUMN_NAME"].asString(),
                  let typeName = row["full_type"].asString() else { return nil }
            let isPK = row["is_pk"].asInt() == 1
            return ColumnInfo(name: name, typeName: typeName, isPrimaryKey: isPK)
        }
    }
}
