import Foundation
import ClickHouseNIO

// Path structure:
// []                                    → databases
// [db]                                  → groups (Tables, Views, …)
// [db, group]                           → items in group
// [db, "Tables", table]                 → sub-groups (Columns, Indexes, Parts)
// [db, "Tables", table, subgrp]         → elements

extension ClickHouseBackend {
    private static let tintDatabase  = NodeTint(r: 0.98, g: 0.82, b: 0.00)
    private static let tintTable     = NodeTint(r: 0.95, g: 0.65, b: 0.15)
    private static let tintView      = NodeTint(r: 0.85, g: 0.75, b: 0.45)
    private static let tintMatView   = NodeTint(r: 0.75, g: 0.82, b: 0.40)
    private static let tintGroup     = NodeTint(r: 0.60, g: 0.60, b: 0.60)
    private static let tintIndex     = NodeTint(r: 0.40, g: 0.69, b: 0.66)
    private static let tintColumn    = NodeTint(r: 0.55, g: 0.66, b: 0.78)
    private static let tintKey       = NodeTint(r: 0.84, g: 0.72, b: 0.39)
    private static let tintDict      = NodeTint(r: 0.69, g: 0.51, b: 0.80)
    private static let tintPart      = NodeTint(r: 0.55, g: 0.75, b: 0.52)

    private static let systemDatabases: Set<String> = [
        "system", "INFORMATION_SCHEMA", "information_schema",
    ]

    // MARK: - Capability queries

    func isDataBrowsable(path: [String]) -> Bool {
        path.count == 3 && ["Tables", "Views", "Materialized Views"].contains(path[1])
    }

    func isEditable(path: [String]) -> Bool {
        path.count == 3 && path[1] == "Tables"
    }

    func isStructureEditable(path: [String]) -> Bool {
        path.count >= 4 && path[1] == "Tables" && path[3] == "Indexes"
    }

    func structurePath(for tablePath: [String]) -> [String]? {
        guard tablePath.count == 3 else { return nil }
        return tablePath + ["Columns"]
    }

    // MARK: - Creation

    func creatableChildLabel(path: [String]) -> String? {
        switch path.count {
        case 0: "Database"
        case 2:
            switch path[1] {
            case "Tables": "Table"
            case "Views": "View"
            default: nil
            }
        default: nil
        }
    }

    private static let clickHouseEngines = [
        "MergeTree", "ReplacingMergeTree", "SummingMergeTree",
        "AggregatingMergeTree", "CollapsingMergeTree",
        "Log", "TinyLog", "StripeLog", "Memory",
    ]

    private static let clickHouseColumnTypes = [
        "UInt64", "UInt32", "Int64", "Int32",
        "Float64", "Float32", "Decimal(18,4)",
        "String", "FixedString(64)",
        "UUID", "Bool",
        "Date", "Date32", "DateTime", "DateTime64(3)",
        "Nullable(String)", "Nullable(UInt64)",
        "Array(String)", "Array(UInt64)",
    ]

    func createFormFields(path: [String]) -> [CreateField] {
        switch path.count {
        case 0:
            return [
                CreateField(id: "name", label: "Name", defaultValue: "", placeholder: "my_database"),
            ]
        case 2:
            switch path[1] {
            case "Tables":
                return [
                    CreateField(id: "name", label: "Table Name", defaultValue: "", placeholder: "my_table"),
                    CreateField(id: "column", label: "Column Name", defaultValue: "id", placeholder: "id"),
                    CreateField(id: "type", label: "Column Type", defaultValue: "UInt64",
                                placeholder: "UInt64", options: Self.clickHouseColumnTypes),
                    CreateField(id: "engine", label: "Engine", defaultValue: "MergeTree",
                                placeholder: "MergeTree", options: Self.clickHouseEngines),
                ]
            case "Views":
                return [
                    CreateField(id: "name", label: "View Name", defaultValue: "", placeholder: "my_view"),
                    CreateField(id: "query", label: "AS Query", defaultValue: "SELECT 1", placeholder: "SELECT ..."),
                ]
            default:
                return []
            }
        default:
            return []
        }
    }

    func generateCreateChildSQL(path: [String], values: [String: String]) -> String? {
        let name = values["name", default: ""].trimmingCharacters(in: .whitespaces)
        guard !name.isEmpty else { return nil }
        let q = quoteIdentifier(name)

        switch path.count {
        case 0:
            return "CREATE DATABASE \(q)"
        case 2:
            let db = quoteIdentifier(path[0])
            switch path[1] {
            case "Tables":
                let col = values["column", default: "id"]
                let type = values["type", default: "UInt64"]
                let engine = values["engine", default: "MergeTree"]
                return "CREATE TABLE \(db).\(q) (\(quoteIdentifier(col)) \(type)) ENGINE = \(engine)() ORDER BY \(quoteIdentifier(col))"
            case "Views":
                let query = values["query", default: "SELECT 1"]
                return "CREATE VIEW \(db).\(q) AS \(query)"
            default:
                return nil
            }
        default:
            return nil
        }
    }

    // MARK: - Deletion

    func isDeletable(path: [String]) -> Bool {
        switch path.count {
        case 1: true
        case 3: ["Tables", "Views", "Materialized Views", "Dictionaries"].contains(path[1])
        default: false
        }
    }

    func generateDropSQL(path: [String]) -> String? {
        switch path.count {
        case 1:
            return "DROP DATABASE \(quoteIdentifier(path[0]))"
        case 3:
            let fqn = "\(quoteIdentifier(path[0])).\(quoteIdentifier(path[2]))"
            switch path[1] {
            case "Tables":             return "DROP TABLE \(fqn)"
            case "Views":              return "DROP VIEW \(fqn)"
            case "Materialized Views": return "DROP TABLE \(fqn)"
            case "Dictionaries":       return "DROP DICTIONARY \(fqn)"
            default: return nil
            }
        default:
            return nil
        }
    }

    // MARK: - Tree navigation

    func listChildren(path: [String]) async throws -> [HierarchyNode] {
        switch path.count {
        case 0:
            let conn = try await getAnyConnection()
            let sql = """
                SELECT name FROM system.databases \
                WHERE name NOT IN ('system','INFORMATION_SCHEMA','information_schema') \
                ORDER BY name
                """
            return try await queryNodeList(
                conn: conn, sql: sql,
                icon: "cylinder.split.1x2", tint: Self.tintDatabase, expandable: true
            )

        case 1:
            return [
                HierarchyNode(name: "Tables", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Views", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Materialized Views", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Dictionaries", icon: "folder", tint: Self.tintGroup, isExpandable: true),
            ]

        case 2:
            let conn = try await connectionFor(database: path[0])
            let db = escapeSQLString(path[0])
            switch path[1] {
            case "Tables":
                return try await queryNodeList(
                    conn: conn,
                    sql: "SELECT name FROM system.tables WHERE database = '\(db)' AND engine NOT IN ('View','MaterializedView') AND NOT is_temporary ORDER BY name",
                    icon: "tablecells", tint: Self.tintTable, expandable: true
                )
            case "Views":
                return try await queryNodeList(
                    conn: conn,
                    sql: "SELECT name FROM system.tables WHERE database = '\(db)' AND engine = 'View' ORDER BY name",
                    icon: "eye", tint: Self.tintView, expandable: true
                )
            case "Materialized Views":
                return try await queryNodeList(
                    conn: conn,
                    sql: "SELECT name FROM system.tables WHERE database = '\(db)' AND engine = 'MaterializedView' ORDER BY name",
                    icon: "eye.fill", tint: Self.tintMatView, expandable: true
                )
            case "Dictionaries":
                return try await queryNodeList(
                    conn: conn,
                    sql: "SELECT name FROM system.dictionaries WHERE database = '\(db)' ORDER BY name",
                    icon: "book.closed", tint: Self.tintDict, expandable: false
                )
            default:
                throw DbError.other("unknown group: \(path[1])")
            }

        case 3:
            switch path[1] {
            case "Tables":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Indexes", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Parts", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            case "Views", "Materialized Views":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            default:
                return []
            }

        case 4:
            let conn = try await connectionFor(database: path[0])
            let db = escapeSQLString(path[0])
            let table = escapeSQLString(path[2])
            switch path[3] {
            case "Columns":
                return try await fetchTreeColumns(conn: conn, database: db, table: table)
            case "Indexes":
                return try await queryNodeList(
                    conn: conn,
                    sql: "SELECT name FROM system.data_skipping_indices WHERE database = '\(db)' AND table = '\(table)' ORDER BY name",
                    icon: "arrow.up.arrow.down", tint: Self.tintIndex, expandable: false
                )
            case "Parts":
                return try await fetchTreeParts(conn: conn, database: db, table: table)
            default:
                return []
            }

        default:
            return []
        }
    }

    // MARK: - Node details

    func fetchNodeDetails(path: [String]) async throws -> QueryResult {
        guard path.count >= 1 else {
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        let conn = try await connectionFor(database: path[0])
        let db = escapeSQLString(path[0])

        let sql: String
        switch path.count {
        case 1, 2:
            sql = databaseDetailSQL(database: db, group: path.count == 2 ? path[1] : nil)
        case 3:
            sql = tableDetailSQL(database: db, table: escapeSQLString(path[2]))
        case 4:
            sql = subGroupDetailSQL(database: db, table: escapeSQLString(path[2]), subGroup: path[3])
        default:
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        return try await runQuery(conn: conn, sql: sql)
    }

    // MARK: - Private helpers

    private func queryNodeList(
        conn: ClickHouseConnection,
        sql: String,
        icon: String,
        tint: NodeTint,
        expandable: Bool
    ) async throws -> [HierarchyNode] {
        let result: ClickHouseQueryResult
        do {
            result = try await conn.query(sql: sql).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }
        guard let col = result.columns.first, let names = col.values as? [String] else { return [] }
        return names.map { HierarchyNode(name: $0, icon: icon, tint: tint, isExpandable: expandable) }
    }

    private func fetchTreeColumns(
        conn: ClickHouseConnection,
        database: String,
        table: String
    ) async throws -> [HierarchyNode] {
        let columns = try await fetchColumnInfo(conn: conn, database: database, table: table)
        return columns.map { col in
            HierarchyNode(
                name: "\(col.name) : \(col.typeName)",
                icon: col.isPrimaryKey ? "key.fill" : "circle.fill",
                tint: col.isPrimaryKey ? Self.tintKey : Self.tintColumn,
                isExpandable: false
            )
        }
    }

    private func fetchTreeParts(
        conn: ClickHouseConnection,
        database: String,
        table: String
    ) async throws -> [HierarchyNode] {
        let sql = """
            SELECT name, rows, formatReadableSize(bytes_on_disk) AS size \
            FROM system.parts \
            WHERE database = '\(database)' AND table = '\(table)' AND active = 1 \
            ORDER BY name
            """
        let result: ClickHouseQueryResult
        do {
            result = try await conn.query(sql: sql).get()
        } catch {
            throw DbError.query(Self.describeError(error))
        }
        let cols = result.columns
        guard cols.count >= 3 else { return [] }
        let names = cols[0].values as? [String] ?? []
        let rowCounts = decodeColumnToStrings(cols[1].values)
        let sizes = cols[2].values as? [String] ?? []

        return zip(names, zip(rowCounts, sizes)).map { name, info in
            HierarchyNode(
                name: "\(name) (\(info.0) rows, \(info.1))",
                icon: "square.stack.3d.up",
                tint: Self.tintPart,
                isExpandable: false
            )
        }
    }

    private func decodeColumnToStrings(_ values: ClickHouseDataTypeArray) -> [String] {
        if let arr = values as? [UInt64] { return arr.map { String($0) } }
        if let arr = values as? [Int64] { return arr.map { String($0) } }
        if let arr = values as? [String] { return arr }
        return Array(repeating: "?", count: values.count)
    }

    private func databaseDetailSQL(database: String, group: String?) -> String {
        guard let group else {
            return """
                SELECT name AS `Name`, \
                engine AS `Engine`, \
                total_rows AS `Rows`, \
                formatReadableSize(total_bytes) AS `Size` \
                FROM system.tables \
                WHERE database = '\(database)' \
                ORDER BY name
                """
        }
        switch group {
        case "Tables":
            return """
                SELECT name AS `Table`, \
                engine AS `Engine`, \
                total_rows AS `Rows`, \
                formatReadableSize(total_bytes) AS `Size`, \
                sorting_key AS `Order By` \
                FROM system.tables \
                WHERE database = '\(database)' AND engine NOT IN ('View','MaterializedView') AND NOT is_temporary \
                ORDER BY name
                """
        case "Views":
            return """
                SELECT name AS `View`, \
                engine AS `Engine`, \
                as_select AS `Query` \
                FROM system.tables \
                WHERE database = '\(database)' AND engine = 'View' \
                ORDER BY name
                """
        case "Materialized Views":
            return """
                SELECT name AS `Materialized View`, \
                engine AS `Engine`, \
                as_select AS `Query` \
                FROM system.tables \
                WHERE database = '\(database)' AND engine = 'MaterializedView' \
                ORDER BY name
                """
        case "Dictionaries":
            return """
                SELECT name AS `Dictionary`, \
                type AS `Type`, \
                status AS `Status`, \
                element_count AS `Elements` \
                FROM system.dictionaries \
                WHERE database = '\(database)' \
                ORDER BY name
                """
        default:
            return "SELECT 1 AS `Info` WHERE 0"
        }
    }

    private func tableDetailSQL(database: String, table: String) -> String {
        """
        SELECT name AS `Table`, \
        engine AS `Engine`, \
        total_rows AS `Rows`, \
        formatReadableSize(total_bytes) AS `Size`, \
        sorting_key AS `Order By`, \
        partition_key AS `Partition Key`, \
        primary_key AS `Primary Key` \
        FROM system.tables \
        WHERE database = '\(database)' AND name = '\(table)'
        """
    }

    private func subGroupDetailSQL(database: String, table: String, subGroup: String) -> String {
        switch subGroup {
        case "Columns":
            return """
                SELECT name AS `Column`, \
                type AS `Type`, \
                default_kind AS `Default Kind`, \
                default_expression AS `Default`, \
                comment AS `Comment` \
                FROM system.columns \
                WHERE database = '\(database)' AND table = '\(table)' \
                ORDER BY position
                """
        case "Indexes":
            return """
                SELECT name AS `Index`, \
                type AS `Type`, \
                expr AS `Expression`, \
                granularity AS `Granularity` \
                FROM system.data_skipping_indices \
                WHERE database = '\(database)' AND table = '\(table)' \
                ORDER BY name
                """
        case "Parts":
            return """
                SELECT name AS `Part`, \
                rows AS `Rows`, \
                formatReadableSize(bytes_on_disk) AS `Size`, \
                modification_time AS `Modified`, \
                min_block_number AS `Min Block`, \
                max_block_number AS `Max Block` \
                FROM system.parts \
                WHERE database = '\(database)' AND table = '\(table)' AND active = 1 \
                ORDER BY name
                """
        default:
            return "SELECT 1 AS `Info` WHERE 0"
        }
    }
}
