import Foundation

extension SQLiteBackend {
    private static let tintDatabase = NodeTint(r: 0.00, g: 0.45, b: 0.68)
    private static let tintTable    = NodeTint(r: 0.90, g: 0.58, b: 0.15)
    private static let tintView     = NodeTint(r: 0.85, g: 0.75, b: 0.45)
    private static let tintGroup    = NodeTint(r: 0.60, g: 0.60, b: 0.60)
    private static let tintIndex    = NodeTint(r: 0.40, g: 0.69, b: 0.66)
    private static let tintColumn   = NodeTint(r: 0.55, g: 0.66, b: 0.78)
    private static let tintKey      = NodeTint(r: 0.84, g: 0.72, b: 0.39)
    private static let tintTrigger  = NodeTint(r: 0.84, g: 0.49, b: 0.39)

    func isDataBrowsable(path: [String]) -> Bool {
        path.count == 3 && ["Tables", "Views"].contains(path[1])
    }

    func isEditable(path: [String]) -> Bool {
        !isReadOnly && path.count == 3 && path[1] == "Tables"
    }

    func isStructureEditable(path: [String]) -> Bool {
        !isReadOnly && path.count >= 4 && path[1] == "Tables"
            && ["Indexes", "Triggers"].contains(path[3])
    }

    func structurePath(for tablePath: [String]) -> [String]? {
        guard tablePath.count == 3 else { return nil }
        return tablePath + ["Columns"]
    }

    func creatableChildLabel(path: [String]) -> String? {
        guard !isReadOnly else { return nil }
        switch path.count {
        case 2:
            switch path[1] {
            case "Tables": return "Table"
            case "Views": return "View"
            default: return nil
            }
        default:
            return nil
        }
    }

    private static let sqliteColumnTypes = [
        "INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "TEXT", "REAL",
        "BLOB", "NUMERIC", "BOOLEAN", "DATE", "DATETIME",
    ]

    func createFormFields(path: [String]) -> [CreateField] {
        guard !isReadOnly, path.count == 2 else { return [] }
        switch path[1] {
        case "Tables":
            return [
                CreateField(id: "name", label: "Table Name", defaultValue: "", placeholder: "my_table"),
                CreateField(id: "column", label: "Column Name", defaultValue: "id", placeholder: "id"),
                CreateField(id: "type", label: "Column Type", defaultValue: "INTEGER PRIMARY KEY AUTOINCREMENT",
                            placeholder: "INTEGER PRIMARY KEY AUTOINCREMENT", options: Self.sqliteColumnTypes),
            ]
        case "Views":
            return [
                CreateField(id: "name", label: "View Name", defaultValue: "", placeholder: "my_view"),
                CreateField(id: "query", label: "AS Query", defaultValue: "SELECT 1", placeholder: "SELECT ..."),
            ]
        default:
            return []
        }
    }

    func generateCreateChildSQL(path: [String], values: [String: String]) -> String? {
        guard !isReadOnly, path.count == 2 else { return nil }
        let name = values["name", default: ""].trimmingCharacters(in: .whitespaces)
        guard !name.isEmpty else { return nil }
        let q = quoteIdentifier(name)

        switch path[1] {
        case "Tables":
            let col = values["column", default: "id"]
            let type = values["type", default: "INTEGER PRIMARY KEY AUTOINCREMENT"]
            return "CREATE TABLE \(q) (\(quoteIdentifier(col)) \(type))"
        case "Views":
            let query = values["query", default: "SELECT 1"]
            return "CREATE VIEW \(q) AS \(query)"
        default:
            return nil
        }
    }

    func isDeletable(path: [String]) -> Bool {
        !isReadOnly && path.count == 3 && ["Tables", "Views"].contains(path[1])
    }

    func generateDropSQL(path: [String]) -> String? {
        guard !isReadOnly, path.count == 3 else { return nil }
        let q = quoteIdentifier(path[2])
        switch path[1] {
        case "Tables": return "DROP TABLE \(q)"
        case "Views":  return "DROP VIEW \(q)"
        default: return nil
        }
    }

    func listChildren(path: [String]) async throws -> [HierarchyNode] {
        switch path.count {
        case 0:
            return [HierarchyNode(name: "main", icon: "cylinder.split.1x2", tint: Self.tintDatabase, isExpandable: true)]
        case 1:
            return [
                HierarchyNode(name: "Tables", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                HierarchyNode(name: "Views", icon: "folder", tint: Self.tintGroup, isExpandable: true),
            ]
        case 2:
            switch path[1] {
            case "Tables":
                let result = try await runQuery(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
                return result.rows.compactMap { row in
                    guard let name = row.first ?? nil else { return nil }
                    return HierarchyNode(name: name, icon: "tablecells", tint: Self.tintTable, isExpandable: true)
                }
            case "Views":
                let result = try await runQuery("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
                return result.rows.compactMap { row in
                    guard let name = row.first ?? nil else { return nil }
                    return HierarchyNode(name: name, icon: "eye", tint: Self.tintView, isExpandable: true)
                }
            default:
                throw DbError.other("unknown group: \(path[1])")
            }
        case 3:
            switch path[1] {
            case "Tables":
                return [
                    HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Indexes", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                    HierarchyNode(name: "Triggers", icon: "folder", tint: Self.tintGroup, isExpandable: true),
                ]
            case "Views":
                return [HierarchyNode(name: "Columns", icon: "folder", tint: Self.tintGroup, isExpandable: true)]
            default:
                return []
            }
        case 4:
            let table = path[2]
            switch path[3] {
            case "Columns":
                let columns = try await fetchColumnInfo(table: table)
                return columns.map { col in
                    HierarchyNode(
                        name: "\(col.name) : \(col.typeName)",
                        icon: col.isPrimaryKey ? "key.fill" : "circle.fill",
                        tint: col.isPrimaryKey ? Self.tintKey : Self.tintColumn,
                        isExpandable: false
                    )
                }
            case "Indexes":
                let result = try await runQuery(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='\(escapeSQLString(table))' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
                return result.rows.compactMap { row in
                    guard let name = row.first ?? nil else { return nil }
                    return HierarchyNode(name: name, icon: "arrow.up.arrow.down", tint: Self.tintIndex, isExpandable: false)
                }
            case "Triggers":
                let result = try await runQuery(
                    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='\(escapeSQLString(table))' ORDER BY name"
                )
                return result.rows.compactMap { row in
                    guard let name = row.first ?? nil else { return nil }
                    return HierarchyNode(name: name, icon: "bolt.fill", tint: Self.tintTrigger, isExpandable: false)
                }
            default:
                return []
            }
        default:
            return []
        }
    }

    func fetchNodeDetails(path: [String]) async throws -> QueryResult {
        guard path.count >= 1 else {
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        switch path.count {
        case 1, 2:
            return try await runQuery(
                "SELECT name AS Name, type AS Type FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            )
        case 3:
            let infoResult = try await runQuery("PRAGMA table_info(\(quoteIdentifier(path[2])))")
            let columns = [
                ColumnInfo(name: "Column", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "Type", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "Not Null", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "Default", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "PK", typeName: "TEXT", isPrimaryKey: false),
            ]
            let rows = infoResult.rows.compactMap { row -> [String?]? in
                guard row.count >= 6 else { return nil }
                return [row[1], row[2], row[3] == "1" ? "YES" : "NO", row[4], row[5] == "0" ? "" : "YES"]
            }
            return QueryResult(columns: columns, rows: rows, rowsAffected: nil, totalCount: nil)
        case 4:
            return try await subGroupDetail(table: path[2], subGroup: path[3])
        default:
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }
    }

    private func subGroupDetail(table: String, subGroup: String) async throws -> QueryResult {
        switch subGroup {
        case "Columns":
            return try await runQuery("PRAGMA table_info(\(quoteIdentifier(table)))")
        case "Indexes":
            let result = try await runQuery(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='\(escapeSQLString(table))' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            let columns = [
                ColumnInfo(name: "Index", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "Definition", typeName: "TEXT", isPrimaryKey: false),
            ]
            return QueryResult(columns: columns, rows: result.rows, rowsAffected: nil, totalCount: nil)
        case "Triggers":
            let result = try await runQuery(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name='\(escapeSQLString(table))' ORDER BY name"
            )
            let columns = [
                ColumnInfo(name: "Trigger", typeName: "TEXT", isPrimaryKey: false),
                ColumnInfo(name: "Definition", typeName: "TEXT", isPrimaryKey: false),
            ]
            return QueryResult(columns: columns, rows: result.rows, rowsAffected: nil, totalCount: nil)
        default:
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }
    }
}
