import Foundation

extension SQLiteBackend {

    func generateUpdateSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) -> String {
        guard !isReadOnly else {
            return "-- SQLite over SSH is read-only for now"
        }

        let table = quoteIdentifier(tablePath[2])

        let setClause: String
        if let newValue {
            let escaped = newValue.replacingOccurrences(of: "'", with: "''")
            setClause = "\(quoteIdentifier(column)) = '\(escaped)'"
        } else {
            setClause = "\(quoteIdentifier(column)) = NULL"
        }

        return "UPDATE \(table) SET \(setClause) WHERE \(whereClause(primaryKey))"
    }

    func generateInsertSQL(
        tablePath: [String],
        columns: [String],
        values: [String?]
    ) -> String {
        guard !isReadOnly else {
            return "-- SQLite over SSH is read-only for now"
        }

        let table = quoteIdentifier(tablePath[2])
        let colList = columns.map { quoteIdentifier($0) }.joined(separator: ", ")
        let valList = values.map { val -> String in
            guard let val else { return "NULL" }
            let escaped = val.replacingOccurrences(of: "'", with: "''")
            return "'\(escaped)'"
        }.joined(separator: ", ")
        return "INSERT INTO \(table) (\(colList)) VALUES (\(valList))"
    }

    func generateDeleteSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)]
    ) -> String {
        guard !isReadOnly else {
            return "-- SQLite over SSH is read-only for now"
        }
        return "DELETE FROM \(quoteIdentifier(tablePath[2])) WHERE \(whereClause(primaryKey))"
    }

    func generateDropElementSQL(path: [String], elementName: String) -> String {
        guard !isReadOnly else {
            return "-- SQLite over SSH is read-only for now"
        }

        let escaped = elementName.replacingOccurrences(of: "\"", with: "\"\"")
        switch path[3] {
        case "Indexes":
            return "DROP INDEX \"\(escaped)\""
        case "Triggers":
            return "DROP TRIGGER \"\(escaped)\""
        default:
            return "-- unsupported element type: \(path[3])"
        }
    }

    private func whereClause(_ primaryKey: [(column: String, value: String)]) -> String {
        primaryKey.map { pk in
            let escaped = pk.value.replacingOccurrences(of: "'", with: "''")
            return "\(quoteIdentifier(pk.column)) = '\(escaped)'"
        }.joined(separator: " AND ")
    }
}
