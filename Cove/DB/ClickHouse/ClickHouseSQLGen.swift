import Foundation

extension ClickHouseBackend {

    func generateUpdateSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) -> String {
        let fqn = fqnFrom(tablePath)

        let setClause: String
        if let newValue {
            let escaped = newValue.replacingOccurrences(of: "'", with: "\\'")
            setClause = "\(quoteIdentifier(column)) = '\(escaped)'"
        } else {
            setClause = "\(quoteIdentifier(column)) = NULL"
        }

        return "ALTER TABLE \(fqn) UPDATE \(setClause) WHERE \(whereClause(primaryKey))"
    }

    func generateInsertSQL(
        tablePath: [String],
        columns: [String],
        values: [String?]
    ) -> String {
        let fqn = fqnFrom(tablePath)
        let colList = columns.map { quoteIdentifier($0) }.joined(separator: ", ")
        let valList = values.map { val -> String in
            guard let val else { return "NULL" }
            let escaped = val.replacingOccurrences(of: "'", with: "\\'")
            return "'\(escaped)'"
        }.joined(separator: ", ")
        return "INSERT INTO \(fqn) (\(colList)) VALUES (\(valList))"
    }

    func generateDeleteSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)]
    ) -> String {
        return "ALTER TABLE \(fqnFrom(tablePath)) DELETE WHERE \(whereClause(primaryKey))"
    }

    func generateDropElementSQL(path: [String], elementName: String) -> String {
        let db = path[0]
        let table = path[2]
        let fqn = "\(quoteIdentifier(db)).\(quoteIdentifier(table))"
        let escaped = elementName.replacingOccurrences(of: "`", with: "``")
        switch path[3] {
        case "Indexes":
            return "ALTER TABLE \(fqn) DROP INDEX `\(escaped)`"
        default:
            return "-- unsupported element type: \(path[3])"
        }
    }

    // MARK: - Private helpers

    func fqnFrom(_ tablePath: [String]) -> String {
        guard tablePath.count >= 3 else { return "-- invalid path" }
        return "\(quoteIdentifier(tablePath[0])).\(quoteIdentifier(tablePath[2]))"
    }

    private func whereClause(_ primaryKey: [(column: String, value: String)]) -> String {
        primaryKey.map { pk in
            let escaped = pk.value.replacingOccurrences(of: "'", with: "\\'")
            return "\(quoteIdentifier(pk.column)) = '\(escaped)'"
        }.joined(separator: " AND ")
    }
}
