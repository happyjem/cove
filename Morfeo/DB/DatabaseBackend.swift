import Foundation

protocol DatabaseBackend: Sendable {
    var name: String { get }
    var syntaxKeywords: Set<String> { get }

    func listChildren(path: [String]) async throws -> [HierarchyNode]

    func fetchTableData(
        path: [String],
        limit: UInt32,
        offset: UInt32,
        sort: (column: String, direction: SortDirection)?
    ) async throws -> QueryResult

    func fetchNodeDetails(path: [String]) async throws -> QueryResult

    func executeQuery(database: String, sql: String) async throws -> QueryResult

    func updateCell(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) async throws

    func generateUpdateSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)],
        column: String,
        newValue: String?
    ) -> String

    func generateInsertSQL(
        tablePath: [String],
        columns: [String],
        values: [String?]
    ) -> String

    func generateDeleteSQL(
        tablePath: [String],
        primaryKey: [(column: String, value: String)]
    ) -> String

    func generateDropElementSQL(path: [String], elementName: String) -> String
}
