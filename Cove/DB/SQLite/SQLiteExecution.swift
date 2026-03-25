import Foundation

protocol SQLiteExecution: Sendable {
    var isReadOnly: Bool { get }

    func validateConnection() async throws
    func query(_ sql: String) async throws -> QueryResult
    func execute(_ sql: String) async throws -> UInt64?
    func fetchColumnInfo(table: String, quoteIdentifier: (String) -> String) async throws -> [ColumnInfo]
}
