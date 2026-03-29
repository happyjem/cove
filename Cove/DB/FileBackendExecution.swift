import Foundation

protocol FileBackendExecution: Sendable {
    var isReadOnly: Bool { get }
    func query(_ sql: String) async throws -> QueryResult
    func execute(_ sql: String) async throws -> UInt64?
}
