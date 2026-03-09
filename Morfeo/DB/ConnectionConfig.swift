import Foundation

enum BackendType: String, Codable, CaseIterable, Sendable {
    case postgres

    var displayName: String {
        switch self {
        case .postgres: "PostgreSQL"
        }
    }

    var iconAsset: String {
        switch self {
        case .postgres: "postgres-logo"
        }
    }
}

struct ConnectionConfig: Sendable {
    let backend: BackendType
    let host: String
    let port: String
    let user: String
    let password: String
    let database: String
}

func morfeoConnect(config: ConnectionConfig) async throws -> any DatabaseBackend {
    switch config.backend {
    case .postgres:
        return try await PostgresBackend.connect(config: config)
    }
}
