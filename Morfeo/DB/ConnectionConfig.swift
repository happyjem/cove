import Foundation

enum BackendType: String, Codable, CaseIterable, Sendable {
    case postgres
    case scylladb
    case redis

    var displayName: String {
        switch self {
        case .postgres: "PostgreSQL"
        case .scylladb: "ScyllaDB"
        case .redis: "Redis"
        }
    }

    var iconAsset: String {
        switch self {
        case .postgres: "postgres-logo"
        case .scylladb: "scylladb-logo"
        case .redis: "redis-logo"
        }
    }

    var defaultPort: String {
        switch self {
        case .postgres: "5432"
        case .scylladb: "9042"
        case .redis: "6379"
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
    case .scylladb:
        return try await ScyllaBackend.connect(config: config)
    case .redis:
        return try await RedisBackend.connect(config: config)
    }
}
