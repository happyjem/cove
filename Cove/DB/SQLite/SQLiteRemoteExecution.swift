import Foundation

final class SQLiteRemoteExecution: SQLiteExecution, @unchecked Sendable {
    let isReadOnly = true

    private let remotePath: String
    private let runner: SSHCommandRunner

    private init(remotePath: String, runner: SSHCommandRunner) {
        self.remotePath = remotePath
        self.runner = runner
    }

    deinit {
        let runner = self.runner
        Task {
            await runner.close()
        }
    }

    static func connect(path: String, sshConfig: SSHTunnelConfig) async throws -> SQLiteRemoteExecution {
        let remotePath = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !remotePath.isEmpty else {
            throw DbError.connection("remote SQLite path is required")
        }

        let runner: SSHCommandRunner
        do {
            runner = try await SSHCommandRunner.connect(config: sshConfig)
        } catch {
            throw DbError.connection(error.localizedDescription)
        }

        let execution = SQLiteRemoteExecution(remotePath: remotePath, runner: runner)
        do {
            try await execution.validateConnection()
            return execution
        } catch {
            await runner.close()
            throw error
        }
    }

    func validateConnection() async throws {
        let sqliteCheck = try await runRemote("command -v sqlite3")
        guard sqliteCheck.exitCode == 0 else {
            throw DbError.connection("sqlite3 is not installed or not available in PATH on the remote host")
        }

        let fileCheck = try await runRemote("test -r \(shellQuote(remotePath))")
        guard fileCheck.exitCode == 0 else {
            throw DbError.connection("remote SQLite file is not readable: \(remotePath)")
        }

        _ = try await runRaw("SELECT 1;")
    }

    func query(_ sql: String) async throws -> QueryResult {
        let normalized = leadingKeyword(in: sql)
        switch normalized {
        case "SELECT", "PRAGMA", "WITH", "EXPLAIN":
            return try await runCSV(sql)
        default:
            let result = try await runRaw(sql)
            return QueryResult(columns: [], rows: [], rowsAffected: result.exitCode == 0 ? 0 : nil, totalCount: nil)
        }
    }

    func execute(_ sql: String) async throws -> UInt64? {
        _ = try await runRaw(sql)
        return 0
    }

    func fetchColumnInfo(table: String, quoteIdentifier: (String) -> String) async throws -> [ColumnInfo] {
        let result = try await runCSV("PRAGMA table_info(\(quoteIdentifier(table)))")
        return result.rows.compactMap { row in
            guard row.count >= 6,
                  let name = row[1],
                  let typeName = row[2] else { return nil }
            let isPK = row[5] == "1"
            return ColumnInfo(name: name, typeName: typeName, isPrimaryKey: isPK)
        }
    }

    private func shellQuote(_ value: String) -> String {
        "'\(value.replacingOccurrences(of: "'", with: "'\"'\"'"))'"
    }

    private func runRemote(_ command: String) async throws -> SSHCommandResult {
        do {
            return try await runner.run(command)
        } catch {
            throw DbError.connection(error.localizedDescription)
        }
    }

    private func runRaw(_ sql: String) async throws -> SSHCommandResult {
        let result = try await runRemote("sqlite3 \(shellQuote(remotePath)) \(shellQuote(sql))")
        guard result.exitCode == 0 else {
            let message = result.stderr.isEmpty ? result.stdout : result.stderr
            throw DbError.query(message.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        return result
    }

    private static let nullSentinel = "\\N"

    private func runCSV(_ sql: String) async throws -> QueryResult {
        let result = try await runRemote("sqlite3 -header -csv -nullvalue \(shellQuote(Self.nullSentinel)) \(shellQuote(remotePath)) \(shellQuote(sql))")
        guard result.exitCode == 0 else {
            let message = result.stderr.isEmpty ? result.stdout : result.stderr
            throw DbError.query(message.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        return parseCSVResult(result.stdout)
    }

    private func parseCSVResult(_ csv: String) -> QueryResult {
        let rows = parseCSVRows(csv)
        guard let header = rows.first else {
            return QueryResult(columns: [], rows: [], rowsAffected: nil, totalCount: nil)
        }

        let columns = header.map { ColumnInfo(name: $0 ?? "", typeName: "TEXT", isPrimaryKey: false) }
        return QueryResult(columns: columns, rows: Array(rows.dropFirst()), rowsAffected: nil, totalCount: nil)
    }

    private func parseCSVRows(_ csv: String) -> [[String?]] {
        var rows: [[String?]] = []
        var row: [String?] = []
        var field = ""
        var inQuotes = false
        var index = csv.startIndex

        func finishField() {
            row.append(field == Self.nullSentinel ? nil : field)
            field = ""
        }

        func finishRow() {
            if !row.isEmpty {
                rows.append(row)
                row = []
            }
        }

        while index < csv.endIndex {
            let char = csv[index]
            if inQuotes {
                if char == "\"" {
                    let next = csv.index(after: index)
                    if next < csv.endIndex, csv[next] == "\"" {
                        field.append("\"")
                        index = next
                    } else {
                        inQuotes = false
                    }
                } else {
                    field.append(char)
                }
            } else {
                switch char {
                case "\"":
                    inQuotes = true
                case ",":
                    finishField()
                case "\n":
                    finishField()
                    finishRow()
                case "\r":
                    break
                default:
                    field.append(char)
                }
            }
            index = csv.index(after: index)
        }

        if inQuotes || !field.isEmpty || !row.isEmpty {
            finishField()
            finishRow()
        }

        return rows
    }

    private func leadingKeyword(in sql: String) -> String {
        sql
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .split(whereSeparator: { $0.isWhitespace || $0 == ";" })
            .first
            .map { String($0).uppercased() } ?? ""
    }
}
