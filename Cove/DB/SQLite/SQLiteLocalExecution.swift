import Foundation
import SQLite3
import Synchronization

final class SQLiteLocalExecution: SQLiteExecution, @unchecked Sendable {
    let isReadOnly = false

    private let handle: Mutex<OpaquePointer?>

    private init(handle: consuming Mutex<OpaquePointer?>) {
        self.handle = handle
    }

    deinit {
        handle.withLock { db in
            if let db {
                sqlite3_close_v2(db)
            }
        }
    }

    static func connect(path: String) throws -> SQLiteLocalExecution {
        var db: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(path, &db, flags, nil)
        guard rc == SQLITE_OK, let db else {
            let msg = db.map { String(cString: sqlite3_errmsg($0)) } ?? "unknown error"
            if let db { sqlite3_close_v2(db) }
            throw DbError.connection("failed to open \(path): \(msg)")
        }

        let execution = SQLiteLocalExecution(handle: Mutex(db))
        try execution.execSQL("PRAGMA journal_mode=WAL")
        try execution.execSQL("PRAGMA foreign_keys=ON")
        return execution
    }

    func validateConnection() async throws {
        _ = try runSQL("SELECT 1")
    }

    func query(_ sql: String) async throws -> QueryResult {
        try runSQL(sql)
    }

    func execute(_ sql: String) async throws -> UInt64? {
        try execSQL(sql)
        return 0
    }

    func fetchColumnInfo(table: String, quoteIdentifier: (String) -> String) async throws -> [ColumnInfo] {
        let result = try runSQL("PRAGMA table_info(\(quoteIdentifier(table)))")
        return result.rows.compactMap { row in
            guard row.count >= 6,
                  let name = row[1],
                  let typeName = row[2] else { return nil }
            let isPK = row[5] == "1"
            return ColumnInfo(name: name, typeName: typeName, isPrimaryKey: isPK)
        }
    }

    private func runSQL(_ sql: String) throws -> QueryResult {
        try handle.withLock { db in
            guard let db else { throw DbError.connection("database closed") }

            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
                throw DbError.query(String(cString: sqlite3_errmsg(db)))
            }
            defer { sqlite3_finalize(stmt) }

            let colCount = sqlite3_column_count(stmt)
            guard colCount > 0 else {
                guard sqlite3_step(stmt) == SQLITE_DONE else {
                    throw DbError.query(String(cString: sqlite3_errmsg(db)))
                }
                let affected = UInt64(sqlite3_changes(db))
                return QueryResult(columns: [], rows: [], rowsAffected: affected, totalCount: nil)
            }

            var columns: [ColumnInfo] = []
            for i in 0..<colCount {
                let name = sqlite3_column_name(stmt, i).map { String(cString: $0) } ?? "?"
                let typeName = sqlite3_column_decltype(stmt, i).map { String(cString: $0) } ?? ""
                columns.append(ColumnInfo(name: name, typeName: typeName, isPrimaryKey: false))
            }

            var rows: [[String?]] = []
            while true {
                let stepResult = sqlite3_step(stmt)
                if stepResult == SQLITE_DONE { break }
                guard stepResult == SQLITE_ROW, let stmt else {
                    throw DbError.query(String(cString: sqlite3_errmsg(db)))
                }

                var row: [String?] = []
                for i in 0..<colCount {
                    row.append(columnValue(stmt: stmt, index: i))
                }
                rows.append(row)
            }

            return QueryResult(columns: columns, rows: rows, rowsAffected: nil, totalCount: nil)
        }
    }

    private func execSQL(_ sql: String) throws {
        try handle.withLock { db in
            guard let db else { throw DbError.connection("database closed") }

            var errMsg: UnsafeMutablePointer<CChar>?
            let rc = sqlite3_exec(db, sql, nil, nil, &errMsg)
            if rc != SQLITE_OK {
                let msg = errMsg.map { String(cString: $0) } ?? "unknown error"
                sqlite3_free(errMsg)
                throw DbError.query(msg)
            }
        }
    }

    private func columnValue(stmt: OpaquePointer, index: Int32) -> String? {
        switch sqlite3_column_type(stmt, index) {
        case SQLITE_NULL:
            return nil
        case SQLITE_BLOB:
            let bytes = sqlite3_column_bytes(stmt, index)
            return "<BLOB \(bytes) bytes>"
        default:
            guard let text = sqlite3_column_text(stmt, index) else { return nil }
            return String(cString: text)
        }
    }
}
