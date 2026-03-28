import Foundation
import CosmoSQLCore

extension SQLServerBackend {
    static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()

    static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSSXXX"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()

    func decodeRowValues(_ row: SQLRow) -> [String?] {
        row.values.map { decodeValue($0) }
    }

    private func decodeValue(_ value: SQLValue) -> String? {
        if value.isNull { return nil }
        switch value {
        case .null:
            return nil
        case .bool(let v):
            return String(v)
        case .int(let v):
            return String(v)
        case .int8(let v):
            return String(v)
        case .int16(let v):
            return String(v)
        case .int32(let v):
            return String(v)
        case .int64(let v):
            return String(v)
        case .float(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .decimal(let v):
            return String(describing: v)
        case .string(let s):
            return s
        case .bytes(let b):
            return "\\x" + b.map { String(format: "%02x", $0) }.joined()
        case .uuid(let u):
            return u.uuidString
        case .date(let d):
            return Self.timestampFormatter.string(from: d)
        @unknown default:
            return String(describing: value)
        }
    }
}
