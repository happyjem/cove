import Foundation
import ClickHouseNIO

extension ClickHouseBackend {

    func transposeResult(
        _ result: ClickHouseQueryResult,
        columnInfos: [ColumnInfo]? = nil
    ) -> (columns: [ColumnInfo], rows: [[String?]]) {
        let chColumns = result.columns
        guard let first = chColumns.first else { return ([], []) }
        let rowCount = first.count

        let infos: [ColumnInfo]
        if let columnInfos {
            infos = columnInfos
        } else {
            infos = chColumns.map { col in
                ColumnInfo(name: col.name, typeName: inferTypeName(col.values), isPrimaryKey: false)
            }
        }

        let stringCols = chColumns.map { decodeColumnValues($0.values) }

        var rows: [[String?]] = []
        rows.reserveCapacity(rowCount)
        for i in 0..<rowCount {
            var row: [String?] = []
            row.reserveCapacity(chColumns.count)
            for colIdx in 0..<chColumns.count {
                row.append(stringCols[colIdx][i])
            }
            rows.append(row)
        }

        return (infos, rows)
    }

    // MARK: - Column value decoding

    private func decodeColumnValues(_ values: ClickHouseDataTypeArray) -> [String?] {
        if let arr = values as? [String] { return arr.map { $0 } }
        if let arr = values as? [String?] { return arr.map { $0 } }

        if let arr = values as? [Int8] { return arr.map { String($0) } }
        if let arr = values as? [Int8?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [Int16] { return arr.map { String($0) } }
        if let arr = values as? [Int16?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [Int32] { return arr.map { String($0) } }
        if let arr = values as? [Int32?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [Int64] { return arr.map { String($0) } }
        if let arr = values as? [Int64?] { return arr.map { $0.map(String.init) } }

        if let arr = values as? [UInt8] { return arr.map { String($0) } }
        if let arr = values as? [UInt8?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [UInt16] { return arr.map { String($0) } }
        if let arr = values as? [UInt16?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [UInt32] { return arr.map { String($0) } }
        if let arr = values as? [UInt32?] { return arr.map { $0.map(String.init) } }
        if let arr = values as? [UInt64] { return arr.map { String($0) } }
        if let arr = values as? [UInt64?] { return arr.map { $0.map(String.init) } }

        if let arr = values as? [Float] { return arr.map { String($0) } }
        if let arr = values as? [Float?] { return arr.map { $0.map { String($0) } } }
        if let arr = values as? [Double] { return arr.map { String($0) } }
        if let arr = values as? [Double?] { return arr.map { $0.map { String($0) } } }

        if let arr = values as? [UUID] { return arr.map { $0.uuidString } }
        if let arr = values as? [UUID?] { return arr.map { $0?.uuidString } }

        if let arr = values as? [Bool] { return arr.map { String($0) } }
        if let arr = values as? [Bool?] { return arr.map { $0.map(String.init) } }

        if let arr = values as? [ClickHouseDate] { return arr.map { Self.dateFormatter.string(from: $0.date) } }
        if let arr = values as? [ClickHouseDate?] { return arr.map { $0.map { Self.dateFormatter.string(from: $0.date) } } }
        if let arr = values as? [ClickHouseDate32] { return arr.map { Self.dateFormatter.string(from: $0.date) } }
        if let arr = values as? [ClickHouseDate32?] { return arr.map { $0.map { Self.dateFormatter.string(from: $0.date) } } }

        if let arr = values as? [ClickHouseDateTime] { return arr.map { Self.timestampFormatter.string(from: $0.date) } }
        if let arr = values as? [ClickHouseDateTime?] { return arr.map { $0.map { Self.timestampFormatter.string(from: $0.date) } } }
        if let arr = values as? [ClickHouseDateTime64] { return arr.map { Self.timestampFormatter.string(from: $0.date) } }
        if let arr = values as? [ClickHouseDateTime64?] { return arr.map { $0.map { Self.timestampFormatter.string(from: $0.date) } } }

        if let arr = values as? [ClickHouseEnum8] { return arr.map { $0.word } }
        if let arr = values as? [ClickHouseEnum8?] { return arr.map { $0?.word } }
        if let arr = values as? [ClickHouseEnum16] { return arr.map { $0.word } }
        if let arr = values as? [ClickHouseEnum16?] { return arr.map { $0?.word } }

        return Array(repeating: "[unsupported]", count: values.count)
    }

    // MARK: - Type inference

    private func inferTypeName(_ values: ClickHouseDataTypeArray) -> String {
        switch values {
        case is [String], is [String?]: return "String"
        case is [Int8], is [Int8?]: return "Int8"
        case is [Int16], is [Int16?]: return "Int16"
        case is [Int32], is [Int32?]: return "Int32"
        case is [Int64], is [Int64?]: return "Int64"
        case is [UInt8], is [UInt8?]: return "UInt8"
        case is [UInt16], is [UInt16?]: return "UInt16"
        case is [UInt32], is [UInt32?]: return "UInt32"
        case is [UInt64], is [UInt64?]: return "UInt64"
        case is [Float], is [Float?]: return "Float32"
        case is [Double], is [Double?]: return "Float64"
        case is [UUID], is [UUID?]: return "UUID"
        case is [Bool], is [Bool?]: return "Bool"
        case is [ClickHouseDate], is [ClickHouseDate?]: return "Date"
        case is [ClickHouseDate32], is [ClickHouseDate32?]: return "Date32"
        case is [ClickHouseDateTime], is [ClickHouseDateTime?]: return "DateTime"
        case is [ClickHouseDateTime64], is [ClickHouseDateTime64?]: return "DateTime64"
        case is [ClickHouseEnum8], is [ClickHouseEnum8?]: return "Enum8"
        case is [ClickHouseEnum16], is [ClickHouseEnum16?]: return "Enum16"
        default: return "Unknown"
        }
    }

    // MARK: - Formatters

    static let dateFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()

    static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss"
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }()
}
