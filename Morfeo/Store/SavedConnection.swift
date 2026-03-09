import Foundation

struct SavedConnection: Codable, Identifiable {
    var id = UUID()
    var name: String
    var backend: BackendType
    var host: String
    var port: String
    var user: String
    var password: String
    var database: String
    var colorHex: String?
}
