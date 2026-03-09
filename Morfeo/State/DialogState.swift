import Foundation

@Observable
final class DialogState {
    var name = ""
    var backend: BackendType = .postgres
    var host = "localhost"
    var port = "5432"
    var user = ""
    var password = ""
    var database = ""
    var error = ""
    var connecting = false
    var visible = false
    var editingConnectionId: UUID?
    var colorHex: String = MorfeoTheme.accentHex

    var isEditing: Bool { editingConnectionId != nil }

    func reset() {
        name = ""
        backend = .postgres
        host = "localhost"
        port = "5432"
        user = ""
        password = ""
        database = ""
        error = ""
        connecting = false
        editingConnectionId = nil
        colorHex = MorfeoTheme.accentHex
    }
}
