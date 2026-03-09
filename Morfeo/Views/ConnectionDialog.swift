import SwiftUI

struct ConnectionDialog: View {
    @Environment(AppState.self) private var state
    @State private var name = ""
    @State private var backend: BackendType = .postgres
    @State private var host = "localhost"
    @State private var port = "5432"
    @State private var user = ""
    @State private var password = ""
    @State private var database = ""
    @State private var selectedColor = MorfeoTheme.accent

    var body: some View {
        let dialog = state.dialog

        VStack(spacing: 12) {
            Text(dialog.isEditing ? "Edit Connection" : "New Connection")
                .font(.system(size: 16, weight: .semibold))
                .foregroundStyle(.primary)
                .frame(maxWidth: .infinity, alignment: .leading)

            formField("Name") {
                HStack(spacing: 8) {
                    TextField("Name", text: $name)
                        .textFieldStyle(.roundedBorder)
                    ColorPicker("", selection: $selectedColor, supportsOpacity: false)
                        .labelsHidden()
                        .fixedSize()
                }
            }

            formField("Backend") {
                Picker("", selection: $backend) {
                    ForEach(BackendType.allCases, id: \.self) { type in
                        Text(type.displayName).tag(type)
                    }
                }
                .labelsHidden()
                .frame(maxWidth: .infinity, alignment: .leading)
            }

            HStack(spacing: 8) {
                formField("Host") {
                    TextField("Host", text: $host)
                        .textFieldStyle(.roundedBorder)
                }
                formField("Port") {
                    TextField("Port", text: $port)
                        .textFieldStyle(.roundedBorder)
                }
                .frame(width: 80)
            }

            formField("User") {
                TextField("User", text: $user)
                    .textFieldStyle(.roundedBorder)
            }

            formField("Password") {
                SecureField("Password", text: $password)
                    .textFieldStyle(.roundedBorder)
            }

            formField("Database") {
                TextField("Database", text: $database)
                    .textFieldStyle(.roundedBorder)
            }

            if !dialog.error.isEmpty {
                Text(dialog.error)
                    .font(.system(size: 12))
                    .foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button("Cancel") {
                    state.dialogCancel()
                }
                .keyboardShortcut(.escape, modifiers: [])

                if dialog.isEditing {
                    Button("Save") {
                        syncToDialog()
                        state.dialogSaveEdit()
                    }
                    .keyboardShortcut(.return, modifiers: [])
                    .buttonStyle(.borderedProminent)
                } else {
                    Button(dialog.connecting ? "Connecting..." : "Connect") {
                        syncToDialog()
                        state.dialogConnect()
                    }
                    .disabled(dialog.connecting)
                    .keyboardShortcut(.return, modifiers: [])
                    .buttonStyle(.borderedProminent)
                }
            }
        }
        .padding(24)
        .frame(width: 400)
        .presentationBackground(.regularMaterial)
        .onAppear {
            name = dialog.name
            backend = dialog.backend
            host = dialog.host
            port = dialog.port
            user = dialog.user
            password = dialog.password
            database = dialog.database
            selectedColor = Color(hex: dialog.colorHex)
        }
        .onDisappear {
            NSColorPanel.shared.close()
        }
    }

    private func syncToDialog() {
        let dialog = state.dialog
        dialog.name = name
        dialog.backend = backend
        dialog.host = host
        dialog.port = port
        dialog.user = user
        dialog.password = password
        dialog.database = database
        dialog.colorHex = selectedColor.hexString
    }

    private func formField<Content: View>(_ label: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(label)
                .font(.system(size: 12))
                .foregroundStyle(.secondary)
            content()
        }
    }
}
