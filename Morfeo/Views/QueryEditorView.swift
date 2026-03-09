import SwiftUI

struct QueryEditorView: View {
    @Environment(AppState.self) private var state

    var body: some View {
        @Bindable var query = state.query

        VStack(spacing: 0) {
            SQLEditorView(text: $query.text, selectedRange: $query.selectedRange, runnableRange: state.query.runnableRange, keywords: state.connection?.syntaxKeywords ?? [])
                .frame(maxHeight: .infinity)

            editorToolbar
        }
    }

    // MARK: - Toolbar

    private var editorToolbar: some View {
        HStack(spacing: 8) {
            if !state.query.error.isEmpty {
                Text(state.query.error)
                    .font(.system(size: 12))
                    .foregroundStyle(.red)
                    .lineLimit(1)
            }

            Spacer()

            if state.query.executing {
                ProgressView()
                    .controlSize(.small)
            }

            Button {
                state.executeQuery()
            } label: {
                Label("Run Current", systemImage: "play.fill")
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.small)
            .keyboardShortcut(.return, modifiers: .command)
        }
        .padding(.horizontal, 12)
        .frame(height: 36)
        .background(.ultraThinMaterial)
        .overlay(
            Rectangle().frame(height: 1).foregroundStyle(Color(nsColor: .separatorColor)),
            alignment: .top
        )
    }
}
