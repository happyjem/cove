import SwiftUI

struct ContentView: View {
    @Environment(AppState.self) private var state


    var body: some View {
        VStack(spacing: 0) {
            if !state.errorText.isEmpty {
                errorBar
            }

            HStack(spacing: 0) {
                ConnectionRail()

                HSplitView {
                    if state.showSidebar {
                        SidebarView()
                            .clipShape(RoundedRectangle(cornerRadius: 10))
                            .overlay(
                                RoundedRectangle(cornerRadius: 10)
                                    .strokeBorder(.quaternary, lineWidth: 1)
                            )
                            .padding(.trailing, 4)
                            .frame(minWidth: 184, idealWidth: 264, maxWidth: 604)
                    }

                    contentArea
                        .background {
                            VisualEffectBackground(material: .underWindowBackground)
                        }
                        .clipShape(RoundedRectangle(cornerRadius: 10))
                        .overlay(
                            RoundedRectangle(cornerRadius: 10)
                                .strokeBorder(.quaternary, lineWidth: 1)
                        )
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .layoutPriority(1)

                    if state.showInspector, let table = state.table,
                       state.contentMode == .table, table.selectedRow != nil {
                        RowInspectorView(table: table)
                            .clipShape(RoundedRectangle(cornerRadius: 10))
                            .overlay(
                                RoundedRectangle(cornerRadius: 10)
                                    .strokeBorder(.quaternary, lineWidth: 1)
                            )
                            .padding(.leading, 4)
                            .frame(minWidth: 204, idealWidth: 284, maxWidth: 404)
                    }
                }
                .hideSplitDividers()
                .padding(.trailing, 6)
                .padding(.vertical, 6)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .background(.ultraThinMaterial)
        .background {
            Color.clear.ignoresSafeArea()
        }
        .toolbarBackgroundVisibility(.hidden, for: .windowToolbar)
        .toolbar { toolbarContent }
        .sheet(isPresented: Binding(
            get: { state.dialog.visible },
            set: { if !$0 { state.dialogCancel() } }
        )) {
            ConnectionDialog()
                .environment(state)
        }
        .alert(
            "Delete Connection",
            isPresented: Binding(
                get: { state.connectionToDelete != nil },
                set: { if !$0 { state.connectionToDelete = nil } }
            )
        ) {
            Button("Cancel", role: .cancel) {
                state.connectionToDelete = nil
            }
            Button("Delete", role: .destructive) {
                state.confirmDeleteConnection()
            }
        } message: {
            if let conn = state.connectionToDelete {
                Text("Are you sure you want to delete \"\(conn.name)\"?")
            }
        }
    }

    // MARK: - Toolbar

    @ToolbarContentBuilder
    private var toolbarContent: some ToolbarContent {
        let hasTable = state.table != nil && state.contentMode == .table
        let hasPending = state.table?.hasPendingEdits ?? false
        let canInspect = hasTable && state.table?.selectedRow != nil

        ToolbarItemGroup(placement: .navigation) {
            if hasTable {
                Button { state.refresh() } label: {
                    Image(systemName: "arrow.clockwise")
                }
                if hasPending {
                    Button { state.discardEdits() } label: {
                        Image(systemName: "arrow.uturn.backward")
                    }
                }
            }
        }

        ToolbarItem(placement: .principal) {
            if !state.breadcrumb.isEmpty {
                Text(state.breadcrumb)
                    .font(.system(size: 12))
                    .foregroundStyle(.primary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 4)
                    .background(.quaternary, in: Capsule())
            }
        }

        ToolbarItem(placement: .primaryAction) {
            if state.connection != nil {
                Button { state.toggleQuery() } label: {
                    Text("SQL")
                        .font(.system(size: 12, weight: .medium))
                }
            }
        }

        ToolbarItemGroup(placement: .automatic) {
            Button { state.showSidebar.toggle() } label: {
                Image(systemName: "sidebar.leading")
            }

            Button { state.showBottomPanel.toggle() } label: {
                Image(systemName: "rectangle.split.1x2")
            }
            .disabled(state.contentMode != .query || state.queryTable == nil)

            Button { state.showInspector.toggle() } label: {
                Image(systemName: "sidebar.trailing")
            }
            .disabled(!canInspect)
        }
    }

    // MARK: - Content

    @ViewBuilder
    private var contentArea: some View {
        switch state.contentMode {
        case .empty:
            Text("Select a table or open a query")
                .font(.system(size: 13))
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, maxHeight: .infinity)

        case .table:
            if let table = state.table {
                VStack(spacing: 0) {
                    if state.tableTab == .structure, state.hasStructureTab {
                        if let structure = state.structureTable {
                            DataTableView(table: structure, isQueryResult: true)
                        } else {
                            ProgressView()
                                .frame(maxWidth: .infinity, maxHeight: .infinity)
                        }
                    } else {
                        DataTableView(table: table, isQueryResult: false)
                    }

                    if state.hasStructureTab, state.tableTab == .structure {
                        structureFooter
                    }
                }
            }

        case .query:
            if state.showBottomPanel, let queryTable = state.queryTable {
                VSplitView {
                    QueryEditorView()
                        .frame(minHeight: 120)

                    VStack(spacing: 0) {
                        DataTableView(table: queryTable, isQueryResult: true)
                        queryResultFooter
                    }
                    .frame(minHeight: 80)
                }
            } else {
                QueryEditorView()
            }
        }
    }

    private var queryResultFooter: some View {
        HStack {
            Text(state.query.status)
                .font(.system(size: 11))
                .foregroundStyle(.secondary)
            Spacer()
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .frame(height: 28)
        .background(.ultraThinMaterial)
    }

    private var structureFooter: some View {
        HStack {
            Picker("", selection: Binding(
                get: { state.tableTab },
                set: { state.tableTabChanged($0) }
            )) {
                Text("Data").tag(TableTab.data)
                Text("Structure").tag(TableTab.structure)
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .controlSize(.small)
            .fixedSize()
            Spacer()
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .frame(height: 34)
        .background(.ultraThinMaterial)
    }

    private var errorBar: some View {
        HStack {
            Text(state.errorText)
                .font(.system(size: 12))
                .foregroundStyle(.white)
            Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 4)
        .background(.red)
    }
}

private struct SplitDividerHider: NSViewRepresentable {
    func makeNSView(context: Context) -> NSView {
        let view = NSView()
        DispatchQueue.main.async {
            guard let splitView = findSplitView(from: view) else { return }
            splitView.dividerStyle = .thin
            for subview in splitView.subviews {
                let className = String(describing: type(of: subview))
                if className.contains("Divider") || className.contains("divider") {
                    subview.alphaValue = 0
                }
            }
        }
        return view
    }

    func updateNSView(_ nsView: NSView, context: Context) {}

    private func findSplitView(from view: NSView) -> NSSplitView? {
        var current: NSView? = view
        while let v = current {
            if let split = v as? NSSplitView { return split }
            current = v.superview
        }
        return nil
    }
}

extension View {
    func hideSplitDividers() -> some View {
        background { SplitDividerHider() }
    }
}

