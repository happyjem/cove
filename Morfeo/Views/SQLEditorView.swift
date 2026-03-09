import SwiftUI
import AppKit

struct SQLEditorView: NSViewRepresentable {
    @Binding var text: String
    @Binding var selectedRange: NSRange
    var runnableRange: NSRange
    var keywords: Set<String> = []

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.borderType = .noBorder
        scrollView.autoresizingMask = [.width, .height]

        let textView = NSTextView()
        textView.isEditable = true
        textView.isSelectable = true
        textView.allowsUndo = true
        textView.isRichText = false
        textView.usesFindPanel = true
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.isAutomaticTextReplacementEnabled = false
        textView.font = NSFont.monospacedSystemFont(ofSize: 13, weight: .regular)
        textView.textColor = .labelColor
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        scrollView.drawsBackground = false
        textView.insertionPointColor = .labelColor
        textView.selectedTextAttributes = [
            .backgroundColor: NSColor.selectedTextBackgroundColor
        ]
        textView.textContainerInset = NSSize(width: 12, height: 8)
        textView.isVerticallyResizable = true
        textView.isHorizontallyResizable = false
        textView.autoresizingMask = [.width]
        textView.textContainer?.widthTracksTextView = true

        let barView = NSView()
        barView.wantsLayer = true
        barView.layer?.backgroundColor = NSColor.controlAccentColor.cgColor
        barView.layer?.cornerRadius = 1.5
        textView.addSubview(barView)
        context.coordinator.barView = barView

        textView.delegate = context.coordinator
        scrollView.documentView = textView
        context.coordinator.textView = textView

        if !text.isEmpty {
            textView.string = text
            highlightText(textView)
        }

        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? NSTextView else { return }
        if textView.string != text {
            let selectedRanges = textView.selectedRanges
            textView.string = text
            highlightText(textView)
            textView.selectedRanges = selectedRanges
        }
        context.coordinator.updateBar(range: runnableRange)
    }

    private func highlightText(_ textView: NSTextView) {
        guard let storage = textView.textStorage else { return }
        let fullRange = NSRange(location: 0, length: storage.length)
        let font = NSFont.monospacedSystemFont(ofSize: 13, weight: .regular)
        storage.beginEditing()
        storage.addAttribute(.foregroundColor, value: NSColor.labelColor, range: fullRange)
        storage.addAttribute(.font, value: font, range: fullRange)
        for token in SQLHighlighter.tokenize(textView.string, keywords: keywords) {
            storage.addAttribute(.foregroundColor, value: SQLHighlighter.colorFor(token.kind), range: token.range)
        }
        storage.endEditing()
    }

    final class Coordinator: NSObject, NSTextViewDelegate {
        let parent: SQLEditorView
        weak var textView: NSTextView?
        var barView: NSView?

        init(_ parent: SQLEditorView) {
            self.parent = parent
        }

        func textDidChange(_ notification: Notification) {
            guard let textView = notification.object as? NSTextView else { return }
            parent.text = textView.string
            parent.selectedRange = textView.selectedRange()
            parent.highlightText(textView)
        }

        func textViewDidChangeSelection(_ notification: Notification) {
            guard let textView = notification.object as? NSTextView else { return }
            parent.selectedRange = textView.selectedRange()
        }

        func updateBar(range: NSRange) {
            guard let textView, let barView,
                  let layoutManager = textView.layoutManager,
                  let textContainer = textView.textContainer
            else { return }

            guard range.length > 0, range.location + range.length <= (textView.string as NSString).length else {
                barView.isHidden = true
                return
            }

            let glyphRange = layoutManager.glyphRange(
                forCharacterRange: range,
                actualCharacterRange: nil
            )
            let blockRect = layoutManager.boundingRect(
                forGlyphRange: glyphRange,
                in: textContainer
            )

            let origin = textView.textContainerOrigin
            barView.frame = NSRect(
                x: 4,
                y: blockRect.minY + origin.y,
                width: 3,
                height: blockRect.height
            )
            barView.isHidden = false
        }
    }
}
