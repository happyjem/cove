import Foundation
import NIOCore
import NIOPosix
import NIOSSH

struct SSHCommandResult: Sendable {
    let stdout: String
    let stderr: String
    let exitCode: Int
}

enum SSHCommandRunnerError: LocalizedError {
    case authFailed
    case connectionRefused(String, Int)

    var errorDescription: String? {
        switch self {
        case .authFailed: "SSH authentication failed: check credentials"
        case .connectionRefused(let host, let port): "SSH connection to \(host):\(port) failed"
        }
    }
}

final class SSHCommandRunner: @unchecked Sendable {
    private let group: MultiThreadedEventLoopGroup
    private let sshChannel: Channel

    private init(group: MultiThreadedEventLoopGroup, sshChannel: Channel) {
        self.group = group
        self.sshChannel = sshChannel
    }

    static func connect(config: SSHTunnelConfig) async throws -> SSHCommandRunner {
        let group: MultiThreadedEventLoopGroup
        let sshChannel: Channel
        do {
            let connection = try await SSHSupport.connect(config: config)
            group = connection.group
            sshChannel = connection.channel
        } catch let error as SSHSupportError {
            switch error {
            case .connectionRefused(let host, let port):
                throw SSHCommandRunnerError.connectionRefused(host, port)
            default:
                throw SSHCommandRunnerError.authFailed
            }
        }

        let runner = SSHCommandRunner(group: group, sshChannel: sshChannel)
        do {
            _ = try await runner.run("true")
            return runner
        } catch {
            await runner.close()
            throw SSHCommandRunnerError.authFailed
        }
    }

    func run(_ command: String) async throws -> SSHCommandResult {
        let eventLoop = sshChannel.eventLoop
        let resultPromise = eventLoop.makePromise(of: SSHCommandResult.self)
        let channelPromise = eventLoop.makePromise(of: Channel.self)

        eventLoop.execute {
            do {
                let sshHandler = try self.sshChannel.pipeline.syncOperations.handler(type: NIOSSHHandler.self)
                sshHandler.createChannel(channelPromise, channelType: .session) { childChannel, _ in
                    childChannel.pipeline.addHandler(SSHExecClientHandler(command: command, completePromise: resultPromise))
                }
            } catch {
                channelPromise.fail(error)
                resultPromise.fail(error)
            }
        }

        _ = try await channelPromise.futureResult.get()
        return try await resultPromise.futureResult.get()
    }

    func close() async {
        await SSHSupport.close(group: group, channels: [sshChannel])
    }
}

private final class SSHExecClientHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = SSHChannelData

    private var stdout = ByteBuffer()
    private var stderr = ByteBuffer()
    private var exitCode = 0
    private var completePromise: EventLoopPromise<SSHCommandResult>?
    private let command: String

    init(command: String, completePromise: EventLoopPromise<SSHCommandResult>) {
        self.command = command
        self.completePromise = completePromise
    }

    func channelActive(context: ChannelHandlerContext) {
        context.triggerUserOutboundEvent(SSHChannelRequestEvent.ExecRequest(command: command, wantReply: false)).whenFailure { [weak context] error in
            self.fail(error)
            context?.close(promise: nil)
        }
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case let event as SSHChannelRequestEvent.ExitStatus:
            exitCode = event.exitStatus
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let sshData = unwrapInboundIn(data)
        guard case .byteBuffer(var buffer) = sshData.data else { return }
        switch sshData.type {
        case .channel:
            stdout.writeBuffer(&buffer)
        case .stdErr:
            stderr.writeBuffer(&buffer)
        default:
            break
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        guard let promise = completePromise else { return }
        completePromise = nil
        promise.succeed(SSHCommandResult(
            stdout: stdout.readString(length: stdout.readableBytes) ?? "",
            stderr: stderr.readString(length: stderr.readableBytes) ?? "",
            exitCode: exitCode
        ))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        fail(error)
        context.close(promise: nil)
    }

    private func fail(_ error: Error) {
        guard let promise = completePromise else { return }
        completePromise = nil
        promise.fail(error)
    }
}
