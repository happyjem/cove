import Foundation
import NIOCore
import NIOPosix
import NIOSSH

enum SSHTunnelError: LocalizedError {
    case authFailed
    case connectionRefused(String, Int)
    case tunnelFailed

    var errorDescription: String? {
        switch self {
        case .authFailed: "SSH authentication failed: check credentials"
        case .connectionRefused(let host, let port): "SSH connection to \(host):\(port) failed"
        case .tunnelFailed: "Could not establish SSH tunnel"
        }
    }
}

final class SSHTunnel: @unchecked Sendable {
    let localPort: Int
    private let group: MultiThreadedEventLoopGroup
    private let serverChannel: Channel
    private let sshChannel: Channel

    private init(group: MultiThreadedEventLoopGroup, serverChannel: Channel, sshChannel: Channel, localPort: Int) {
        self.group = group
        self.serverChannel = serverChannel
        self.sshChannel = sshChannel
        self.localPort = localPort
    }

    static func establish(
        config: SSHTunnelConfig,
        remoteHost: String,
        remotePort: Int
    ) async throws -> SSHTunnel {
        let originAddr = try SocketAddress(ipAddress: "127.0.0.1", port: 0)

        let group: MultiThreadedEventLoopGroup
        let sshChannel: Channel
        do {
            let connection = try await SSHSupport.connect(config: config)
            group = connection.group
            sshChannel = connection.channel
        } catch let error as SSHSupportError {
            switch error {
            case .authFailed:
                throw SSHTunnelError.authFailed
            case .connectionRefused(let host, let port):
                throw SSHTunnelError.connectionRefused(host, port)
            default:
                throw SSHTunnelError.authFailed
            }
        }

        let sshHandler: NIOSSHHandler
        do {
            sshHandler = try await sshChannel.pipeline.handler(type: NIOSSHHandler.self).get()
        } catch {
            await SSHSupport.close(group: group, channels: [sshChannel])
            throw SSHTunnelError.tunnelFailed
        }

        do {
            let verifyPromise = sshChannel.eventLoop.makePromise(of: Channel.self)
            sshHandler.createChannel(verifyPromise, channelType: .directTCPIP(.init(
                targetHost: remoteHost,
                targetPort: remotePort,
                originatorAddress: originAddr
            ))) { childChannel, _ in
                childChannel.eventLoop.makeSucceededVoidFuture()
            }
            let testChannel = try await verifyPromise.futureResult.get()
            try? await testChannel.close()
        } catch {
            await SSHSupport.close(group: group, channels: [sshChannel])
            throw SSHTunnelError.authFailed
        }

        let rHost = remoteHost
        let rPort = remotePort
        let serverChannel: Channel
        do {
            serverChannel = try await ServerBootstrap(group: group)
                .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .childChannelInitializer { localChannel in
                    localChannel.setOption(ChannelOptions.autoRead, value: false).flatMap {
                        let promise = localChannel.eventLoop.makePromise(of: Channel.self)
                        sshHandler.createChannel(promise, channelType: .directTCPIP(.init(
                            targetHost: rHost,
                            targetPort: rPort,
                            originatorAddress: originAddr
                        ))) { sshChild, _ in
                            sshChild.pipeline.addHandler(SSHToLocalHandler(localChannel: localChannel))
                        }
                        return promise.futureResult.flatMap { sshChild in
                            localChannel.pipeline.addHandler(LocalToSSHHandler(sshChannel: sshChild)).flatMap {
                                localChannel.setOption(ChannelOptions.autoRead, value: true)
                            }
                        }
                    }
                }
                .bind(host: "127.0.0.1", port: 0)
                .get()
        } catch {
            await SSHSupport.close(group: group, channels: [sshChannel])
            throw SSHTunnelError.tunnelFailed
        }

        guard let port = serverChannel.localAddress?.port else {
            await SSHSupport.close(group: group, channels: [serverChannel, sshChannel])
            throw SSHTunnelError.tunnelFailed
        }

        return SSHTunnel(group: group, serverChannel: serverChannel, sshChannel: sshChannel, localPort: port)
    }

    func close() async {
        await SSHSupport.close(group: group, channels: [serverChannel, sshChannel])
    }
}

private final class SSHToLocalHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = SSHChannelData
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = SSHChannelData
    typealias OutboundOut = SSHChannelData

    private let localChannel: Channel

    init(localChannel: Channel) {
        self.localChannel = localChannel
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let sshData = unwrapInboundIn(data)
        guard case .byteBuffer(let buffer) = sshData.data, sshData.type == .channel else { return }
        localChannel.writeAndFlush(buffer, promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        localChannel.close(promise: nil)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        localChannel.close(promise: nil)
        context.close(promise: nil)
    }
}

private final class LocalToSSHHandler: ChannelDuplexHandler, @unchecked Sendable {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer

    private let sshChannel: Channel

    init(sshChannel: Channel) {
        self.sshChannel = sshChannel
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        let sshData = SSHChannelData(type: .channel, data: .byteBuffer(buffer))
        sshChannel.writeAndFlush(sshData, promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        sshChannel.close(promise: nil)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        sshChannel.close(promise: nil)
        context.close(promise: nil)
    }
}
