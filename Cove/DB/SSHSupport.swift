import Foundation
import NIOCore
import NIOPosix
import NIOSSH
import Crypto

enum SSHSupportError: LocalizedError {
    case authFailed
    case connectionRefused(String, Int)
    case missingPrivateKey
    case unsupportedKeyFormat
    case encryptedKey
    case wrongPassphrase

    var errorDescription: String? {
        switch self {
        case .authFailed: "SSH authentication failed: check credentials"
        case .connectionRefused(let host, let port): "SSH connection to \(host):\(port) failed"
        case .missingPrivateKey: "Private key path not specified"
        case .unsupportedKeyFormat: "Unsupported private key format"
        case .encryptedKey: "Private key is encrypted"
        case .wrongPassphrase: "Incorrect passphrase for private key"
        }
    }
}

enum SSHSupport {
    static func connect(config: SSHTunnelConfig) async throws -> (group: MultiThreadedEventLoopGroup, channel: Channel) {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let sshPort = Int(config.sshPort) ?? 22
        let authDelegate = try authDelegate(for: config)

        do {
            let channel = try await ClientBootstrap(group: group)
                .channelInitializer { channel in
                    channel.pipeline.addHandler(
                        NIOSSHHandler(
                            role: .client(SSHClientConfiguration(
                                userAuthDelegate: authDelegate,
                                serverAuthDelegate: AcceptAllHostKeys()
                            )),
                            allocator: channel.allocator,
                            inboundChildChannelInitializer: nil
                        )
                    )
                }
                .connectTimeout(.seconds(10))
                .connect(host: config.sshHost, port: sshPort)
                .get()
            return (group, channel)
        } catch {
            try? await group.shutdownGracefully()
            throw SSHSupportError.connectionRefused(config.sshHost, sshPort)
        }
    }

    static func authDelegate(for config: SSHTunnelConfig) throws -> any NIOSSHClientUserAuthenticationDelegate & Sendable {
        switch config.authMethod {
        case .password:
            return PasswordAuthDelegate(username: config.sshUser, password: config.sshPassword ?? "")
        case .privateKey:
            guard let keyPath = config.privateKeyPath, !keyPath.isEmpty else {
                throw SSHSupportError.missingPrivateKey
            }
            let key = try loadPrivateKey(at: keyPath, passphrase: config.passphrase)
            return PrivateKeyAuthDelegate(username: config.sshUser, key: key)
        }
    }

    static func close(group: MultiThreadedEventLoopGroup, channels: [Channel]) async {
        for channel in channels {
            try? await channel.close()
        }
        try? await group.shutdownGracefully()
    }

    static func loadPrivateKey(at path: String, passphrase: String? = nil) throws -> NIOSSHPrivateKey {
        let expandedPath = (path as NSString).expandingTildeInPath
        let url = URL(fileURLWithPath: expandedPath)
        let data = try Data(contentsOf: url)
        guard let pem = String(data: data, encoding: .utf8) else {
            throw SSHSupportError.unsupportedKeyFormat
        }

        let lines = pem.components(separatedBy: .newlines)
        let header = lines.first(where: { $0.hasPrefix("-----BEGIN") }) ?? ""

        if header.contains("OPENSSH PRIVATE KEY") {
            let base64Lines = lines.filter { !$0.hasPrefix("-----") && !$0.isEmpty }
            guard let keyData = Data(base64Encoded: base64Lines.joined()) else {
                throw SSHSupportError.unsupportedKeyFormat
            }
            do {
                return try parseOpenSSHKey(keyData)
            } catch SSHSupportError.encryptedKey {
                return try decryptViaSSHKeygen(path: expandedPath, passphrase: passphrase ?? "")
            }
        }

        let base64Lines = lines.filter { !$0.hasPrefix("-----") && !$0.isEmpty }
        guard let keyData = Data(base64Encoded: base64Lines.joined()) else {
            throw SSHSupportError.unsupportedKeyFormat
        }

        if let p256 = try? P256.Signing.PrivateKey(derRepresentation: keyData) {
            return NIOSSHPrivateKey(p256Key: p256)
        }
        if let p384 = try? P384.Signing.PrivateKey(derRepresentation: keyData) {
            return NIOSSHPrivateKey(p384Key: p384)
        }
        if let p521 = try? P521.Signing.PrivateKey(derRepresentation: keyData) {
            return NIOSSHPrivateKey(p521Key: p521)
        }
        if keyData.count == 32 {
            let ed25519 = try Curve25519.Signing.PrivateKey(rawRepresentation: keyData)
            return NIOSSHPrivateKey(ed25519Key: ed25519)
        }

        throw SSHSupportError.unsupportedKeyFormat
    }

    private static func decryptViaSSHKeygen(path: String, passphrase: String) throws -> NIOSSHPrivateKey {
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.copyItem(atPath: path, toPath: tmp.path)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/ssh-keygen")
        process.arguments = ["-p", "-P", passphrase, "-N", "", "-f", tmp.path]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()

        if process.terminationStatus != 0 {
            throw SSHSupportError.wrongPassphrase
        }

        let decryptedData = try Data(contentsOf: tmp)
        guard let decryptedPem = String(data: decryptedData, encoding: .utf8) else {
            throw SSHSupportError.unsupportedKeyFormat
        }
        let lines = decryptedPem.components(separatedBy: .newlines)
        let base64Lines = lines.filter { !$0.hasPrefix("-----") && !$0.isEmpty }
        guard let keyData = Data(base64Encoded: base64Lines.joined()) else {
            throw SSHSupportError.unsupportedKeyFormat
        }
        return try parseOpenSSHKey(keyData)
    }

    private static func parseOpenSSHKey(_ data: Data) throws -> NIOSSHPrivateKey {
        var offset = 0
        let magic = Array("openssh-key-v1\0".utf8)
        guard data.count > magic.count else { throw SSHSupportError.unsupportedKeyFormat }

        for (i, b) in magic.enumerated() {
            guard data[i] == b else { throw SSHSupportError.unsupportedKeyFormat }
        }
        offset = magic.count

        func readUInt32() throws -> UInt32 {
            guard offset + 4 <= data.count else { throw SSHSupportError.unsupportedKeyFormat }
            let v = UInt32(data[offset]) << 24 | UInt32(data[offset+1]) << 16 |
                    UInt32(data[offset+2]) << 8 | UInt32(data[offset+3])
            offset += 4
            return v
        }

        func readString() throws -> Data {
            let len = Int(try readUInt32())
            guard offset + len <= data.count else { throw SSHSupportError.unsupportedKeyFormat }
            let result = data[offset..<offset+len]
            offset += len
            return Data(result)
        }

        let cipherName = String(data: try readString(), encoding: .utf8) ?? ""
        let kdfName = String(data: try readString(), encoding: .utf8) ?? ""
        _ = try readString()

        guard cipherName == "none" && kdfName == "none" else {
            throw SSHSupportError.encryptedKey
        }

        let numKeys = try readUInt32()
        guard numKeys == 1 else { throw SSHSupportError.unsupportedKeyFormat }

        _ = try readString()
        let privateSection = try readString()
        let pData = privateSection
        var pOff = 0

        func pReadUInt32() throws -> UInt32 {
            guard pOff + 4 <= pData.count else { throw SSHSupportError.unsupportedKeyFormat }
            let v = UInt32(pData[pOff]) << 24 | UInt32(pData[pOff+1]) << 16 |
                    UInt32(pData[pOff+2]) << 8 | UInt32(pData[pOff+3])
            pOff += 4
            return v
        }

        func pReadString() throws -> Data {
            let len = Int(try pReadUInt32())
            guard pOff + len <= pData.count else { throw SSHSupportError.unsupportedKeyFormat }
            let result = pData[pOff..<pOff+len]
            pOff += len
            return Data(result)
        }

        let check1 = try pReadUInt32()
        let check2 = try pReadUInt32()
        guard check1 == check2 else { throw SSHSupportError.unsupportedKeyFormat }

        let keyType = String(data: try pReadString(), encoding: .utf8) ?? ""
        switch keyType {
        case "ssh-ed25519":
            _ = try pReadString()
            let combined = try pReadString()
            guard combined.count == 64 else { throw SSHSupportError.unsupportedKeyFormat }
            let key = try Curve25519.Signing.PrivateKey(rawRepresentation: combined.prefix(32))
            return NIOSSHPrivateKey(ed25519Key: key)
        case "ecdsa-sha2-nistp256":
            _ = try pReadString()
            _ = try pReadString()
            return NIOSSHPrivateKey(p256Key: try P256.Signing.PrivateKey(rawRepresentation: try pReadString()))
        case "ecdsa-sha2-nistp384":
            _ = try pReadString()
            _ = try pReadString()
            return NIOSSHPrivateKey(p384Key: try P384.Signing.PrivateKey(rawRepresentation: try pReadString()))
        case "ecdsa-sha2-nistp521":
            _ = try pReadString()
            _ = try pReadString()
            return NIOSSHPrivateKey(p521Key: try P521.Signing.PrivateKey(rawRepresentation: try pReadString()))
        default:
            throw SSHSupportError.unsupportedKeyFormat
        }
    }
}

final class PasswordAuthDelegate: NIOSSHClientUserAuthenticationDelegate, Sendable {
    let username: String
    let password: String

    init(username: String, password: String) {
        self.username = username
        self.password = password
    }

    func nextAuthenticationType(
        availableMethods: NIOSSHAvailableUserAuthenticationMethods,
        nextChallengePromise: EventLoopPromise<NIOSSHUserAuthenticationOffer?>
    ) {
        if availableMethods.contains(.password) {
            nextChallengePromise.succeed(NIOSSHUserAuthenticationOffer(
                username: username,
                serviceName: "",
                offer: .password(.init(password: password))
            ))
        } else {
            nextChallengePromise.succeed(nil)
        }
    }
}

final class PrivateKeyAuthDelegate: NIOSSHClientUserAuthenticationDelegate, Sendable {
    let username: String
    let key: NIOSSHPrivateKey

    init(username: String, key: NIOSSHPrivateKey) {
        self.username = username
        self.key = key
    }

    func nextAuthenticationType(
        availableMethods: NIOSSHAvailableUserAuthenticationMethods,
        nextChallengePromise: EventLoopPromise<NIOSSHUserAuthenticationOffer?>
    ) {
        if availableMethods.contains(.publicKey) {
            nextChallengePromise.succeed(NIOSSHUserAuthenticationOffer(
                username: username,
                serviceName: "",
                offer: .privateKey(.init(privateKey: key))
            ))
        } else {
            nextChallengePromise.succeed(nil)
        }
    }
}

final class AcceptAllHostKeys: NIOSSHClientServerAuthenticationDelegate, Sendable {
    func validateHostKey(
        hostKey: NIOSSHPublicKey,
        validationCompletePromise: EventLoopPromise<Void>
    ) {
        validationCompletePromise.succeed(())
    }
}
