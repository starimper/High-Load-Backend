package kz.kbtu.pastebin.grpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** gRPC message framing: 1 byte "compressed" flag + 4 bytes length + message. Also status codes. */
final class GrpcWire {
    static final int MAX_MESSAGE_BYTES = 1_100_000;
    private GrpcWire() {}

    static final int OK = 0, INVALID_ARGUMENT = 3, DEADLINE_EXCEEDED = 4, NOT_FOUND = 5, FAILED_PRECONDITION = 9,
            OUT_OF_RANGE = 11, INTERNAL = 13, UNAVAILABLE = 14;

    static final String PATH_CREATE = "/pastebin.v1.Pastebin/CreatePaste";
    static final String PATH_GET = "/pastebin.v1.Pastebin/GetPaste";

    static ByteBuf frame(byte[] message, boolean gzip) throws IOException {
        byte[] body = message;
        if (gzip) {
            var bos = new ByteArrayOutputStream();
            try (var gz = new GZIPOutputStream(bos)) {
                gz.write(message);
            }
            body = bos.toByteArray();
        }
        ByteBuf buf = Unpooled.buffer(5 + body.length);
        buf.writeByte(gzip ? 1 : 0);
        buf.writeInt(body.length);
        buf.writeBytes(body);
        return buf;
    }

    /** Returns the message inside the first frame (gunzipped if the flag says so). */
    static byte[] unframe(byte[] data) throws IOException {
        if (data.length < 5) throw new IOException("short gRPC frame");
        boolean compressed = data[0] == 1;
        int len = ((data[1] & 0xFF) << 24) | ((data[2] & 0xFF) << 16) | ((data[3] & 0xFF) << 8) | (data[4] & 0xFF);
        if (len < 0 || data.length < 5 + len) throw new IOException("truncated gRPC frame");
        if (len > MAX_MESSAGE_BYTES) throw new MessageTooLargeException("compressed gRPC message is too large");
        byte[] msg = new byte[len];
        System.arraycopy(data, 5, msg, 0, len);
        if (compressed) {
            try (var in = new GZIPInputStream(new ByteArrayInputStream(msg))) {
                var out = new ByteArrayOutputStream(Math.min(MAX_MESSAGE_BYTES, 8192));
                byte[] chunk = new byte[8192]; int total = 0; int n;
                while ((n = in.read(chunk)) >= 0) {
                    if (n == 0) continue;
                    total += n; if (total > MAX_MESSAGE_BYTES) throw new MessageTooLargeException("decompressed gRPC message is too large");
                    out.write(chunk, 0, n);
                }
                return out.toByteArray();
            }
        }
        return msg;
    }

    static final class MessageTooLargeException extends IOException {
        MessageTooLargeException(String message) { super(message); }
    }
}
