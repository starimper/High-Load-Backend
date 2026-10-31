package kz.kbtu.pastebin.grpc;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Protobuf wire format for the messages in src/main/proto/pastebin.proto.
 * Written by hand because the sandbox could not download protobuf-java; the bytes are the standard
 * Protobuf encoding (checked against protoc output and a real grpcio client, see InteropCheck).
 * Wire types used: 0 = varint (int64), 2 = length-delimited (string).
 */
public final class Proto {
    private Proto() {}

    public record CreateReq(String content, long ttlSeconds) {}
    public record CreateResp(String key, String url, String expiresAt) {}
    public record GetReq(String key) {}
    public record GetResp(String key, String content, String createdAt, String expiresAt) {}

    // ---------- encoding ----------
    private static void varint(ByteArrayOutputStream o, long v) {
        while ((v & ~0x7FL) != 0) {
            o.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        o.write((int) v);
    }

    private static void str(ByteArrayOutputStream o, int field, String s) {
        if (s == null || s.isEmpty()) return; // proto3: default values are not sent
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        varint(o, ((long) field << 3) | 2);
        varint(o, b.length);
        o.writeBytes(b);
    }

    private static void i64(ByteArrayOutputStream o, int field, long v) {
        if (v == 0) return;
        varint(o, ((long) field << 3));
        varint(o, v);
    }

    public static byte[] encode(CreateReq m) {
        var o = new ByteArrayOutputStream();
        str(o, 1, m.content());
        i64(o, 2, m.ttlSeconds());
        return o.toByteArray();
    }

    public static byte[] encode(CreateResp m) {
        var o = new ByteArrayOutputStream();
        str(o, 1, m.key());
        str(o, 2, m.url());
        str(o, 3, m.expiresAt());
        return o.toByteArray();
    }

    public static byte[] encode(GetReq m) {
        var o = new ByteArrayOutputStream();
        str(o, 1, m.key());
        return o.toByteArray();
    }

    public static byte[] encode(GetResp m) {
        var o = new ByteArrayOutputStream();
        str(o, 1, m.key());
        str(o, 2, m.content());
        str(o, 3, m.createdAt());
        str(o, 4, m.expiresAt());
        return o.toByteArray();
    }

    // ---------- decoding ----------
    private static final class Reader {
        final byte[] b;
        int pos;

        Reader(byte[] b) {
            this.b = b;
        }

        boolean more() {
            return pos < b.length;
        }

        long varint() {
            long r = 0;
            int shift = 0;
            while (true) {
                int x = b[pos++];
                r |= (long) (x & 0x7F) << shift;
                if ((x & 0x80) == 0) return r;
                shift += 7;
            }
        }

        String string() {
            int len = (int) varint();
            String s = new String(b, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        void skip(int wireType) {
            switch (wireType) {
                case 0 -> varint();
                case 1 -> pos += 8;
                case 2 -> pos += (int) varint();
                case 5 -> pos += 4;
                default -> throw new IllegalArgumentException("bad wire type " + wireType);
            }
        }
    }

    public static CreateReq decodeCreateReq(byte[] bytes) {
        var r = new Reader(bytes);
        String content = "";
        long ttl = 0;
        while (r.more()) {
            int tag = (int) r.varint();
            switch (tag) {
                case (1 << 3) | 2 -> content = r.string();
                case (2 << 3) -> ttl = r.varint();
                default -> r.skip(tag & 7);
            }
        }
        return new CreateReq(content, ttl);
    }

    public static CreateResp decodeCreateResp(byte[] bytes) {
        var r = new Reader(bytes);
        String key = "", url = "", exp = "";
        while (r.more()) {
            int tag = (int) r.varint();
            switch (tag) {
                case (1 << 3) | 2 -> key = r.string();
                case (2 << 3) | 2 -> url = r.string();
                case (3 << 3) | 2 -> exp = r.string();
                default -> r.skip(tag & 7);
            }
        }
        return new CreateResp(key, url, exp);
    }

    public static GetReq decodeGetReq(byte[] bytes) {
        var r = new Reader(bytes);
        String key = "";
        while (r.more()) {
            int tag = (int) r.varint();
            if (tag == ((1 << 3) | 2)) key = r.string();
            else r.skip(tag & 7);
        }
        return new GetReq(key);
    }

    public static GetResp decodeGetResp(byte[] bytes) {
        var r = new Reader(bytes);
        String key = "", content = "", created = "", exp = "";
        while (r.more()) {
            int tag = (int) r.varint();
            switch (tag) {
                case (1 << 3) | 2 -> key = r.string();
                case (2 << 3) | 2 -> content = r.string();
                case (3 << 3) | 2 -> created = r.string();
                case (4 << 3) | 2 -> exp = r.string();
                default -> r.skip(tag & 7);
            }
        }
        return new GetResp(key, content, created, exp);
    }
}
