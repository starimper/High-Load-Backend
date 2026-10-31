package kz.kbtu.pastebin.grpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import kz.kbtu.pastebin.client.ClientOptions;
import kz.kbtu.pastebin.client.CreateResult;
import kz.kbtu.pastebin.client.GetResult;
import kz.kbtu.pastebin.client.PastebinClient;
import kz.kbtu.pastebin.client.RetryExecutor;
import kz.kbtu.pastebin.client.RpcFailure;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** gRPC client: one HTTP/2 connection, every call is a new stream on it (requests can overlap). */
public final class GrpcPastebinClient implements PastebinClient {
    private final String host;
    private final int port;
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final RetryExecutor executor;
    private final ClientOptions options;
    private volatile Channel connection;
    private final SslContext sslContext;

    public GrpcPastebinClient(String host, int port, ClientOptions options) {
        this.host = host;
        this.port = port;
        this.options = options;
        this.executor = new RetryExecutor(options);
        try {
            this.sslContext = options.tls ? SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build() : null;
        } catch (Exception e) { throw new IllegalStateException("cannot initialize TLS", e); }
    }

    private synchronized Channel connection(long timeoutMs) throws RpcFailure {
        Channel c = connection;
        if (c != null && c.isActive()) return c;
        try {
            var cf = new Bootstrap().group(group).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) Math.max(1, timeoutMs))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            if (sslContext != null) ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), host, port));
                            ch.pipeline().addLast(
                                    Http2FrameCodecBuilder.forClient()
                                            .initialSettings(Http2Settings.defaultSettings().initialWindowSize(4 * 1024 * 1024))
                                            .build(),
                                    new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
                        }
                    }).connect(host, port);
            if (!cf.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                cf.cancel(true);
                throw new TimeoutException();
            }
            if (!cf.isSuccess()) throw new ExecutionException(cf.cause());
            c = cf.channel();
            connection = c;
            return c;
        } catch (TimeoutException e) {
            throw new RpcFailure(RpcFailure.Kind.RETRYABLE, "DEADLINE_EXCEEDED", "connect timeout", e);
        } catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new RpcFailure(RpcFailure.Kind.RETRYABLE, "UNAVAILABLE", "cannot connect: " + e.getMessage(), e);
        }
    }

    private static final class Result {
        int status = -1;
        String message = "";
        byte[] data = new byte[0];
    }

    /** One call = one HTTP/2 stream: HEADERS, DATA(end), then wait for HEADERS, DATA, trailers. */
    private byte[] call(String path, byte[] request, String idempotencyKey, long timeoutMs) throws RpcFailure {
        long start = System.nanoTime();
        Channel conn = connection(timeoutMs);
        CompletableFuture<Result> done = new CompletableFuture<>();
        Http2StreamChannel stream = null;
        try {
            stream = new Http2StreamChannelBootstrap(conn).handler(new ChannelInboundHandlerAdapter() {
                private final Result r = new Result();
                private final ByteArrayOutputStream body = new ByteArrayOutputStream();

                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    if (msg instanceof Http2HeadersFrame hf) {
                        Http2Headers h = hf.headers();
                        CharSequence s = h.get("grpc-status");
                        if (s != null) {
                            r.status = Integer.parseInt(s.toString());
                            CharSequence m = h.get("grpc-message");
                            if (m != null) r.message = m.toString();
                        } else if (hf.isEndStream()) {
                            r.status = GrpcWire.INTERNAL;
                            r.message = "HTTP " + h.status();
                        }
                        if (hf.isEndStream()) finish();
                    } else if (msg instanceof Http2DataFrame df) {
                        ByteBuf c = df.content();
                        byte[] tmp = new byte[c.readableBytes()];
                        c.readBytes(tmp);
                        body.writeBytes(tmp);
                        boolean end = df.isEndStream();
                        ReferenceCountUtil.release(df);
                        if (end) finish();
                    } else {
                        ReferenceCountUtil.release(msg);
                    }
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) {
                    if (!done.isDone()) {
                        r.status = GrpcWire.UNAVAILABLE;
                        r.message = "connection closed";
                        finish();
                    }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                    r.status = GrpcWire.UNAVAILABLE;
                    r.message = String.valueOf(cause.getMessage());
                    finish();
                }

                private void finish() {
                    r.data = body.toByteArray();
                    done.complete(r);
                }
            }).open().get(timeoutMs, TimeUnit.MILLISECONDS);

            Http2Headers h = new DefaultHttp2Headers().method("POST").scheme("http").authority(host + ":" + port)
                    .path(path).set("content-type", "application/grpc").set("te", "trailers")
                    .set("grpc-timeout", timeoutMs + "m");
            if (idempotencyKey != null) h.set("idempotency-key", idempotencyKey);
            if (options.gzip) h.set("grpc-encoding", "gzip").set("grpc-accept-encoding", "gzip");
            boolean gz = options.gzip && request.length > 1024;
            stream.write(new DefaultHttp2HeadersFrame(h, false));
            stream.writeAndFlush(new DefaultHttp2DataFrame(GrpcWire.frame(request, gz), true));

            long left = timeoutMs - (System.nanoTime() - start) / 1_000_000L;
            Result r = done.get(Math.max(1, left), TimeUnit.MILLISECONDS);
            if (r.status != GrpcWire.OK) throw failure(r.status, r.message);
            return GrpcWire.unframe(r.data);
        } catch (TimeoutException e) {
            throw new RpcFailure(RpcFailure.Kind.RETRYABLE, "DEADLINE_EXCEEDED", "deadline " + timeoutMs + " ms", e);
        } catch (ExecutionException | java.io.IOException e) {
            throw new RpcFailure(RpcFailure.Kind.RETRYABLE, "UNAVAILABLE", String.valueOf(e.getMessage()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcFailure(RpcFailure.Kind.REJECTED, "INTERRUPTED", "interrupted", e);
        } finally {
            if (stream != null) stream.close(); // on timeout this sends RST_STREAM(CANCEL) to the server
        }
    }

    private static RpcFailure failure(int code, String message) {
        return switch (code) {
            case GrpcWire.UNAVAILABLE -> new RpcFailure(RpcFailure.Kind.RETRYABLE, "UNAVAILABLE", message, null);
            case GrpcWire.DEADLINE_EXCEEDED -> new RpcFailure(RpcFailure.Kind.RETRYABLE, "DEADLINE_EXCEEDED", message, null);
            case GrpcWire.NOT_FOUND -> new RpcFailure(RpcFailure.Kind.NOT_FOUND, "NOT_FOUND", message, null);
            default -> new RpcFailure(RpcFailure.Kind.REJECTED, "GRPC_" + code, message, null);
        };
    }

    @Override
    public CreateResult create(String content, long ttlSeconds, String idempotencyKey) throws RpcFailure {
        boolean safe = idempotencyKey != null || options.retryCreateWithoutKey;
        byte[] req = Proto.encode(new Proto.CreateReq(content, ttlSeconds));
        return executor.execute(safe, t -> {
            try {
                var r = Proto.decodeCreateResp(call(GrpcWire.PATH_CREATE, req, idempotencyKey, t));
                return new CreateResult(r.key(), r.url(), r.expiresAt());
            } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
                throw new RpcFailure(RpcFailure.Kind.REJECTED, "BAD_RESPONSE", e.toString(), e);
            }
        });
    }

    @Override
    public GetResult get(String key) throws RpcFailure {
        byte[] req = Proto.encode(new Proto.GetReq(key));
        return executor.execute(true, t -> {
            var r = Proto.decodeGetResp(call(GrpcWire.PATH_GET, req, null, t));
            return new GetResult(r.key(), r.content(), r.createdAt(), r.expiresAt());
        });
    }

    @Override
    public RetryExecutor executor() {
        return executor;
    }

    @Override
    public void close() {
        Channel c = connection;
        if (c != null) c.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
}
