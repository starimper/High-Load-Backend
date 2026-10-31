package kz.kbtu.pastebin.grpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCountUtil;
import kz.kbtu.pastebin.core.FaultInjector;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.core.ServiceException;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * gRPC server: HTTP/2 (Netty) + Protobuf messages + gRPC framing and status trailers.
 * Same PasteService as the REST server. The idempotency key arrives as metadata "idempotency-key"
 * (an HTTP/2 header), the counterpart of the REST header "Idempotency-Key".
 */
public final class GrpcPastebinServer {
    private final EventLoopGroup group = new NioEventLoopGroup(2);
    private final ExecutorService workers;
    private final PasteService service;
    private final FaultInjector faults;
    private final String baseUrl;
    private Channel server;
    private final int requestedPort;
    private final SslContext sslContext;

    public GrpcPastebinServer(int port, PasteService service, FaultInjector faults, String baseUrl, int threads) {
        this(port, service, faults, baseUrl, threads, null);
    }

    public GrpcPastebinServer(int port, PasteService service, FaultInjector faults, String baseUrl, int threads,
                              SslContext sslContext) {
        this.requestedPort = port;
        this.service = service;
        this.faults = faults;
        this.baseUrl = baseUrl;
        this.workers = new ThreadPoolExecutor(threads, threads, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), new ThreadPoolExecutor.AbortPolicy());
        this.sslContext = sslContext;
    }

    public GrpcPastebinServer start() throws InterruptedException {
        server = new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                        protected void initChannel(SocketChannel ch) {
                            if (sslContext != null) ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                            ch.pipeline().addLast(
                                Http2FrameCodecBuilder.forServer()
                                        .initialSettings(Http2Settings.defaultSettings().initialWindowSize(4 * 1024 * 1024))
                                        .build(),
                                new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                                    @Override
                                    protected void initChannel(Channel stream) {
                                        stream.pipeline().addLast(new CallHandler());
                                    }
                                }));
                    }
                }).bind("127.0.0.1", requestedPort).sync().channel();
        return this;
    }

    public int port() {
        return ((java.net.InetSocketAddress) server.localAddress()).getPort();
    }

    public void stop() {
        if (server != null) server.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        workers.shutdownNow();
    }

    /** One HTTP/2 stream = one call. */
    private final class CallHandler extends ChannelInboundHandlerAdapter {
        private Http2Headers headers;
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        private volatile boolean cancelled;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Http2HeadersFrame hf) {
                headers = hf.headers();
                if (hf.isEndStream()) dispatch(ctx);
            } else if (msg instanceof Http2DataFrame df) {
                ByteBuf content = df.content();
                byte[] tmp = new byte[content.readableBytes()];
                content.readBytes(tmp);
                if (body.size() + tmp.length > GrpcWire.MAX_MESSAGE_BYTES) {
                    ReferenceCountUtil.release(df);
                    trailersOnly(ctx, GrpcWire.OUT_OF_RANGE, "request body is too large");
                    cancelled = true;
                    return;
                }
                body.writeBytes(tmp);
                boolean end = df.isEndStream();
                ReferenceCountUtil.release(df);
                if (end) dispatch(ctx);
            } else if (msg instanceof Http2ResetFrame) {
                cancelled = true; // client gave up (deadline) - nobody waits for the answer
            } else {
                ReferenceCountUtil.release(msg);
            }
        }

        private void dispatch(ChannelHandlerContext ctx) {
            Http2Headers h = headers;
            byte[] data = body.toByteArray();
            try { workers.execute(() -> process(ctx, h, data)); }
            catch (RejectedExecutionException e) { trailersOnly(ctx, GrpcWire.UNAVAILABLE, "server is overloaded"); }
        }

        private void process(ChannelHandlerContext ctx, Http2Headers h, byte[] data) {
            String path = h.path() == null ? "" : h.path().toString();
            String idem = h.get("idempotency-key") == null ? null : h.get("idempotency-key").toString();
            CharSequence acc = h.get("grpc-accept-encoding");
            boolean clientAcceptsGzip = acc != null && acc.toString().contains("gzip");
            try {
                byte[] reply;
                faults.before();
                if (cancelled) return;
                if (path.equals(GrpcWire.PATH_CREATE)) {
                    var req = Proto.decodeCreateReq(GrpcWire.unframe(data));
                    var c = service.create(req.content(), req.ttlSeconds(), idem);
                    faults.after();
                    reply = Proto.encode(new Proto.CreateResp(c.key(), baseUrl + c.key(), c.expiresAt().toString()));
                } else if (path.equals(GrpcWire.PATH_GET)) {
                    var req = Proto.decodeGetReq(GrpcWire.unframe(data));
                    var p = service.get(req.key());
                    faults.after();
                    reply = Proto.encode(new Proto.GetResp(p.key(), p.content(), p.createdAt().toString(),
                            p.expiresAt().toString()));
                } else {
                    trailersOnly(ctx, 12, "unknown method " + path); // UNIMPLEMENTED
                    return;
                }
                if (cancelled) return;
                boolean gz = clientAcceptsGzip && reply.length > 1024;
                Http2Headers rh = new DefaultHttp2Headers().status("200").set("content-type", "application/grpc");
                if (gz) rh.set("grpc-encoding", "gzip");
                ctx.write(new DefaultHttp2HeadersFrame(rh, false));
                ctx.write(new DefaultHttp2DataFrame(GrpcWire.frame(reply, gz), false));
                ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().set("grpc-status", "0"), true));
            } catch (GrpcWire.MessageTooLargeException e) {
                trailersOnly(ctx, GrpcWire.OUT_OF_RANGE, e.getMessage());
            } catch (ServiceException e) {
                trailersOnly(ctx, toCode(e), e.getMessage());
            } catch (Exception e) {
                trailersOnly(ctx, GrpcWire.INTERNAL, String.valueOf(e.getMessage()));
            }
        }

        private void trailersOnly(ChannelHandlerContext ctx, int code, String message) {
            Http2Headers t = new DefaultHttp2Headers().status("200").set("content-type", "application/grpc")
                    .set("grpc-status", Integer.toString(code))
                    .set("grpc-message", message == null ? "" : message.replaceAll("[^\\x20-\\x7E]", "?"));
            ctx.writeAndFlush(new DefaultHttp2HeadersFrame(t, true));
        }
    }

    static int toCode(ServiceException e) {
        return switch (e.code()) {
            case INVALID_ARGUMENT -> GrpcWire.INVALID_ARGUMENT;
            case TOO_LARGE -> GrpcWire.OUT_OF_RANGE;
            case NOT_FOUND -> GrpcWire.NOT_FOUND;
            case IDEMPOTENCY_KEY_REUSED -> GrpcWire.FAILED_PRECONDITION;
            case UNAVAILABLE -> GrpcWire.UNAVAILABLE;
        };
    }
}
