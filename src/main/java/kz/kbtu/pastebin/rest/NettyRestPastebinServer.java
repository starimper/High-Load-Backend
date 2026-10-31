package kz.kbtu.pastebin.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslContext;
import io.netty.util.CharsetUtil;
import kz.kbtu.pastebin.core.ErrorCode;
import kz.kbtu.pastebin.core.FaultInjector;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.core.ServiceException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Netty HTTP/1.1 REST adapter used for the fair protocol benchmark. */
public final class NettyRestPastebinServer {
    private static final String PATH = "/v1/pastes";
    private static final int MAX_REQUEST_BYTES = 1_100_000; // 1 MiB content plus JSON/header overhead
    private static final int APPLICATION_QUEUE_CAPACITY = 256;
    private final PasteService service;
    private final FaultInjector faults;
    private final String baseUrl;
    private final int requestedPort;
    private final int threads;
    private final SslContext sslContext;
    private final EventLoopGroup acceptor = new NioEventLoopGroup(1);
    private final EventLoopGroup workers = new NioEventLoopGroup(2);
    private final ExecutorService application;
    private final ObjectMapper mapper = new ObjectMapper();
    private Channel server;

    public NettyRestPastebinServer(int port, PasteService service, FaultInjector faults, String baseUrl, int threads) {
        this(port, service, faults, baseUrl, threads, null);
    }

    public NettyRestPastebinServer(int port, PasteService service, FaultInjector faults, String baseUrl, int threads,
                                   SslContext sslContext) {
        this.requestedPort = port;
        this.service = service;
        this.faults = faults;
        this.baseUrl = baseUrl;
        this.threads = threads;
        this.sslContext = sslContext;
        this.application = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(APPLICATION_QUEUE_CAPACITY), new ThreadPoolExecutor.AbortPolicy());
    }

    public NettyRestPastebinServer start() throws InterruptedException {
        server = new ServerBootstrap().group(acceptor, workers).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        if (sslContext != null) ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                        ch.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(MAX_REQUEST_BYTES),
                                new Handler());
                    }
                }).bind("127.0.0.1", requestedPort).sync().channel();
        return this;
    }

    public int port() { return ((InetSocketAddress) server.localAddress()).getPort(); }

    public void stop() {
        if (server != null) server.close();
        acceptor.shutdownGracefully();
        workers.shutdownGracefully();
        application.shutdownNow();
    }

    private final class Handler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            req.retain();
            try {
                application.execute(() -> { try { handle(ctx, req); } finally { req.release(); } });
            } catch (RejectedExecutionException e) {
                req.release();
                respond(ctx, req, 503, Map.of("error", "UNAVAILABLE", "message", "server is overloaded"));
            }
        }

        private void handle(ChannelHandlerContext ctx, FullHttpRequest req) {
            try {
                String path = new QueryStringDecoder(req.uri()).path();
                if (req.method().equals(HttpMethod.GET) && path.equals("/")) browserHome(ctx, req);
                else if (req.method().equals(HttpMethod.GET) && path.startsWith("/p/")) browserPaste(ctx, req, path.substring(3));
                else if (req.method().equals(HttpMethod.POST) && path.equals(PATH)) create(ctx, req);
                else if (req.method().equals(HttpMethod.GET) && path.startsWith(PATH + "/"))
                    get(ctx, req, path.substring(PATH.length() + 1));
                else respond(ctx, req, 404, Map.of("error", "NOT_FOUND", "message", "unknown path"));
            } catch (ServiceException e) { respondError(ctx, req, e); }
            catch (Exception e) { respond(ctx, req, 400, Map.of("error", "INVALID_ARGUMENT", "message", String.valueOf(e.getMessage()))); }
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof io.netty.handler.codec.TooLongFrameException) {
                byte[] body = "{\"error\":\"TOO_LARGE\",\"message\":\"request body is too large\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
                        io.netty.buffer.Unpooled.wrappedBuffer(body));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                HttpUtil.setContentLength(response, body.length);
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            } else ctx.close();
        }

        private void create(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
            byte[] body = readBody(req);
            faults.before();
            JsonNode json = mapper.readTree(body);
            if (json == null || !json.hasNonNull("content") || !json.hasNonNull("ttl_seconds"))
                throw new ServiceException(ErrorCode.INVALID_ARGUMENT, "content and ttl_seconds are required");
            var c = service.create(json.get("content").asText(), json.get("ttl_seconds").asLong(),
                    req.headers().get("Idempotency-Key"));
            faults.after();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("key", c.key()); out.put("url", baseUrl + c.key()); out.put("expires_at", c.expiresAt().toString());
            respond(ctx, req, 201, out);
        }

        private void get(ChannelHandlerContext ctx, FullHttpRequest req, String key) throws Exception {
            faults.before(); var p = service.get(key); faults.after();
            respond(ctx, req, 200, Map.of("key", p.key(), "content", p.content(),
                    "created_at", p.createdAt().toString(), "expires_at", p.expiresAt().toString()));
        }

        private void browserHome(ChannelHandlerContext ctx, FullHttpRequest req) {
            respondHtml(ctx, req, """
                    <!doctype html><html><head><meta charset="utf-8"><title>Local Pastebin</title>
                    <style>body{font:16px system-ui;max-width:760px;margin:40px auto;padding:0 20px}textarea{width:100%;height:280px}input,button{padding:10px;margin:8px 4px 8px 0}#result{margin-top:20px}</style></head>
                    <body><h1>Local Pastebin</h1><p>Create a text snippet and receive a local link.</p>
                    <textarea id="content" placeholder="Write or paste text here"></textarea><br>
                    <label>TTL seconds <input id="ttl" type="number" min="600" value="3600"></label>
                    <button id="create" onclick="createPaste()">Create snippet</button><div id="result"></div>
                    <script>
                    let browserIdempotencyKey=null,browserRequestSignature=null;
                    async function createPaste(){const result=document.getElementById('result');const button=document.getElementById('create');
                      const content=document.getElementById('content').value;const ttl=Number(document.getElementById('ttl').value);const signature=content+'\\n'+ttl;
                      if(browserRequestSignature!==signature){browserRequestSignature=signature;browserIdempotencyKey=crypto.randomUUID();}
                      const requestKey=browserIdempotencyKey;button.disabled=true;result.textContent='Creating...';
                      try{const r=await fetch('/v1/pastes',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':requestKey},body:JSON.stringify({content:content,ttl_seconds:ttl})});
                      const j=await r.json(); if(!r.ok) throw new Error(j.message||j.error); result.innerHTML=(j.replayed?'Already created: ':'Open snippet: ')+'<a href="/p/'+j.key+'">'+location.origin+'/p/'+j.key+'</a>';}
                      catch(e){result.textContent='Error: '+e.message+' Retry will reuse the same Idempotency-Key.';}finally{button.disabled=false;}}
                    </script></body></html>
                    """);
        }

        private void browserPaste(ChannelHandlerContext ctx, FullHttpRequest req, String key) {
            try {
                var p = service.get(key);
                respondHtml(ctx, req, "<!doctype html><html><head><meta charset=\"utf-8\"><title>Paste " + html(p.key())
                        + "</title><style>body{font:16px system-ui;max-width:900px;margin:40px auto;padding:0 20px}pre{white-space:pre-wrap;background:#f4f4f4;padding:20px;border-radius:8px}</style></head><body><p><a href=\"/\">Create another</a></p><h1>Paste "
                        + html(p.key()) + "</h1><p>Expires: " + html(p.expiresAt().toString()) + "</p><pre>" + html(p.content()) + "</pre></body></html>");
            } catch (ServiceException e) { respondHtml(ctx, req, 404, "<!doctype html><h1>Not found</h1><p>" + html(e.getMessage()) + "</p>"); }
        }

        private void respondHtml(ChannelHandlerContext ctx, FullHttpRequest req, String html) {
            respondHtml(ctx, req, 200, html);
        }

        private void respondHtml(ChannelHandlerContext ctx, FullHttpRequest req, int status, String html) {
            byte[] bytes = html.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status),
                    io.netty.buffer.Unpooled.wrappedBuffer(bytes));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            HttpUtil.setContentLength(response, bytes.length);
            boolean keepAlive = HttpUtil.isKeepAlive(req);
            if (keepAlive) response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response).addListener(keepAlive ? ChannelFutureListener.CLOSE_ON_FAILURE : ChannelFutureListener.CLOSE);
        }

        private String html(String value) {
            return value == null ? "" : value.replace("&", "&amp;").replace("<", "&lt;")
                    .replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;");
        }

        private byte[] readBody(FullHttpRequest req) throws Exception {
            ByteBuf b = req.content();
            if (b.readableBytes() > MAX_REQUEST_BYTES) throw new ServiceException(ErrorCode.TOO_LARGE, "request body is too large");
            byte[] bytes = new byte[b.readableBytes()]; b.getBytes(b.readerIndex(), bytes);
            if (HttpHeaderValues.GZIP.contentEqualsIgnoreCase(req.headers().get(HttpHeaderNames.CONTENT_ENCODING)))
                try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) { return readLimited(in, MAX_REQUEST_BYTES); }
            return bytes;
        }

        private byte[] readLimited(java.io.InputStream in, int limit) throws java.io.IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(limit, 8192));
            byte[] chunk = new byte[8192]; int total = 0; int n;
            while ((n = in.read(chunk)) >= 0) {
                if (n == 0) continue;
                total += n; if (total > limit) throw new ServiceException(ErrorCode.TOO_LARGE, "decompressed request body is too large");
                out.write(chunk, 0, n);
            }
            return out.toByteArray();
        }

        private void respondError(ChannelHandlerContext ctx, FullHttpRequest req, ServiceException e) {
            int status = switch (e.code()) { case INVALID_ARGUMENT -> 400; case TOO_LARGE -> 413; case NOT_FOUND -> 404;
                case IDEMPOTENCY_KEY_REUSED -> 422; case UNAVAILABLE -> 503; };
            respond(ctx, req, status, Map.of("error", e.code().name(), "message", e.getMessage()));
        }

        private void respond(ChannelHandlerContext ctx, FullHttpRequest req, int status, Object payload) {
            try {
                byte[] bytes = mapper.writeValueAsBytes(payload);
                String accept = req.headers().get(HttpHeaderNames.ACCEPT_ENCODING);
                if (accept != null && accept.contains("gzip") && bytes.length > 1024) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length / 2);
                    try (GZIPOutputStream gz = new GZIPOutputStream(out)) { gz.write(bytes); }
                    bytes = out.toByteArray();
                }
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                        HttpResponseStatus.valueOf(status), io.netty.buffer.Unpooled.wrappedBuffer(bytes));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                if (accept != null && accept.contains("gzip") && bytes.length > 0 && payload != null
                        && bytes[0] == 0x1f) response.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");
                HttpUtil.setContentLength(response, bytes.length);
                boolean keepAlive = HttpUtil.isKeepAlive(req);
                if (keepAlive) response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                ctx.writeAndFlush(response).addListener(keepAlive ? ChannelFutureListener.CLOSE_ON_FAILURE : ChannelFutureListener.CLOSE);
            } catch (Exception e) { ctx.close(); }
        }
    }
}
