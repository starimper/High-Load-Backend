package kz.kbtu.pastebin.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import kz.kbtu.pastebin.core.ErrorCode;
import kz.kbtu.pastebin.core.FaultInjector;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.core.ServiceException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** REST/JSON adapter (HTTP/1.1 + JSON) over PasteService: the baseline for the comparison. */
public final class RestPastebinServer {
    private static final String PATH = "/v1/pastes";

    private final HttpServer server;
    private final ExecutorService executor;
    private final PasteService service;
    private final FaultInjector faults;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    public RestPastebinServer(int port, PasteService service, FaultInjector faults, String baseUrl, int threads)
            throws IOException {
        this.service = service;
        this.faults = faults;
        this.baseUrl = baseUrl;
        this.executor = Executors.newFixedThreadPool(threads);
        this.server = HttpServer.create(new InetSocketAddress(port), 512);
        this.server.setExecutor(executor);
        this.server.createContext(PATH, this::handle);
    }

    public RestPastebinServer start() {
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            if (method.equals("POST") && path.equals(PATH)) {
                create(ex);
            } else if (method.equals("GET") && path.startsWith(PATH + "/")) {
                get(ex, path.substring(PATH.length() + 1));
            } else {
                respond(ex, 404, Map.of("error", "NOT_FOUND", "message", "unknown path"));
            }
        } catch (ServiceException e) {
            respondError(ex, e);
        } catch (Exception e) {
            respond(ex, 400, Map.of("error", "INVALID_ARGUMENT", "message", String.valueOf(e.getMessage())));
        } finally {
            ex.close();
        }
    }

    private void create(HttpExchange ex) throws IOException {
        byte[] body = readBody(ex);
        faults.before();
        JsonNode json = mapper.readTree(body);
        if (json == null || !json.hasNonNull("content") || !json.hasNonNull("ttl_seconds")) {
            throw new ServiceException(ErrorCode.INVALID_ARGUMENT, "content and ttl_seconds are required");
        }
        String idem = ex.getRequestHeaders().getFirst("Idempotency-Key");
        var created = service.create(json.get("content").asText(), json.get("ttl_seconds").asLong(), idem);
        faults.after();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("key", created.key());
        out.put("url", baseUrl + created.key());
        out.put("expires_at", created.expiresAt().toString());
        if (created.replayed()) {
            ex.getResponseHeaders().set("Idempotent-Replayed", "true");
        }
        respond(ex, 201, out);
    }

    private void get(HttpExchange ex, String key) throws IOException {
        faults.before();
        var p = service.get(key);
        faults.after();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("key", p.key());
        out.put("content", p.content());
        out.put("created_at", p.createdAt().toString());
        out.put("expires_at", p.expiresAt().toString());
        respond(ex, 200, out);
    }

    private static byte[] readBody(HttpExchange ex) throws IOException {
        InputStream in = ex.getRequestBody();
        if ("gzip".equalsIgnoreCase(ex.getRequestHeaders().getFirst("Content-Encoding"))) {
            in = new GZIPInputStream(in);
        }
        return in.readAllBytes();
    }

    private void respondError(HttpExchange ex, ServiceException e) throws IOException {
        int status = switch (e.code()) {
            case INVALID_ARGUMENT -> 400;
            case TOO_LARGE -> 413;
            case NOT_FOUND -> 404;
            case IDEMPOTENCY_KEY_REUSED -> 422;
            case UNAVAILABLE -> 503;
        };
        if (e.code() == ErrorCode.UNAVAILABLE) {
            ex.getResponseHeaders().set("Retry-After", "1");
        }
        respond(ex, status, Map.of("error", e.code().name(), "message", e.getMessage()));
    }

    private void respond(HttpExchange ex, int status, Object payload) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(payload);
        String accept = ex.getRequestHeaders().getFirst("Accept-Encoding");
        if (accept != null && accept.contains("gzip") && bytes.length > 1024) { // below ~1 KB gzip does not pay off
            ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length / 2);
            try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
                gz.write(bytes);
            }
            bytes = bos.toByteArray();
            ex.getResponseHeaders().set("Content-Encoding", "gzip");
        }
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
