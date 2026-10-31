package kz.kbtu.pastebin.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kz.kbtu.pastebin.client.ClientOptions;
import kz.kbtu.pastebin.client.CreateResult;
import kz.kbtu.pastebin.client.GetResult;
import kz.kbtu.pastebin.client.PastebinClient;
import kz.kbtu.pastebin.client.RetryExecutor;
import kz.kbtu.pastebin.client.RpcFailure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class RestPastebinClient implements PastebinClient {
    private final URI base;
    private final HttpClient http;
    private final RetryExecutor executor;
    private final ClientOptions options;
    private final ObjectMapper mapper = new ObjectMapper();

    public RestPastebinClient(String host, int port, ClientOptions options) {
        this.options = options;
        this.base = URI.create((options.tls ? "https://" : "http://") + host + ":" + port);
        // HTTP/1.1 with keep-alive: the usual REST setup and the baseline of the comparison
        HttpClient.Builder builder = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(options.perAttemptTimeoutMs));
        if (options.tls) {
            try { builder.sslContext(insecureContext()); } catch (Exception e) { throw new IllegalStateException(e); }
        }
        this.http = builder.build();
        this.executor = new RetryExecutor(options);
    }

    @Override
    public CreateResult create(String content, long ttlSeconds, String idempotencyKey) throws RpcFailure {
        boolean safe = idempotencyKey != null || options.retryCreateWithoutKey;
        return executor.execute(safe, timeoutMs -> {
            try {
                ObjectNode body = mapper.createObjectNode();
                body.put("content", content);
                body.put("ttl_seconds", ttlSeconds);
                byte[] bytes = mapper.writeValueAsBytes(body);
                HttpRequest.Builder rb = HttpRequest.newBuilder(base.resolve("/v1/pastes"))
                        .timeout(Duration.ofMillis(timeoutMs))
                        .header("Content-Type", "application/json");
                if (idempotencyKey != null) {
                    rb.header("Idempotency-Key", idempotencyKey);
                }
                // Compress responses when requested. Request-body compression is intentionally
                // omitted from the REST baseline: ordinary REST clients usually send JSON
                // uncompressed, so enabling it would make the comparison less representative.
                if (options.gzip) rb.header("Accept-Encoding", "gzip");
                HttpResponse<byte[]> r = http.send(rb.POST(HttpRequest.BodyPublishers.ofByteArray(bytes)).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                JsonNode json = parse(r);
                return new CreateResult(json.get("key").asText(), json.get("url").asText(), json.get("expires_at").asText());
            } catch (IOException e) {
                throw ioFailure(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcFailure(RpcFailure.Kind.REJECTED, "INTERRUPTED", "interrupted", e);
            }
        });
    }

    @Override
    public GetResult get(String key) throws RpcFailure {
        return executor.execute(true, timeoutMs -> {
            try {
                HttpRequest.Builder rb = HttpRequest.newBuilder(base.resolve("/v1/pastes/" + key))
                        .timeout(Duration.ofMillis(timeoutMs));
                if (options.gzip) {
                    rb.header("Accept-Encoding", "gzip");
                }
                HttpResponse<byte[]> r = http.send(rb.GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                JsonNode json = parse(r);
                return new GetResult(json.get("key").asText(), json.get("content").asText(),
                        json.get("created_at").asText(), json.get("expires_at").asText());
            } catch (IOException e) {
                throw ioFailure(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcFailure(RpcFailure.Kind.REJECTED, "INTERRUPTED", "interrupted", e);
            }
        });
    }

    private JsonNode parse(HttpResponse<byte[]> r) throws IOException, RpcFailure {
        byte[] body = r.body();
        if ("gzip".equalsIgnoreCase(r.headers().firstValue("Content-Encoding").orElse(""))) {
            try (GZIPInputStream in = new GZIPInputStream(new java.io.ByteArrayInputStream(body))) {
                body = in.readAllBytes();
            }
        }
        int status = r.statusCode();
        if (status == 200 || status == 201) {
            return mapper.readTree(body);
        }
        String message = new String(body, java.nio.charset.StandardCharsets.UTF_8);
        RpcFailure.Kind kind = switch (status) {
            case 503, 429 -> RpcFailure.Kind.RETRYABLE;
            case 404 -> RpcFailure.Kind.NOT_FOUND;
            default -> RpcFailure.Kind.REJECTED;
        };
        throw new RpcFailure(kind, "HTTP " + status, message, null);
    }

    private static RpcFailure ioFailure(IOException e) {
        // timeouts and connection problems are temporary
        String status = e instanceof HttpTimeoutException ? "DEADLINE_EXCEEDED" : "UNAVAILABLE";
        return new RpcFailure(RpcFailure.Kind.RETRYABLE, status, String.valueOf(e.getMessage()), e);
    }

    @Override
    public RetryExecutor executor() {
        return executor;
    }

    @Override
    public void close() {
        // java.net.http.HttpClient needs no explicit close here
    }

    private static SSLContext insecureContext() throws Exception {
        TrustManager[] trust = {new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) {}
        }};
        SSLContext c = SSLContext.getInstance("TLS"); c.init(null, trust, new java.security.SecureRandom()); return c;
    }
}
