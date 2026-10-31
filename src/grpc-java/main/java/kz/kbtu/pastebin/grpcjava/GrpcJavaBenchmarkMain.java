package kz.kbtu.pastebin.grpcjava;

import kz.kbtu.pastebin.client.ClientOptions;
import kz.kbtu.pastebin.client.PastebinClient;
import kz.kbtu.pastebin.core.PasteService;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.UUID;

/** Separate generated-stub benchmark; its CSV is intentionally not mixed with hand-built gRPC results. */
public final class GrpcJavaBenchmarkMain {
    public static void main(String[] args) throws Exception {
        Path out = Path.of("results", "grpc-java.csv");
        Files.createDirectories(out.getParent());
        try (GrpcJavaPastebinServer server = new GrpcJavaPastebinServer(0, new PasteService(), "http://localhost:8080/p/").start();
             PrintWriter csv = new PrintWriter(Files.newBufferedWriter(out))) {
            csv.println("adapter,operation,payload_bytes,n,mean_ms,p50_ms,p95_ms");
            for (int size : new int[]{1_000, 64_000}) {
                for (String operation : new String[]{"GET", "POST"}) {
                    int n = size == 64_000 ? 200 : 500;
                    String content = "generated grpc-java ".repeat(Math.max(1, size / 20));
                    content = content.substring(0, size);
                    double[] samples = new double[n];
                    try (PastebinClient client = new GrpcJavaPastebinClient("localhost", server.port(), options())) {
                        String key = client.create(content, 3600, UUID.randomUUID().toString()).key();
                        for (int i = 0; i < 50; i++) run(client, operation, content, key);
                        for (int i = 0; i < n; i++) {
                            long start = System.nanoTime(); run(client, operation, content, key);
                            samples[i] = (System.nanoTime() - start) / 1_000_000.0;
                        }
                    }
                    java.util.Arrays.sort(samples);
                    double mean = java.util.Arrays.stream(samples).average().orElseThrow();
                    csv.printf(Locale.ROOT, "generated-grpc-java,%s,%d,%d,%.3f,%.3f,%.3f%n", operation, size, n,
                            mean, pct(samples, .50), pct(samples, .95));
                    System.out.printf(Locale.ROOT, "generated grpc-java %-4s %dB mean=%.3f p50=%.3f p95=%.3f ms%n",
                            operation, size, mean, pct(samples, .50), pct(samples, .95));
                }
            }
        }
    }

    private static ClientOptions options() { ClientOptions o = new ClientOptions(); o.perAttemptTimeoutMs = 5000; o.totalBudgetMs = 20000; return o; }
    private static void run(PastebinClient c, String op, String content, String key) throws Exception {
        if (op.equals("GET")) c.get(key); else c.create(content, 3600, UUID.randomUUID().toString());
    }
    private static double pct(double[] a, double p) { return a[(int) Math.min(a.length - 1, Math.ceil(p * a.length) - 1)]; }
}
