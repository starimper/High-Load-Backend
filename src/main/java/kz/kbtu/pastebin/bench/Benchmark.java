package kz.kbtu.pastebin.bench;

import kz.kbtu.pastebin.client.*;
import kz.kbtu.pastebin.core.FaultInjector;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.grpc.GrpcPastebinClient;
import kz.kbtu.pastebin.grpc.GrpcPastebinServer;
import kz.kbtu.pastebin.rest.RestPastebinClient;
import kz.kbtu.pastebin.rest.NettyRestPastebinServer;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measurements: latency, payload size on the wire, throughput, reliability.
 * Usage: Benchmark <latency|bytes|throughput|mixed|reliability> --host H --grpc-port P --rest-port P --out results
 * (latency/bytes/throughput talk to a ServerMain running in ANOTHER process; reliability starts its own servers)
 */
public final class Benchmark {
    static final int[] SIZES = {100, 1_000, 10_000, 64_000, 1_000_000};
    static String host = "localhost";
    static int grpcPort = 9090, restPort = 8080;
    static Path out = Path.of("results");
    static boolean tls = false;
    static int repeats = 1;

    static ClientOptions opts(boolean gzip) {
        ClientOptions o = new ClientOptions();
        o.gzip = gzip;
        o.perAttemptTimeoutMs = 5000; // generous: this is a measurement, we do not want timeouts to distort it
        o.totalBudgetMs = 20000;
        o.retriesEnabled = false;
        o.tls = tls;
        return o;
    }

    static PastebinClient client(String proto, int port, boolean gzip) {
        return proto.equals("grpc") ? new GrpcPastebinClient(host, port, opts(gzip))
                : new RestPastebinClient(host, port, opts(gzip));
    }

    interface Op { void run() throws Exception; }

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        for (int i = 1; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--host" -> host = args[i + 1];
                case "--grpc-port" -> grpcPort = Integer.parseInt(args[i + 1]);
                case "--rest-port" -> restPort = Integer.parseInt(args[i + 1]);
                case "--out" -> out = Path.of(args[i + 1]);
                case "--tls" -> tls = Boolean.parseBoolean(args[i + 1]);
                case "--repeats" -> repeats = Math.max(1, Integer.parseInt(args[i + 1]));
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        Path root = out;
        for (int run = 1; run <= repeats; run++) {
            out = repeats == 1 ? root : root.resolve("run-" + String.format(Locale.ROOT, "%02d", run));
            Files.createDirectories(out);
            switch (mode) {
                case "latency" -> latency();
                case "bytes" -> bytes();
                case "throughput" -> throughput();
                case "mixed" -> mixed();
                case "reliability" -> reliability();
                default -> throw new IllegalArgumentException(mode);
            }
        }
        System.exit(0);
    }

    static int iterations(int size, boolean create) {
        if (size >= 1_000_000) return create ? 40 : 150;
        if (size >= 64_000) return 600;
        return 2000;
    }

    // ------------------------------------------------------------------ latency
    static void latency() throws Exception {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("latency.csv")))) {
            w.println("protocol,operation,payload_bytes,gzip,n,mean_ms,p50_ms,p95_ms,p99_ms,max_ms");
            for (boolean gzip : new boolean[]{false, true}) {
                for (int size : SIZES) {
                    if (gzip && size < 10_000) continue; // gzip does not pay off below ~1 KB, and is not used there
                    for (String opName : new String[]{"GET", "POST"}) {
                        Map<String, List<Double>> samples = new LinkedHashMap<>();
                        samples.put("grpc", new ArrayList<>());
                        samples.put("rest", new ArrayList<>());
                        int rounds = 3;
                        int n = iterations(size, opName.equals("POST"));
                        int per = n / rounds;
                        for (int round = 0; round < rounds; round++) {
                            // alternate the order in every round so neither protocol always goes first
                            List<String> order = round % 2 == 0 ? List.of("grpc", "rest") : List.of("rest", "grpc");
                            for (String proto : order) {
                                samples.get(proto).addAll(measure(proto, opName, size, gzip, per));
                            }
                        }
                        for (var e : samples.entrySet()) {
                            Stats s = new Stats(e.getValue().stream().mapToDouble(Double::doubleValue).toArray());
                            // With 39 samples, p99 is effectively the maximum and is misleading.
                            // Leave it blank for that case; p95 and max remain available.
                            String p99 = (s.n() < 100) ? "" : String.format(Locale.ROOT, "%.3f", s.pct(99));
                            w.printf(Locale.ROOT, "%s,%s,%d,%s,%d,%.3f,%.3f,%.3f,%s,%.3f%n", e.getKey(), opName, size,
                                    gzip, s.n(), s.mean, s.pct(50), s.pct(95), p99, s.pct(100));
                            System.out.printf(Locale.ROOT, "%-4s %-4s %8d gzip=%-5s n=%d p50=%.3f p95=%.3f %s=%.3f ms%n",
                                    e.getKey(), opName, size, gzip, s.n(), s.pct(50), s.pct(95), s.n() < 100 ? "max" : "p99",
                                    s.n() < 100 ? s.pct(100) : s.pct(99));
                        }
                        w.flush();
                    }
                }
            }
        }
    }

    static List<Double> measure(String proto, String opName, int size, boolean gzip, int count) throws Exception {
        int port = proto.equals("grpc") ? grpcPort : restPort;
        String text = PayloadGenerator.text(size, size);
        List<Double> ms = new ArrayList<>(count);
        try (PastebinClient c = client(proto, port, gzip)) {
            String key = c.create(text, 3600, UUID.randomUUID().toString()).key();
            Op op = opName.equals("GET")
                    ? () -> c.get(key)
                    : () -> c.create(text, 3600, UUID.randomUUID().toString());
            int warm = Math.min(200, Math.max(10, count / 4));
            for (int i = 0; i < warm; i++) op.run();
            for (int i = 0; i < count; i++) {
                long t0 = System.nanoTime();
                op.run();
                ms.add((System.nanoTime() - t0) / 1e6);
            }
        }
        return ms;
    }

    // ------------------------------------------------------------------ bytes on the wire
    static void bytes() throws Exception {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("payload.csv")))) {
            w.println("protocol,operation,content_bytes,gzip,request_bytes,response_bytes,total_bytes,overhead_bytes");
            for (boolean gzip : new boolean[]{false, true}) {
                for (int size : SIZES) {
                    if (gzip && size < 10_000) continue;
                    String text = PayloadGenerator.text(size, size);
                    for (String proto : new String[]{"grpc", "rest"}) {
                        int target = proto.equals("grpc") ? grpcPort : restPort;
                        try (CountingProxy proxy = new CountingProxy(host, target);
                             PastebinClient c = client(proto, proxy.port(), gzip)) {
                            String key = c.create(text, 3600, UUID.randomUUID().toString()).key();
                            for (String opName : new String[]{"GET", "POST"}) {
                                Op op = opName.equals("GET") ? () -> c.get(key)
                                        : () -> c.create(text, 3600, UUID.randomUUID().toString());
                                for (int i = 0; i < 5; i++) op.run(); // warm-up: connection and HTTP/2 settings are done
                                int reps = size >= 1_000_000 ? 5 : 50;
                                proxy.reset();
                                for (int i = 0; i < reps; i++) op.run();
                                Thread.sleep(200); // let in-flight ACK/WINDOW_UPDATE frames pass through the proxy
                                long req = proxy.toServer.get() / reps, resp = proxy.toClient.get() / reps;
                                w.printf("%s,%s,%d,%s,%d,%d,%d,%d%n", proto, opName, size, gzip, req, resp, req + resp,
                                        req + resp - 2L * size);
                                System.out.printf("%-4s %-4s %8d gzip=%-5s request=%d response=%d total=%d%n", proto, opName,
                                        size, gzip, req, resp, req + resp);
                            }
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ throughput (8 parallel callers)
    static void throughput() throws Exception {
        int threads = 8;
        long seconds = 6;
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("throughput.csv")))) {
            w.println("protocol,operation,payload_bytes,threads,requests_per_s,p50_ms,p99_ms,errors");
            for (int size : new int[]{1_000, 64_000}) {
                for (String opName : new String[]{"GET", "POST"}) {
                    if (opName.equals("POST") && size > 1_000) continue;
                    for (int round = 0; round < 2; round++) {
                        for (String proto : round == 0 ? List.of("grpc", "rest") : List.of("rest", "grpc")) {
                            Result r = load(proto, opName, size, threads, seconds);
                            if (round == 0) continue; // first pass is warm-up of the server JIT, second is reported
                            Stats s = new Stats(r.latenciesMs);
                            w.printf(Locale.ROOT, "%s,%s,%d,%d,%.0f,%.3f,%.3f,%d%n", proto, opName, size, threads,
                                    r.latenciesMs.length / (double) seconds, s.pct(50), s.pct(99), r.errors);
                            System.out.printf(Locale.ROOT, "%-4s %-4s %6d  %.0f req/s p50=%.2f p99=%.2f errors=%d%n", proto,
                                    opName, size, r.latenciesMs.length / (double) seconds, s.pct(50), s.pct(99), r.errors);
                        }
                    }
                }
            }
        }
    }

    record Result(double[] latenciesMs, int errors) {}

    static Result load(String proto, String opName, int size, int threads, long seconds) throws Exception {
        int port = proto.equals("grpc") ? grpcPort : restPort;
        String text = PayloadGenerator.text(size, size);
        try (PastebinClient c = client(proto, port, false)) {
            String key = c.create(text, 3600, UUID.randomUUID().toString()).key();
            Op op = opName.equals("GET") ? () -> c.get(key) : () -> c.create(text, 3600, UUID.randomUUID().toString());
            for (int i = 0; i < 300; i++) op.run();
            long end = System.nanoTime() + seconds * 1_000_000_000L;
            List<Future<List<Double>>> fs = new ArrayList<>();
            AtomicInteger errors = new AtomicInteger();
            ExecutorService ex = Executors.newFixedThreadPool(threads);
            for (int t = 0; t < threads; t++) {
                fs.add(ex.submit(() -> {
                    List<Double> l = new ArrayList<>();
                    while (System.nanoTime() < end) {
                        long t0 = System.nanoTime();
                        try {
                            op.run();
                            l.add((System.nanoTime() - t0) / 1e6);
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                    }
                    return l;
                }));
            }
            List<Double> all = new ArrayList<>();
            for (var f : fs) all.addAll(f.get());
            ex.shutdown();
            return new Result(all.stream().mapToDouble(Double::doubleValue).toArray(), errors.get());
        }
    }

    /** Read-heavy mixed workload: 80% 1 KB GET, 15% 64 KB GET, 5% 1 KB POST. */
    static void mixed() throws Exception {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("mixed.csv")))) {
            w.println("protocol,workload,threads,requests_per_s,p50_ms,p99_ms,errors");
            for (String proto : List.of("grpc", "rest")) {
                int port = proto.equals("grpc") ? grpcPort : restPort;
                try (PastebinClient setup = client(proto, port, false)) {
                    String small = PayloadGenerator.text(1_000, 1);
                    String large = PayloadGenerator.text(64_000, 2);
                    String smallKey = setup.create(small, 3600, UUID.randomUUID().toString()).key();
                    String largeKey = setup.create(large, 3600, UUID.randomUUID().toString()).key();
                    for (int i = 0; i < 300; i++) setup.get(smallKey);
                    long end = System.nanoTime() + 6_000_000_000L;
                    ExecutorService ex = Executors.newFixedThreadPool(8);
                    AtomicInteger errors = new AtomicInteger();
                    List<Future<List<Double>>> fs = new ArrayList<>();
                    List<PastebinClient> clients = new ArrayList<>();
                    for (int t = 0; t < 8; t++) {
                        PastebinClient workerClient = client(proto, port, false); clients.add(workerClient);
                        int workerSeed = t;
                        fs.add(ex.submit(() -> {
                        List<Double> times = new ArrayList<>();
                        java.util.Random random = new java.util.Random(workerSeed);
                        while (System.nanoTime() < end) {
                            int choice = random.nextInt(100); long start = System.nanoTime();
                            try { if (choice < 80) workerClient.get(smallKey); else if (choice < 95) workerClient.get(largeKey); else workerClient.create(small, 3600, UUID.randomUUID().toString());
                                times.add((System.nanoTime() - start) / 1e6); } catch (Exception e) { errors.incrementAndGet(); }
                        }
                        return times;
                        }));
                    }
                    List<Double> all = new ArrayList<>(); for (Future<List<Double>> f : fs) all.addAll(f.get()); ex.shutdown();
                    clients.forEach(PastebinClient::close);
                    Stats s = new Stats(all.stream().mapToDouble(Double::doubleValue).toArray());
                    w.printf(Locale.ROOT, "%s,80%% GET 1KB + 15%% GET 64KB + 5%% POST 1KB,8,%.0f,%.3f,%.3f,%d%n",
                            proto, all.size() / 6.0, s.pct(50), s.pct(99), errors.get());
                    System.out.printf(Locale.ROOT, "%s mixed %.0f req/s p50=%.3f p99=%.3f errors=%d%n", proto, all.size() / 6.0, s.pct(50), s.pct(99), errors.get());
                }
            }
        }
    }

    // ------------------------------------------------------------------ reliability with injected faults
    static void reliability() throws Exception {
        int logical = Integer.getInteger("logical", 2000);
        double before = 0.15, lost = 0.15; // 15% of calls fail before processing, 15% lose the response after it was saved
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("reliability.csv")))) {
            w.println("protocol,client_mode,logical_creates,succeeded,failed,snippets_stored,duplicates,attempts,retries,mean_ms,p99_ms");
            for (String proto : new String[]{"grpc", "rest"}) {
                for (String mode : new String[]{"no_retry", "retry_with_key", "retry_without_key"}) {
                    PasteService service = new PasteService();
                    FaultInjector inj = new FaultInjector(before, lost, 0, 0);
                    GrpcPastebinServer g = new GrpcPastebinServer(0, service, inj, "https://paste.example/", 16).start();
                    NettyRestPastebinServer r = new NettyRestPastebinServer(0, service, inj, "https://paste.example/", 16).start();
                    ClientOptions o = new ClientOptions();
                    o.backoffBaseMs = 5;
                    o.backoffCapMs = 40;
                    o.retriesEnabled = !mode.equals("no_retry");
                    o.retryCreateWithoutKey = mode.equals("retry_without_key");
                    boolean useKey = !mode.equals("retry_without_key") && !mode.equals("no_retry") || mode.equals("no_retry");
                    List<Double> ms = new ArrayList<>();
                    int ok = 0, failed = 0;
                    PastebinClient c = proto.equals("grpc") ? new GrpcPastebinClient("localhost", g.port(), o)
                            : new RestPastebinClient("localhost", r.port(), o);
                    for (int i = 0; i < logical; i++) {
                        String text = "snippet number " + i;
                        long t0 = System.nanoTime();
                        try {
                            // no_retry still sends a key (a careful client), it just never repeats the call
                            c.create(text, 3600, mode.equals("retry_without_key") ? null : UUID.randomUUID().toString());
                            ok++;
                        } catch (RpcFailure f) {
                            failed++;
                        }
                        ms.add((System.nanoTime() - t0) / 1e6);
                    }
                    Stats s = new Stats(ms.stream().mapToDouble(Double::doubleValue).toArray());
                    int stored = service.size();
                    w.printf(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%.3f,%.3f%n", proto, mode, logical, ok, failed, stored,
                            stored - logical > 0 ? stored - logical : 0, c.executor().attempts(), c.executor().retries(),
                            s.mean, s.pct(99));
                    System.out.printf(Locale.ROOT, "%-4s %-18s ok=%d failed=%d stored=%d attempts=%d retries=%d mean=%.2f p99=%.2f%n",
                            proto, mode, ok, failed, stored, c.executor().attempts(), c.executor().retries(), s.mean, s.pct(99));
                    c.close();
                    g.stop();
                    r.stop();
                }
            }
        }
    }
}
