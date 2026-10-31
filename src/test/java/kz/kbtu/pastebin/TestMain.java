package kz.kbtu.pastebin;

import kz.kbtu.pastebin.client.*;
import kz.kbtu.pastebin.core.*;
import kz.kbtu.pastebin.grpc.GrpcPastebinClient;
import kz.kbtu.pastebin.grpc.GrpcPastebinServer;
import kz.kbtu.pastebin.grpc.Proto;
import kz.kbtu.pastebin.rest.RestPastebinClient;
import kz.kbtu.pastebin.rest.RestPastebinServer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Small test runner without JUnit (the sandbox could not download it). Run: java -cp ... kz.kbtu.pastebin.TestMain */
public final class TestMain {
    static int passed, failed;
    static int scenarioCursor;
    static int selectedScenario = -1;

    interface T { void run() throws Exception; }

    static void test(String name, T t) {
        int index = scenarioCursor++;
        if (selectedScenario >= 0 && selectedScenario != index) return;
        try {
            t.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (Throwable e) {
            failed++;
            System.out.println("FAIL  " + name + "  -> " + e);
            e.printStackTrace(System.out);
        }
    }

    static void check(boolean c, String msg) {
        if (!c) throw new AssertionError(msg);
    }

    static final String UUID1 = "11111111-2222-3333-4444-555555555555";

    /** Fault injector controlled by the test: fail the first N calls before / after processing. */
    static final class Scripted extends FaultInjector {
        final AtomicInteger beforeFails, afterFails;
        final long sleepMs;

        Scripted(int before, int after, long sleepMs) {
            super(0, 0, 0, 0);
            beforeFails = new AtomicInteger(before);
            afterFails = new AtomicInteger(after);
            this.sleepMs = sleepMs;
        }

        @Override
        public void before() {
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (beforeFails.getAndUpdate(x -> x > 0 ? x - 1 : 0) > 0)
                throw new ServiceException(ErrorCode.UNAVAILABLE, "scripted failure before");
        }

        @Override
        public void after() {
            if (afterFails.getAndUpdate(x -> x > 0 ? x - 1 : 0) > 0)
                throw new ServiceException(ErrorCode.UNAVAILABLE, "scripted failure after (response lost)");
        }
    }

    static final class Fixture implements AutoCloseable {
        final PasteService service = new PasteService();
        final GrpcPastebinServer grpc;
        final RestPastebinServer rest;

        Fixture(FaultInjector f) throws Exception {
            grpc = new GrpcPastebinServer(0, service, f, "https://paste.example/", 8).start();
            rest = new RestPastebinServer(0, service, f, "https://paste.example/", 8).start();
        }

        PastebinClient client(String proto, ClientOptions o) {
            return proto.equals("grpc") ? new GrpcPastebinClient("localhost", grpc.port(), o)
                    : new RestPastebinClient("localhost", rest.port(), o);
        }

        @Override
        public void close() {
            grpc.stop();
            rest.stop();
        }
    }

    static ClientOptions fast() {
        ClientOptions o = new ClientOptions();
        o.backoffBaseMs = 5;
        o.backoffCapMs = 20;
        return o;
    }

    public static void main(String[] args) throws Exception {
        System.exit(runAll(args));
    }

    /** Runs the complete legacy scenario matrix without terminating the hosting JVM. */
    public static int runAll(String[] args) throws Exception {
        passed = failed = 0;
        scenarioCursor = 0;
        // ---------- PasteService (shared logic) ----------
        test("service: same idempotency key returns the first result, one record", () -> {
            var s = new PasteService();
            var a = s.create("hello", 3600, UUID1);
            var b = s.create("hello", 3600, UUID1);
            check(a.key().equals(b.key()), "keys differ");
            check(!a.replayed() && b.replayed(), "replayed flags");
            check(s.size() == 1, "size " + s.size());
        });
        test("service: same key with different body is rejected", () -> {
            var s = new PasteService();
            s.create("hello", 3600, UUID1);
            try {
                s.create("other", 3600, UUID1);
                check(false, "no exception");
            } catch (ServiceException e) {
                check(e.code() == ErrorCode.IDEMPOTENCY_KEY_REUSED, e.code().toString());
            }
        });
        test("service: 50 parallel requests with one key create exactly one snippet", () -> {
            var s = new PasteService();
            ExecutorService ex = Executors.newFixedThreadPool(16);
            List<Future<String>> fs = new ArrayList<>();
            for (int i = 0; i < 50; i++) fs.add(ex.submit(() -> s.create("same", 3600, UUID1).key()));
            Set<String> keys = new HashSet<>();
            for (var f : fs) keys.add(f.get());
            ex.shutdown();
            check(keys.size() == 1 && s.size() == 1, "keys=" + keys + " size=" + s.size());
        });
        test("service: without key every call creates a new snippet", () -> {
            var s = new PasteService();
            s.create("x", 3600, null);
            s.create("x", 3600, null);
            check(s.size() == 2, "size " + s.size());
        });
        test("service: expiry and idempotency retention follow the clock", () -> {
            var now = new Instant[]{Instant.parse("2026-01-01T00:00:00Z")};
            Clock clock = new Clock() {
                public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
                public Clock withZone(java.time.ZoneId z) { return this; }
                public Instant instant() { return now[0]; }
            };
            var s = new PasteService(clock, KeyGenerator.RANDOM_BASE62);
            var a = s.create("x", 600, UUID1);
            s.get(a.key());
            now[0] = now[0].plusSeconds(601);
            try { s.get(a.key()); check(false, "still readable"); }
            catch (ServiceException e) { check(e.code() == ErrorCode.NOT_FOUND, "code"); }
            now[0] = now[0].plus(Duration.ofHours(25));
            var b = s.create("x", 600, UUID1); // key forgotten after 24 h -> new snippet
            check(!b.replayed(), "replayed after retention");
        });
        test("service: key collision is retried with a new key", () -> {
            var seq = new ArrayDeque<>(List.of("AAAAAAAA", "AAAAAAAA", "BBBBBBBB"));
            var s = new PasteService(Clock.systemUTC(), seq::poll);
            var a = s.create("one", 3600, null);
            var b = s.create("two", 3600, null);
            check(a.key().equals("AAAAAAAA") && b.key().equals("BBBBBBBB"), a.key() + " " + b.key());
        });
        test("service: validation (empty, ttl, 1 MB limit)", () -> {
            var s = new PasteService();
            for (Supplier<?> bad : List.<Supplier<?>>of(
                    () -> s.create("", 3600, null),
                    () -> s.create("x", 10, null),
                    () -> s.create("x".repeat(PasteService.MAX_BYTES + 1), 3600, null))) {
                try { bad.get(); check(false, "accepted"); } catch (ServiceException e) { /* expected */ }
            }
            s.create("x".repeat(PasteService.MAX_BYTES), 3600, null); // exactly 1 MB is fine
        });

        // ---------- Protobuf encoding ----------
        test("protobuf: bytes equal protoc output; round trip", () -> {
            check(HexFormat.of().formatHex(Proto.encode(new Proto.CreateReq("héllo", 3600))).equals("0a0668c3a96c6c6f10901c"), "CreateReq");
            check(HexFormat.of().formatHex(Proto.encode(new Proto.GetResp("abc", "x", "t1", "t2"))).equals("0a036162631201781a02743122027432"), "GetResp");
            var r = Proto.decodeCreateReq(Proto.encode(new Proto.CreateReq("мир", 600)));
            check(r.content().equals("мир") && r.ttlSeconds() == 600, "round trip");
        });

        // ---------- Both protocols, same behaviour ----------
        for (String proto : List.of("grpc", "rest")) {
            test(proto + ": create then get returns the same text (unicode)", () -> {
                try (var f = new Fixture(FaultInjector.NONE); var c = f.client(proto, fast())) {
                    var r = c.create("héllo мир\nline2", 3600, UUID.randomUUID().toString());
                    var g = c.get(r.key());
                    check(g.content().equals("héllo мир\nline2"), g.content());
                    check(r.url().endsWith(r.key()), r.url());
                }
            });
            test(proto + ": 1 MB snippet works, also with gzip", () -> {
                try (var f = new Fixture(FaultInjector.NONE)) {
                    for (boolean gz : new boolean[]{false, true}) {
                        var o = fast();
                        o.gzip = gz;
                        o.perAttemptTimeoutMs = 5000;
                        o.totalBudgetMs = 10000;
                        try (var c = f.client(proto, o)) {
                            String text = "abcdefghij".repeat(100_000);
                            var r = c.create(text, 3600, UUID.randomUUID().toString());
                            check(c.get(r.key()).content().equals(text), "content differs gzip=" + gz);
                        }
                    }
                }
            });
            test(proto + ": error mapping (404, bad request, key reuse, too large)", () -> {
                try (var f = new Fixture(FaultInjector.NONE); var c = f.client(proto, fast())) {
                    expect(RpcFailure.Kind.NOT_FOUND, () -> c.get("missing1"));
                    expect(RpcFailure.Kind.REJECTED, () -> c.create("", 3600, null));
                    c.create("a", 3600, UUID1);
                    expect(RpcFailure.Kind.REJECTED, () -> c.create("b", 3600, UUID1));
                    var o = fast();
                    o.perAttemptTimeoutMs = 5000; o.totalBudgetMs = 10000;
                    try (var c2 = f.client(proto, o)) {
                        expect(RpcFailure.Kind.REJECTED, () -> c2.create("x".repeat(PasteService.MAX_BYTES + 1), 3600, null));
                    }
                }
            });
            test(proto + ": idempotency over the wire - same key twice, one snippet", () -> {
                try (var f = new Fixture(FaultInjector.NONE); var c = f.client(proto, fast())) {
                    var a = c.create("same", 3600, UUID1);
                    var b = c.create("same", 3600, UUID1);
                    check(a.key().equals(b.key()) && f.service.size() == 1, "dup created");
                }
            });
            test(proto + ": response lost + retry with key -> exactly one snippet", () -> {
                var inj = new Scripted(0, 2, 0); // first 2 responses are lost after the snippet was stored
                try (var f = new Fixture(inj); var c = f.client(proto, fast())) {
                    var r = c.create("important", 3600, UUID.randomUUID().toString());
                    check(f.service.size() == 1, "snippets: " + f.service.size());
                    check(c.executor().retries() == 2, "retries: " + c.executor().retries());
                    check(c.get(r.key()).content().equals("important"), "content");
                }
            });
            test(proto + ": response lost, NO key -> client does not retry (no duplicate risk)", () -> {
                var inj = new Scripted(0, 1, 0);
                try (var f = new Fixture(inj); var c = f.client(proto, fast())) {
                    expect(RpcFailure.Kind.RETRYABLE, () -> c.create("x", 3600, null));
                    check(c.executor().retries() == 0, "retried");
                    check(f.service.size() == 1, "size " + f.service.size());
                }
            });
            test(proto + ": retry without key (demo switch) creates duplicates", () -> {
                var inj = new Scripted(0, 2, 0);
                var o = fast();
                o.retryCreateWithoutKey = true;
                try (var f = new Fixture(inj); var c = f.client(proto, o)) {
                    c.create("x", 3600, null);
                    check(f.service.size() == 3, "expected 3 duplicates, got " + f.service.size());
                }
            });
            test(proto + ": get is retried on temporary errors", () -> {
                var inj = new Scripted(2, 0, 0);
                try (var f = new Fixture(inj); var c = f.client(proto, fast())) {
                    var r = f.service.create("z", 3600, null);
                    check(c.get(r.key()).content().equals("z"), "content");
                    check(c.executor().retries() == 2, "retries " + c.executor().retries());
                }
            });
            test(proto + ": non-retryable errors are not retried", () -> {
                try (var f = new Fixture(FaultInjector.NONE); var c = f.client(proto, fast())) {
                    expect(RpcFailure.Kind.NOT_FOUND, () -> c.get("nothere1"));
                    check(c.executor().retries() == 0 && c.executor().attempts() == 1, "attempts " + c.executor().attempts());
                }
            });
            test(proto + ": at most 3 retries, then the error is returned", () -> {
                var inj = new Scripted(100, 0, 0);
                try (var f = new Fixture(inj); var c = f.client(proto, fast())) {
                    expect(RpcFailure.Kind.RETRYABLE, () -> c.get("whatever"));
                    check(c.executor().attempts() == 4 && c.executor().retries() == 3, "attempts " + c.executor().attempts());
                }
            });
            test(proto + ": slow server -> per-attempt timeout, total budget respected", () -> {
                var inj = new Scripted(0, 0, 400); // every call takes 400 ms
                var o = fast();
                o.perAttemptTimeoutMs = 100;
                o.totalBudgetMs = 600;
                try (var f = new Fixture(inj); var c = f.client(proto, o)) {
                    long t0 = System.nanoTime();
                    try { c.get("x"); check(false, "no failure"); }
                    catch (RpcFailure e) { check(e.retryable(), e.status()); }
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    check(ms < 900, "took " + ms + " ms, budget was 600");
                    check(c.executor().attempts() >= 2, "attempts " + c.executor().attempts());
                }
            });
            test(proto + ": server down -> UNAVAILABLE, retried within budget", () -> {
                var f = new Fixture(FaultInjector.NONE);
                var c = f.client(proto, fast());
                f.close();
                long t0 = System.nanoTime();
                expect(RpcFailure.Kind.RETRYABLE, () -> c.get("abc"));
                check((System.nanoTime() - t0) / 1_000_000 < 2500, "too slow");
                c.close();
            });
        }

        test("retry: back-off is random and never above min(cap, base*2^n)", () -> {
            var o = new ClientOptions();
            o.backoffBaseMs = 20; o.backoffCapMs = 100; o.maxRetries = 3; o.totalBudgetMs = 10_000;
            var ex = new RetryExecutor(o);
            long t0 = System.nanoTime();
            try { ex.execute(true, t -> { throw new RpcFailure(RpcFailure.Kind.RETRYABLE, "UNAVAILABLE", "x", null); }); }
            catch (RpcFailure e) { /* expected */ }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            check(ms <= 20 + 40 + 80 + 60, "waited " + ms + " ms"); // ceilings 20+40+80, plus slack
            check(ex.retries() == 3, "retries");
        });

        System.out.println("\n" + passed + " passed, " + failed + " failed");
        return failed == 0 ? 0 : 1;
    }

    /** Executes exactly one scenario for JUnit's individual DynamicTest cases. */
    public static int runScenario(int index) throws Exception {
        selectedScenario = index;
        try { return runAll(new String[0]); }
        finally { selectedScenario = -1; }
    }

    static void expect(RpcFailure.Kind kind, ThrowingRun r) {
        try {
            r.run();
            throw new AssertionError("expected failure " + kind);
        } catch (RpcFailure f) {
            if (f.kind() != kind) throw new AssertionError("expected " + kind + " but was " + f.kind() + " (" + f.status() + ": " + f.getMessage() + ")");
        }
    }

    interface ThrowingRun { void run() throws RpcFailure; }
}
