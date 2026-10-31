package kz.kbtu.pastebin.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Business logic of Pastebin, independent of the protocol. gRPC and REST are only thin adapters on top of it,
 * so the benchmark compares the protocols and not two different implementations.
 *
 * Storage is in memory on purpose: PostgreSQL and S3 from SIS #1 would add the same cost to both protocols.
 * The rules are the same as in SIS #1:
 * - the key is 8 random base62 characters; a collision is detected by "put if absent" (like ON CONFLICT DO NOTHING)
 * - an expired snippet looks like a missing one (NOT_FOUND)
 * - the Idempotency-Key is kept 24 hours; the same key with the same request returns the first result,
 *   the same key with a different request is rejected
 */
public class PasteService {
    public static final int MAX_BYTES = 1_048_576;
    public static final long MIN_TTL_SECONDS = 600;
    public static final long MAX_TTL_SECONDS = 30L * 24 * 3600;
    public static final Duration IDEMPOTENCY_RETENTION = Duration.ofHours(24);

    public record Paste(String key, String content, Instant createdAt, Instant expiresAt) {}

    public record Created(String key, Instant expiresAt, boolean replayed) {}

    private record IdemEntry(String fingerprint, Instant createdAt, CompletableFuture<Created> result) {}

    private final Clock clock;
    private final KeyGenerator keys;
    private final ConcurrentHashMap<String, Paste> pastes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IdemEntry> idempotency = new ConcurrentHashMap<>();

    public PasteService() {
        this(Clock.systemUTC(), KeyGenerator.RANDOM_BASE62);
    }

    public PasteService(Clock clock, KeyGenerator keys) {
        this.clock = clock;
        this.keys = keys;
    }

    /** idempotencyKey may be null: then every call creates a new snippet. */
    public Created create(String content, long ttlSeconds, String idempotencyKey) {
        if (content == null || content.isEmpty()) {
            throw new ServiceException(ErrorCode.INVALID_ARGUMENT, "content is empty");
        }
        if (ttlSeconds < MIN_TTL_SECONDS || ttlSeconds > MAX_TTL_SECONDS) {
            throw new ServiceException(ErrorCode.INVALID_ARGUMENT, "ttl_seconds must be between 600 and 2592000");
        }
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_BYTES) {
            throw new ServiceException(ErrorCode.TOO_LARGE, "content is larger than 1 MB");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return insert(content, ttlSeconds);
        }

        String fingerprint = fingerprint(bytes, ttlSeconds);
        while (true) {
            IdemEntry mine = new IdemEntry(fingerprint, clock.instant(), new CompletableFuture<>());
            IdemEntry previous = idempotency.putIfAbsent(idempotencyKey, mine);
            if (previous == null) {
                // we are the first request with this key: do the work, then publish the result
                try {
                    Created created = insert(content, ttlSeconds);
                    mine.result().complete(created);
                    return created;
                } catch (RuntimeException e) {
                    idempotency.remove(idempotencyKey, mine); // a failed attempt must not block a retry
                    mine.result().completeExceptionally(e);
                    throw e;
                }
            }
            if (previous.createdAt().plus(IDEMPOTENCY_RETENTION).isBefore(clock.instant())) {
                idempotency.remove(idempotencyKey, previous); // older than 24 h: forget it and try again
                continue;
            }
            if (!previous.fingerprint().equals(fingerprint)) {
                throw new ServiceException(ErrorCode.IDEMPOTENCY_KEY_REUSED,
                        "this Idempotency-Key was already used with a different request");
            }
            try {
                Created first = previous.result().join(); // waits if the first request is still running
                return new Created(first.key(), first.expiresAt(), true);
            } catch (CompletionException e) {
                if (e.getCause() instanceof ServiceException se) {
                    throw se;
                }
                throw e;
            }
        }
    }

    public Paste get(String key) {
        Paste p = key == null ? null : pastes.get(key);
        if (p == null || !p.expiresAt().isAfter(clock.instant())) {
            throw new ServiceException(ErrorCode.NOT_FOUND, "snippet not found");
        }
        return p;
    }

    /** What the hourly Cleanup Worker does. Returns the number of removed snippets. */
    public int purgeExpired() {
        Instant now = clock.instant();
        int removed = 0;
        for (var e : pastes.entrySet()) {
            if (!e.getValue().expiresAt().isAfter(now) && pastes.remove(e.getKey(), e.getValue())) {
                removed++;
            }
        }
        idempotency.entrySet().removeIf(e -> e.getValue().createdAt().plus(IDEMPOTENCY_RETENTION).isBefore(now));
        return removed;
    }

    public int size() {
        return pastes.size();
    }

    /** content -> how many snippets store exactly this content (used to count duplicates in experiments). */
    public Map<String, Integer> contentCounts() {
        Map<String, Integer> counts = new HashMap<>();
        pastes.values().forEach(p -> counts.merge(p.content(), 1, Integer::sum));
        return counts;
    }

    public void clear() {
        pastes.clear();
        idempotency.clear();
    }

    private Created insert(String content, long ttlSeconds) {
        Instant now = clock.instant();
        Instant expires = now.plusSeconds(ttlSeconds);
        for (int attempt = 0; attempt < 10; attempt++) {
            String key = keys.next();
            Paste p = new Paste(key, content, now, expires);
            if (pastes.putIfAbsent(key, p) == null) { // equivalent of INSERT ... ON CONFLICT (key) DO NOTHING
                return new Created(key, expires, false);
            }
        }
        throw new ServiceException(ErrorCode.UNAVAILABLE, "could not find a free key");
    }

    private static String fingerprint(byte[] contentBytes, long ttlSeconds) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(Long.toString(ttlSeconds).getBytes(StandardCharsets.UTF_8));
            md.update((byte) ':');
            md.update(contentBytes);
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
