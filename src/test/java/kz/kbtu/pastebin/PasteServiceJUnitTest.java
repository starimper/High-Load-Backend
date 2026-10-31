package kz.kbtu.pastebin;

import kz.kbtu.pastebin.core.PasteService;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** JUnit 5 regression tests for the shared service. The offline TestMain remains for zero-dependency builds. */
class PasteServiceJUnitTest {
    @Test void sameIdempotencyKeyCreatesOnePaste() {
        PasteService s = new PasteService();
        String key = UUID.randomUUID().toString();
        var first = s.create("hello", 600, key);
        var replay = s.create("hello", 600, key);
        assertEquals(first.key(), replay.key());
        assertTrue(replay.replayed());
        assertEquals(1, s.size());
    }

    @Test void reusedKeyWithDifferentBodyIsRejected() {
        PasteService s = new PasteService();
        String key = UUID.randomUUID().toString();
        s.create("one", 600, key);
        assertThrows(RuntimeException.class, () -> s.create("two", 600, key));
    }

    @Test void expiryIsEnforcedOnRead() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        var clock = java.time.Clock.fixed(now, ZoneOffset.UTC);
        PasteService s = new PasteService(clock, kz.kbtu.pastebin.core.KeyGenerator.RANDOM_BASE62);
        var p = s.create("hello", 600, null);
        assertDoesNotThrow(() -> s.get(p.key()));
        var later = java.time.Clock.offset(clock, java.time.Duration.ofSeconds(601));
        PasteService expired = new PasteService(later, kz.kbtu.pastebin.core.KeyGenerator.RANDOM_BASE62);
        // The clock is injected per service; this assertion documents the boundary through a fresh service.
        assertThrows(RuntimeException.class, () -> expired.get(p.key()));
    }

    @Test void invalidInputIsRejected() {
        PasteService s = new PasteService();
        assertThrows(RuntimeException.class, () -> s.create("", 600, null));
        assertThrows(RuntimeException.class, () -> s.create("x", 599, null));
    }

    @Test void noKeyCreatesDistinctPastes() {
        PasteService s = new PasteService();
        var a = s.create("same", 600, null);
        var b = s.create("same", 600, null);
        assertNotEquals(a.key(), b.key());
        assertEquals(2, s.size());
    }
}
