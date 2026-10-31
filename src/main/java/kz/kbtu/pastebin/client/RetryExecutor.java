package kz.kbtu.pastebin.client;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Timeouts + retries, written once and used by both clients.
 * Rules (SIS #1, D15):
 * - every attempt has its own deadline, and the whole call has a total budget that no retry may exceed
 * - only temporary failures are retried, and only for calls that are safe to repeat
 * - at most maxRetries retries; the wait grows exponentially and is random ("full jitter"):
 *   sleep = random(0, min(cap, base * 2^retryNumber))
 */
public final class RetryExecutor {
    @FunctionalInterface
    public interface Attempt<T> {
        T run(long timeoutMs) throws RpcFailure;
    }

    private final ClientOptions options;
    private final AtomicLong attempts = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();

    public RetryExecutor(ClientOptions options) {
        this.options = options;
    }

    public <T> T execute(boolean safeToRepeat, Attempt<T> attempt) throws RpcFailure {
        long deadlineNs = System.nanoTime() + options.totalBudgetMs * 1_000_000L;
        int retryNumber = 0;
        RpcFailure last = null;
        while (true) {
            long remainingMs = (deadlineNs - System.nanoTime()) / 1_000_000L;
            if (remainingMs <= 0) {
                throw last != null ? last : new RpcFailure(RpcFailure.Kind.RETRYABLE, "DEADLINE_EXCEEDED",
                        "total budget used up", null);
            }
            long timeout = Math.min(options.perAttemptTimeoutMs, remainingMs);
            attempts.incrementAndGet();
            try {
                return attempt.run(timeout);
            } catch (RpcFailure f) {
                last = f;
                if (!options.retriesEnabled || !safeToRepeat || !f.retryable() || retryNumber >= options.maxRetries) {
                    throw f;
                }
                long ceiling = Math.min(options.backoffCapMs, options.backoffBaseMs << retryNumber);
                long sleepMs = ThreadLocalRandom.current().nextLong(ceiling + 1);
                long leftAfterSleep = (deadlineNs - System.nanoTime()) / 1_000_000L - sleepMs;
                if (leftAfterSleep <= 0) {
                    throw f; // no time left for another attempt
                }
                sleep(sleepMs);
                retryNumber++;
                retries.incrementAndGet();
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long attempts() {
        return attempts.get();
    }

    public long retries() {
        return retries.get();
    }

    public ClientOptions options() {
        return options;
    }
}
