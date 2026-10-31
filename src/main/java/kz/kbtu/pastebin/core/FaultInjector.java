package kz.kbtu.pastebin.core;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Test tool: makes the server misbehave so that timeouts and retries can be demonstrated.
 * <ul>
 *   <li>errorRate     - fail BEFORE doing any work (temporary error, nothing was stored)</li>
 *   <li>lostRate      - do the work, then fail (the response is "lost": the data IS stored)</li>
 *   <li>slowRate/slowMs - add a delay before doing the work (to trigger client deadlines)</li>
 * </ul>
 */
public class FaultInjector {
    public static final FaultInjector NONE = new FaultInjector(0, 0, 0, 0);

    private final double errorRate;
    private final double lostRate;
    private final double slowRate;
    private final long slowMs;

    public FaultInjector(double errorRate, double lostRate, double slowRate, long slowMs) {
        this.errorRate = errorRate;
        this.lostRate = lostRate;
        this.slowRate = slowRate;
        this.slowMs = slowMs;
    }

    public void before() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        if (slowRate > 0 && r.nextDouble() < slowRate) {
            try {
                Thread.sleep(slowMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (errorRate > 0 && r.nextDouble() < errorRate) {
            throw new ServiceException(ErrorCode.UNAVAILABLE, "injected failure before processing");
        }
    }

    public void after() {
        if (lostRate > 0 && ThreadLocalRandom.current().nextDouble() < lostRate) {
            throw new ServiceException(ErrorCode.UNAVAILABLE, "injected failure after processing (response lost)");
        }
    }
}
