package kz.kbtu.pastebin.client;

/** Timeouts and retry settings shared by the REST and gRPC clients. */
public final class ClientOptions {
    /** deadline of ONE attempt */
    public long perAttemptTimeoutMs = 200;
    /** deadline of the WHOLE call including all retries and waiting between them */
    public long totalBudgetMs = 2000;
    /** at most 3 retries (4 attempts in total) */
    public int maxRetries = 3;
    public long backoffBaseMs = 50;
    public long backoffCapMs = 500;
    public boolean retriesEnabled = true;
    /** demo switch: retry create even without an Idempotency-Key (shows why this is dangerous) */
    public boolean retryCreateWithoutKey = false;
    /** compress request bodies / ask for compressed responses (gzip) */
    public boolean gzip = false;
    /** Use TLS for the transport. Benchmark mode trusts the supplied self-signed certificate. */
    public boolean tls = false;

    public ClientOptions copy() {
        ClientOptions o = new ClientOptions();
        o.perAttemptTimeoutMs = perAttemptTimeoutMs;
        o.totalBudgetMs = totalBudgetMs;
        o.maxRetries = maxRetries;
        o.backoffBaseMs = backoffBaseMs;
        o.backoffCapMs = backoffCapMs;
        o.retriesEnabled = retriesEnabled;
        o.retryCreateWithoutKey = retryCreateWithoutKey;
        o.gzip = gzip;
        o.tls = tls;
        return o;
    }
}
