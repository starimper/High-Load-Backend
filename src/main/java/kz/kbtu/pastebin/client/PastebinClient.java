package kz.kbtu.pastebin.client;

/** The service interface as the caller sees it. Two implementations: gRPC and REST/JSON. */
public interface PastebinClient extends AutoCloseable {
    /**
     * @param idempotencyKey UUID made by the caller; the same key on a repeated call returns the first result.
     *                       Without a key the call is NOT retried (a retry could create a duplicate).
     */
    CreateResult create(String content, long ttlSeconds, String idempotencyKey) throws RpcFailure;

    GetResult get(String key) throws RpcFailure;

    RetryExecutor executor();

    @Override
    void close();
}
