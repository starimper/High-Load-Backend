package kz.kbtu.pastebin.client;

/** A failed call, already translated to the same few kinds for both protocols. */
public final class RpcFailure extends Exception {
    public enum Kind {
        /** temporary problem (UNAVAILABLE / 503 / timeout / connection error): may be retried */
        RETRYABLE,
        NOT_FOUND,
        /** bad request, too large, idempotency key reused: retrying would give the same answer */
        REJECTED
    }

    private final Kind kind;
    private final String status; // "UNAVAILABLE", "HTTP 503", ...

    public RpcFailure(Kind kind, String status, String message, Throwable cause) {
        super(status + ": " + message, cause);
        this.kind = kind;
        this.status = status;
    }

    public Kind kind() {
        return kind;
    }

    public String status() {
        return status;
    }

    public boolean retryable() {
        return kind == Kind.RETRYABLE;
    }
}
