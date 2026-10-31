package kz.kbtu.pastebin.core;

/** Protocol-independent error codes. Each adapter (gRPC, REST) maps them to its own status codes. */
public enum ErrorCode {
    INVALID_ARGUMENT,        // REST 400, gRPC INVALID_ARGUMENT
    TOO_LARGE,               // REST 413, gRPC OUT_OF_RANGE
    NOT_FOUND,               // REST 404, gRPC NOT_FOUND (also used for expired snippets)
    IDEMPOTENCY_KEY_REUSED,  // REST 422, gRPC FAILED_PRECONDITION
    UNAVAILABLE              // REST 503, gRPC UNAVAILABLE (temporary, safe to retry)
}
