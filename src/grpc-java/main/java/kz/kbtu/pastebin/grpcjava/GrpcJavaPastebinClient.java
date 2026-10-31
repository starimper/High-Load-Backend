package kz.kbtu.pastebin.grpcjava;

import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import kz.kbtu.pastebin.client.*;
import kz.kbtu.pastebin.grpc.pb.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Standard grpc-java client using generated blocking stubs and the shared retry policy. */
public final class GrpcJavaPastebinClient implements PastebinClient {
    private static final Metadata.Key<String> IDEM = Metadata.Key.of("idempotency-key", Metadata.ASCII_STRING_MARSHALLER);
    private final ManagedChannel channel; private final ClientOptions options; private final RetryExecutor executor;
    public GrpcJavaPastebinClient(String host, int port, ClientOptions options) {
        this.options = options; this.executor = new RetryExecutor(options);
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
    @Override public CreateResult create(String content, long ttlSeconds, String key) throws RpcFailure {
        boolean safe = key != null || options.retryCreateWithoutKey;
        return executor.execute(safe, timeout -> {
            try {
                var stub = PastebinGrpc.newBlockingStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);
                if (key != null) { Metadata m = new Metadata(); m.put(IDEM, key);
                    stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(m)); }
                var r = stub.createPaste(CreatePasteRequest.newBuilder().setContent(content).setTtlSeconds(ttlSeconds).build());
                return new CreateResult(r.getKey(), r.getUrl(), r.getExpiresAt());
            } catch (StatusRuntimeException e) { throw translate(e); }
        });
    }
    @Override public GetResult get(String key) throws RpcFailure { return executor.execute(true, timeout -> {
        try { var r = PastebinGrpc.newBlockingStub(channel).withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                .getPaste(GetPasteRequest.newBuilder().setKey(key).build());
            return new GetResult(r.getKey(), r.getContent(), r.getCreatedAt(), r.getExpiresAt());
        } catch (StatusRuntimeException e) { throw translate(e); }
    }); }
    private static RpcFailure translate(StatusRuntimeException e) {
        Status.Code c = e.getStatus().getCode();
        RpcFailure.Kind k = c == Status.Code.UNAVAILABLE || c == Status.Code.DEADLINE_EXCEEDED ? RpcFailure.Kind.RETRYABLE
                : c == Status.Code.NOT_FOUND ? RpcFailure.Kind.NOT_FOUND : RpcFailure.Kind.REJECTED;
        return new RpcFailure(k, c.name(), e.getStatus().getDescription(), e);
    }
    @Override public RetryExecutor executor() { return executor; }
    @Override public void close() { channel.shutdownNow(); }
}
