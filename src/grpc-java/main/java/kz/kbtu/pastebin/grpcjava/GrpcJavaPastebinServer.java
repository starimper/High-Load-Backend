package kz.kbtu.pastebin.grpcjava;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.core.ServiceException;
import kz.kbtu.pastebin.grpc.pb.*;

import java.io.IOException;

/** Standard grpc-java implementation, compiled only by the Maven -Pgrpc-java profile. */
public final class GrpcJavaPastebinServer implements AutoCloseable {
    private static final Metadata.Key<String> IDEM = Metadata.Key.of("idempotency-key", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> IDEM_CONTEXT = Context.key("pastebin-idem");
    private final Server server;

    public GrpcJavaPastebinServer(int port, PasteService service, String baseUrl) {
        ServerInterceptor metadata = new ServerInterceptor() {
            @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                return Contexts.interceptCall(Context.current().withValue(IDEM_CONTEXT, headers.get(IDEM)), call, headers, next);
            }
        };
        this.server = ServerBuilder.forPort(port).addService(ServerInterceptors.intercept(
                new Service(service, baseUrl), metadata)).build();
    }

    public GrpcJavaPastebinServer start() throws IOException { server.start(); return this; }
    public int port() { return server.getPort(); }
    @Override public void close() { server.shutdownNow(); }

    private static final class Service extends PastebinGrpc.PastebinImplBase {
        private final PasteService service; private final String baseUrl;
        Service(PasteService service, String baseUrl) { this.service = service; this.baseUrl = baseUrl; }
        @Override public void createPaste(CreatePasteRequest request, StreamObserver<CreatePasteResponse> out) {
            try {
                String idem = IDEM_CONTEXT.get();
                var c = service.create(request.getContent(), request.getTtlSeconds(), idem);
                out.onNext(CreatePasteResponse.newBuilder().setKey(c.key()).setUrl(baseUrl + c.key())
                        .setExpiresAt(c.expiresAt().toString()).build()); out.onCompleted();
            } catch (ServiceException e) { out.onError(StatusFrom.service(e).asRuntimeException()); }
        }
        @Override public void getPaste(GetPasteRequest request, StreamObserver<GetPasteResponse> out) {
            try { var p = service.get(request.getKey()); out.onNext(GetPasteResponse.newBuilder().setKey(p.key())
                    .setContent(p.content()).setCreatedAt(p.createdAt().toString()).setExpiresAt(p.expiresAt().toString()).build()); out.onCompleted(); }
            catch (ServiceException e) { out.onError(StatusFrom.service(e).asRuntimeException()); }
        }
    }

    static final class StatusFrom {
        static Status service(ServiceException e) { return switch (e.code()) {
            case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT; case TOO_LARGE -> Status.OUT_OF_RANGE;
            case NOT_FOUND -> Status.NOT_FOUND; case IDEMPOTENCY_KEY_REUSED -> Status.FAILED_PRECONDITION;
            case UNAVAILABLE -> Status.UNAVAILABLE; }; }
    }
}
