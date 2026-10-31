package kz.kbtu.pastebin.grpcjava;

import kz.kbtu.pastebin.client.ClientOptions;
import kz.kbtu.pastebin.core.PasteService;

/** One-command proof that the generated grpc-java stubs can create and read a paste. */
public final class GrpcJavaSmokeMain {
    public static void main(String[] args) throws Exception {
        PasteService service = new PasteService();
        try (GrpcJavaPastebinServer server = new GrpcJavaPastebinServer(0, service, "https://paste.example/").start();
             GrpcJavaPastebinClient client = new GrpcJavaPastebinClient("localhost", server.port(), new ClientOptions())) {
            var created = client.create("grpc-java smoke test", 600, java.util.UUID.randomUUID().toString());
            var read = client.get(created.key());
            System.out.println("grpc-java smoke: key=" + created.key() + " content=" + read.content());
        }
    }
}
