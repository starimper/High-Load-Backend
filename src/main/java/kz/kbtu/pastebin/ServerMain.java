package kz.kbtu.pastebin;

import kz.kbtu.pastebin.core.FaultInjector;
import kz.kbtu.pastebin.core.PasteService;
import kz.kbtu.pastebin.core.PostgresPasteService;
import kz.kbtu.pastebin.grpc.GrpcPastebinServer;
import kz.kbtu.pastebin.rest.NettyRestPastebinServer;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Starts both protocols over ONE PasteService, so they serve the same data.
 * Usage: java -cp pastebin-protocol.jar kz.kbtu.pastebin.ServerMain [--protocol both|rest|grpc] [--grpc-port 9090] [--rest-port 8080]
 *        [--threads 32] [--error-rate 0] [--lost-rate 0] [--slow-rate 0] [--slow-ms 0]
 */
public final class ServerMain {
    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i], args[i + 1]);
        }
        int grpcPort = Integer.parseInt(a.getOrDefault("--grpc-port", "9090"));
        int restPort = Integer.parseInt(a.getOrDefault("--rest-port", "8080"));
        String protocol = a.getOrDefault("--protocol", "both").toLowerCase();
        if (!protocol.equals("both") && !protocol.equals("rest") && !protocol.equals("grpc"))
            throw new IllegalArgumentException("--protocol must be both, rest, or grpc");
        int threads = Integer.parseInt(a.getOrDefault("--threads", "32"));
        FaultInjector faults = new FaultInjector(
                Double.parseDouble(a.getOrDefault("--error-rate", "0")),
                Double.parseDouble(a.getOrDefault("--lost-rate", "0")),
                Double.parseDouble(a.getOrDefault("--slow-rate", "0")),
                Long.parseLong(a.getOrDefault("--slow-ms", "0")));
        PostgresPasteService postgres = null;
        PasteService service;
        if (a.containsKey("--postgres-url")) {
            postgres = new PostgresPasteService(a.get("--postgres-url"), a.getOrDefault("--postgres-user", "postgres"),
                    a.getOrDefault("--postgres-password", System.getenv().getOrDefault("POSTGRES_PASSWORD", "")));
            service = postgres;
        } else service = new PasteService();
        SslContext tls = null;
        if (a.containsKey("--tls-cert") && a.containsKey("--tls-key")) {
            tls = SslContextBuilder.forServer(new java.io.File(a.get("--tls-cert")),
                    new java.io.File(a.get("--tls-key"))).build();
        } else if (a.containsKey("--tls-keystore") && a.containsKey("--tls-keystore-password")) {
            java.security.KeyStore ks = java.security.KeyStore.getInstance("PKCS12");
            char[] password = a.get("--tls-keystore-password").toCharArray();
            try (java.io.InputStream in = new java.io.FileInputStream(a.get("--tls-keystore"))) { ks.load(in, password); }
            javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, password);
            tls = SslContextBuilder.forServer(kmf).build();
        }
        String baseUrl = (tls == null ? "http" : "https") + "://localhost:" + restPort + "/p/";
        if (protocol.equals("both") || protocol.equals("grpc"))
            new GrpcPastebinServer(grpcPort, service, faults, baseUrl, threads, tls).start();
        if (protocol.equals("both") || protocol.equals("rest"))
            new NettyRestPastebinServer(restPort, service, faults, baseUrl, threads, tls).start();
        PostgresPasteService database = postgres;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { if (database != null) database.close(); }));
        System.out.println("READY protocol=" + protocol + " grpc=" + grpcPort + " rest=" + restPort);
        Thread.currentThread().join();
    }
}
