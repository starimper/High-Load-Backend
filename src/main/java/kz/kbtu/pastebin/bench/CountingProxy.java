package kz.kbtu.pastebin.bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/** TCP forwarder that counts the bytes in both directions (what really crosses the wire, above TCP/IP headers). */
final class CountingProxy implements AutoCloseable {
    final AtomicLong toServer = new AtomicLong();
    final AtomicLong toClient = new AtomicLong();
    private final ServerSocket listener;
    private final String host;
    private final int port;

    CountingProxy(String targetHost, int targetPort) throws IOException {
        this.host = targetHost;
        this.port = targetPort;
        this.listener = new ServerSocket(0);
        Thread t = new Thread(this::acceptLoop, "proxy-accept");
        t.setDaemon(true);
        t.start();
    }

    int port() {
        return listener.getLocalPort();
    }

    void reset() {
        toServer.set(0);
        toClient.set(0);
    }

    private void acceptLoop() {
        try {
            while (true) {
                Socket c = listener.accept();
                Socket s = new Socket(host, port);
                c.setTcpNoDelay(true);
                s.setTcpNoDelay(true);
                pump(c, s, toServer);
                pump(s, c, toClient);
            }
        } catch (IOException e) {
            // closed
        }
    }

    private static void pump(Socket from, Socket to, AtomicLong counter) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[64 * 1024];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int n;
                while ((n = in.read(buf)) > 0) {
                    counter.addAndGet(n);
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException e) {
                // connection closed
            } finally {
                try { from.close(); } catch (IOException ignored) { }
                try { to.close(); } catch (IOException ignored) { }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }
}
