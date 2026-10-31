package kz.kbtu.pastebin;

import kz.kbtu.pastebin.core.PostgresPasteService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.UUID;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/** Optional integration test against a real PostgreSQL instance.
 * Run with -Dpostgres.integration=true and the postgres.* properties. */
@EnabledIfSystemProperty(named = "postgres.integration", matches = "true")
class PostgresPasteServiceIntegrationTest {
    @Test
    void createGetIdempotencyAndCleanupWorkAgainstPostgres() {
        String adminUrl = System.getProperty("postgres.url", "jdbc:postgresql://localhost:5432/postgres");
        String user = System.getProperty("postgres.user", "postgres");
        String password = System.getProperty("postgres.password", "admin");
        String schema = "pastebin_it_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection admin = DriverManager.getConnection(adminUrl, user, password);
             Statement statement = admin.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
            try {
                String url = adminUrl + (adminUrl.contains("?") ? "&" : "?") + "currentSchema=" + schema;
                try (PostgresPasteService service = new PostgresPasteService(url, user, password)) {
                    String idempotencyKey = UUID.randomUUID().toString();
                    var first = service.create("postgres integration", 600, idempotencyKey);
                    var replay = service.create("postgres integration", 600, idempotencyKey);
                    assertEquals(first.key(), replay.key());
                    assertTrue(replay.replayed());
                    assertEquals("postgres integration", service.get(first.key()).content());
                    assertThrows(RuntimeException.class, () -> service.create("different", 600, idempotencyKey));
                    assertEquals(1, service.size());
                }
            } finally {
                statement.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        } catch (Exception e) {
            throw new AssertionError("PostgreSQL integration test failed", e);
        }
    }
}
