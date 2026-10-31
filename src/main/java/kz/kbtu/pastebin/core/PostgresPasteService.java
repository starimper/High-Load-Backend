package kz.kbtu.pastebin.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;

/** Optional/demo PostgreSQL persistence adapter for local browser demos and integration tests. */
public final class PostgresPasteService extends PasteService implements AutoCloseable {
    private final HikariDataSource dataSource;

    public PostgresPasteService(String url, String user, String password) { this(url, user, password, 10); }

    public PostgresPasteService(String url, String user, String password, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url); config.setUsername(user); config.setPassword(password);
        config.setMaximumPoolSize(Math.max(2, maxPoolSize));
        config.setMinimumIdle(Math.min(2, Math.max(1, maxPoolSize)));
        config.setConnectionTimeout(5_000); config.setValidationTimeout(2_000);
        config.setPoolName("pastebin-postgres");
        this.dataSource = new HikariDataSource(config);
        try { Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .baselineOnMigrate(true).baselineVersion("0").load().migrate(); }
        catch (RuntimeException e) { dataSource.close(); throw unavailable("cannot migrate PostgreSQL schema: " + e.getMessage(), e); }
    }

    private Connection connection() throws SQLException { return dataSource.getConnection(); }

    @Override public Created create(String content, long ttlSeconds, String idempotencyKey) {
        validate(content, ttlSeconds);
        if (idempotencyKey != null && idempotencyKey.length() > 200)
            throw new ServiceException(ErrorCode.INVALID_ARGUMENT, "Idempotency-Key is too long");
        String fingerprint = fingerprint(content, ttlSeconds);
        try (Connection c = connection()) {
            c.setAutoCommit(false);
            try {
                if (idempotencyKey != null && !idempotencyKey.isBlank()) {
                    try (PreparedStatement purge = c.prepareStatement("DELETE FROM idempotency_keys WHERE idem_key=? AND created_at < now()-interval '24 hours'")) {
                        purge.setString(1, idempotencyKey); purge.executeUpdate();
                    }
                    try (PreparedStatement p = c.prepareStatement("INSERT INTO idempotency_keys(idem_key,fingerprint,created_at) VALUES (?,?,now()) ON CONFLICT (idem_key) DO NOTHING")) {
                        p.setString(1, idempotencyKey); p.setString(2, fingerprint);
                        if (p.executeUpdate() == 0) {
                            try (PreparedStatement q = c.prepareStatement("SELECT fingerprint,paste_key FROM idempotency_keys WHERE idem_key=? FOR UPDATE")) {
                                q.setString(1, idempotencyKey);
                                try (ResultSet r = q.executeQuery()) {
                                    if (!r.next()) { c.rollback(); return create(content, ttlSeconds, idempotencyKey); }
                                    if (!fingerprint.equals(r.getString(1))) throw new ServiceException(ErrorCode.IDEMPOTENCY_KEY_REUSED, "this Idempotency-Key was already used with a different request");
                                    String key = r.getString(2);
                                    if (key == null) { c.rollback(); return create(content, ttlSeconds, idempotencyKey); }
                                    Instant expires = snippetExpires(c, key);
                                    c.commit(); return new Created(key, expires, true);
                                }
                            }
                        }
                    }
                    Created created = insert(c, content, ttlSeconds);
                    try (PreparedStatement u = c.prepareStatement("UPDATE idempotency_keys SET paste_key=? WHERE idem_key=?")) {
                        u.setString(1, created.key()); u.setString(2, idempotencyKey); u.executeUpdate();
                    }
                    c.commit(); return created;
                }
                Created created = insert(c, content, ttlSeconds); c.commit(); return created;
            } catch (RuntimeException | SQLException e) { try { c.rollback(); } catch (SQLException ignored) { } throw e; }
        } catch (ServiceException e) { throw e; }
        catch (SQLException e) { throw unavailable("PostgreSQL create failed: " + e.getMessage(), e); }
    }

    private Created insert(Connection c, String content, long ttlSeconds) throws SQLException {
        Instant now = Instant.now(); Instant expires = now.plusSeconds(ttlSeconds);
        for (int i = 0; i < 10; i++) {
            String key = KeyGenerator.RANDOM_BASE62.next();
            try (PreparedStatement p = c.prepareStatement("INSERT INTO snippets(paste_key,created_at,expires_at,content) VALUES (?,?,?,?) ON CONFLICT DO NOTHING")) {
                p.setString(1, key); p.setTimestamp(2, Timestamp.from(now)); p.setTimestamp(3, Timestamp.from(expires)); p.setString(4, content);
                if (p.executeUpdate() == 1) return new Created(key, expires, false);
            }
        }
        throw new ServiceException(ErrorCode.UNAVAILABLE, "could not find a free key");
    }

    @Override public Paste get(String key) {
        try (Connection c = connection(); PreparedStatement p = c.prepareStatement("SELECT paste_key,content,created_at,expires_at FROM snippets WHERE paste_key=? AND expires_at>now()")) {
            p.setString(1, key);
            try (ResultSet r = p.executeQuery()) {
                if (!r.next()) throw new ServiceException(ErrorCode.NOT_FOUND, "snippet not found");
                return new Paste(r.getString(1), r.getString(2), r.getTimestamp(3).toInstant(), r.getTimestamp(4).toInstant());
            }
        } catch (ServiceException e) { throw e; }
        catch (SQLException e) { throw unavailable("PostgreSQL read failed: " + e.getMessage(), e); }
    }

    @Override public int purgeExpired() {
        try (Connection c = connection(); Statement s = c.createStatement()) {
            int n = s.executeUpdate("DELETE FROM snippets WHERE expires_at<=now()");
            s.executeUpdate("DELETE FROM idempotency_keys WHERE created_at<now()-interval '24 hours'"); return n;
        } catch (SQLException e) { throw unavailable("PostgreSQL cleanup failed", e); }
    }
    @Override public int size() { try (Connection c=connection(); Statement s=c.createStatement(); ResultSet r=s.executeQuery("SELECT count(*) FROM snippets WHERE expires_at>now()")) { r.next(); return r.getInt(1); } catch(SQLException e){ throw unavailable("PostgreSQL count failed",e); } }
    @Override public Map<String,Integer> contentCounts() {
        Map<String,Integer> counts = new java.util.HashMap<>();
        try (Connection c = connection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT content,count(*) FROM snippets WHERE expires_at>now() GROUP BY content")) {
            while (r.next()) counts.put(r.getString(1), r.getInt(2)); return counts;
        } catch (SQLException e) { throw unavailable("PostgreSQL content count failed", e); }
    }
    @Override public void clear() { try (Connection c=connection(); Statement s=c.createStatement()){ s.executeUpdate("DELETE FROM idempotency_keys"); s.executeUpdate("DELETE FROM snippets"); } catch(SQLException e){ throw unavailable("PostgreSQL clear failed",e); } }
    @Override public void close() { dataSource.close(); }

    private Instant snippetExpires(Connection c, String key) throws SQLException {
        try (PreparedStatement p=c.prepareStatement("SELECT expires_at FROM snippets WHERE paste_key=?")){ p.setString(1,key); try(ResultSet r=p.executeQuery()){ if(!r.next()) throw new ServiceException(ErrorCode.UNAVAILABLE,"idempotency record points to missing snippet"); return r.getTimestamp(1).toInstant(); }}
    }
    private static void validate(String content,long ttl){ if(content==null||content.isEmpty()) throw new ServiceException(ErrorCode.INVALID_ARGUMENT,"content is empty"); if(ttl<MIN_TTL_SECONDS||ttl>MAX_TTL_SECONDS) throw new ServiceException(ErrorCode.INVALID_ARGUMENT,"ttl_seconds must be between 600 and 2592000"); if(content.getBytes(StandardCharsets.UTF_8).length>MAX_BYTES) throw new ServiceException(ErrorCode.TOO_LARGE,"content is larger than 1 MB"); }
    private static String fingerprint(String content,long ttl){ try { MessageDigest m=MessageDigest.getInstance("SHA-256"); m.update((ttl+":"+content).getBytes(StandardCharsets.UTF_8)); return HexFormat.of().formatHex(m.digest()); } catch(Exception e){ throw new IllegalStateException(e); } }
    private static ServiceException unavailable(String message, Throwable cause){ return new ServiceException(ErrorCode.UNAVAILABLE,message); }
}
