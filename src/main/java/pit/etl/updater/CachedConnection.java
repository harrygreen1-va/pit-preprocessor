package pit.etl.updater;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @since 4/2/2019
 */
class CachedConnection implements AutoCloseable {
    private static final ThreadLocal<CachedConnection> CONNECTION_POOL = new ThreadLocal<>();
    private final Connection connection;

    private CachedConnection(Connection connection) {
        this.connection = connection;
    }

    static CachedConnection getConnection(String url) throws SQLException {
        CachedConnection connection = CONNECTION_POOL.get();
        if (connection == null) {
            CONNECTION_POOL.set(connection = new CachedConnection(DriverManager.getConnection(url)));
        }
        return connection;
    }

    public void close() {
    }

    PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }
}
