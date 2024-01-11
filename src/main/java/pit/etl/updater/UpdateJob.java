package pit.etl.updater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.etl.config.Config;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @since 4/2/2019
 */
public class UpdateJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateJob.class);

    private Config config;
    private final List<String> filtered;
    private final int batchSize;
    private final String table;
    private final String sourceSystem;

    public UpdateJob(Config config, List<String> filtered, int batchSize, String table, String sourceSystem) {
        this.config = config;
        this.filtered = filtered;
        this.batchSize = batchSize;
        this.table = table;
        this.sourceSystem = sourceSystem;
    }

    @Override
    public void run() {
        try (CachedConnection connection = CachedConnection.getConnection(config.get("db.url"))) {
            for (String batchId : filtered) {
                String sql = String.format("update top(%d) %s set source_system = ? where etl_batch_id = ? and source_system <> ?", batchSize, table);
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int rc = batchSize; rc == batchSize; ) {
                        statement.setObject(1, sourceSystem);
                        statement.setObject(2, batchId);
                        statement.setObject(3, sourceSystem);
                        rc = statement.executeUpdate();
                        LOGGER.info("{} updated {} rows in {}", Thread.currentThread().getName(), rc, table);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.error("sql", e);
        }
    }
}
