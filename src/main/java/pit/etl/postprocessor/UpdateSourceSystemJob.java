package pit.etl.postprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.TableMeta;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.DataSourceFactory;
import pit.etl.dbloader.JobInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

public class UpdateSourceSystemJob implements PostJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());


    public void postprocess(JobInfo jobInfo, BatchTracker batch) {
        logger.info("Starting source system update for tables {}", jobInfo.tables());
        Set<String> tables = jobInfo.tables();
        TableMeta meta = batch.getTableMeta();
        for (String table : tables) {
            if (meta.isTableExcluded(table)) {
                logger.info("Table {} won't be process since it is not used by the batch {}", table, batch.fileInfo());
            }
            else {
                updateSourceSystemForTable(batch, table);
            }
        }
    }

    private void updateSourceSystemForTable(BatchTracker batch, String table) {
        try (Connection conn = DataSourceFactory.obtainConnection()) {



            String sql = String.format("update %s set source_system = ? where etl_batch_id = ? and source_system <> ?", table);
            PreparedStatement stmt = conn.prepareStatement(sql);

            String sourceSystem = batch.fileInfo().sourceSystem();

            stmt.setString(1, sourceSystem);
            stmt.setString(3, sourceSystem);
            stmt.setString(2, batch.batchId());

            logger.info("Updating {} for batch id {} with source system {}", table, batch.batchId(), sourceSystem);

            int numberOfUpdated = stmt.executeUpdate();
            logger.info("Updated {} records", numberOfUpdated);
            if (numberOfUpdated == 0) {
                logger.warn("Did not find any records to update in {} for batch {}", table, batch.batchId());
            }

            conn.commit();

        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }

    }
}
