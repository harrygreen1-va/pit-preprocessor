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

public class UpdateKeysJob implements PostJob {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public void postprocess(JobInfo jobInfo, BatchTracker batch) {
        logger.info("Starting source system update for tables {}", jobInfo.tables());
        Set<String> tables = jobInfo.tables();

        for (String table : tables) {
            logger.info("* Updating keys for the table {}", table);
            updateKeys(batch, table);
        }
    }


    private String genClaimLevelUpdate(String table) {
        String sqlTemplate = """
                UPDATE tblToUpdate
                SET claim_key=claim.claim_key, last_updated_date=GETDATE()
                FROM %s tblToUpdate
                JOIN dim_va_claim claim ON tblToUpdate.file_row_num=claim.file_row_num AND tblToUpdate.etl_batch_id=claim.etl_batch_id
                WHERE tblToUpdate.etl_batch_id=?""";

        return  String.format(sqlTemplate, table);

    }

    private String genLineLevelUpdate(String table, TableMeta meta) {
        String sqlTemplate = """
                UPDATE tblToUpdate
                SET claim_key=line.claim_key, %s=line.claim_detail_key, last_updated_date=GETDATE()
                FROM %s tblToUpdate
                JOIN %s line on tblToUpdate.file_row_num=line.file_row_num AND tblToUpdate.etl_batch_id=line.etl_batch_id
                WHERE tblToUpdate.etl_batch_id=?""";
/*
        String sqlTemplate = """
                UPDATE tblToUpdate
                SET claim_key=claim.claim_key, %s=line.claim_detail_key, last_updated_date=GETDATE()
                FROM %s tblToUpdate
                JOIN dim_va_claim claim ON tblToUpdate.claim_id=claim.claim_id AND tblToUpdate.etl_batch_id=claim.etl_batch_id
                LEFT JOIN %s line on line.claim_key=claim.claim_key and line.%s=tblToUpdate.source_claim_line_id
                AND tblToUpdate.etl_batch_id=line.etl_batch_id
                WHERE tblToUpdate.etl_batch_id=?""";

 */

        // line_fk_name, table_to_update, line_table_name
        return String.format(sqlTemplate, meta.lineFk(), table, meta.lineTable(), meta.lineId());
    }

    private void updateKeys(BatchTracker batch, String table) {
        try (Connection conn = DataSourceFactory.obtainConnection()) {
            TableMeta meta = batch.getTableMeta();
            String sql;
            if (meta.isLineLevel(table)) {
                sql = genLineLevelUpdate(table, meta);
            }
            else {
                sql = genClaimLevelUpdate(table);
            }

            logger.info("Generated the updated keys statement:\n{}", sql);

            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, batch.batchId());

            logger.info("Setting keys in {} for batch id {}", table, batch);

            int numberOfUpdated = stmt.executeUpdate();
            logger.info("Updated {} records", numberOfUpdated);
            if (numberOfUpdated == 0) {
                logger.warn("Did not find any records to update in {} for batch {}", table, batch);
            }

            conn.commit();

        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }

    }
}