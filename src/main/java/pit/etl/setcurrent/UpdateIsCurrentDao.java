package pit.etl.setcurrent;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import pit.etl.config.DataSourceFactory;
import pit.util.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class UpdateIsCurrentDao {

    @SneakyThrows(SQLException.class)
    List<IsCurrentRecord> fetchClaimsToProcess(String sql, boolean isForResubmission) {
        List<IsCurrentRecord> records = new ArrayList<>();
        //noinspection SqlSourceToSinkFlow
        try (var conn = obtainConnection(); var preparedStmt = conn.prepareStatement(sql)) {
            log.info("Fetching claims to set is_current. Is this for resubmission: {}\n{}", isForResubmission, sql);
            var rs = preparedStmt.executeQuery();
            log.debug("Finished fetching claims for is_current");
            while (rs.next()) {
                int i = 0;
                var claimKey = rs.getLong(++i);
                var claimId = rs.getString(++i);
                var isCurrentValue = rs.getString(++i);
                var updatedBatchId = rs.getString(++i);
                Long nextSubmissionClaimKey = null;

                if (isForResubmission) {
                    nextSubmissionClaimKey = rs.getLong(++i);
                }
                var updatingEtlBatchId = rs.getString(++i);
                String isCurrentFromLog = null;
                if (!isForResubmission) {
                    isCurrentFromLog = rs.getString(++i);
                }
                records.add(new IsCurrentRecord(claimKey, claimId, isCurrentValue, updatedBatchId, updatingEtlBatchId,
                        nextSubmissionClaimKey, isCurrentFromLog));
            }
        }
        log.info("Fetched {} records", records.size());

        return records;
    }

    @SneakyThrows(SQLException.class)
    void populateIsCurrentLogAndPerformUpdate(List<IsCurrentUpdate> isCurrentUpdates, IsCurrentBatchesContainer isCurrentBatchesContainer, boolean isForResubmissions, boolean isDryRun) {
        try (Connection conn = obtainConnection()) {
            populateIsCurrentLog(conn, isCurrentUpdates, isCurrentBatchesContainer, isForResubmissions);

            if (isDryRun) {
                log.warn("Running in the dry run mode; the etl.is_current_log table will be populated but no updates will be made");
            }
            else {
                updateIsCurrent(conn, isCurrentBatchesContainer, isForResubmissions);
            }
            conn.commit();
        }
    }


    @SuppressWarnings("SqlResolve")
    private final static String INSERT_INTO_LOG_SQL = """
            insert into etl.is_current_log(claim_key, claim_id, old_is_current, is_current, etl_batch_id, updated_etl_batch_id, next_submission_claim_key)
            values(?,?,?,?,?,?,?)
                        """;

    private void populateIsCurrentLog(Connection conn, List<IsCurrentUpdate> isCurrentUpdates, IsCurrentBatchesContainer isCurrentBatchesContainer, boolean isForResubmissions) throws SQLException {
        deleteLogEntriesForBatches(conn, isCurrentBatchesContainer, isForResubmissions);

        try (var insertIntoLogStmt = conn.prepareStatement(INSERT_INTO_LOG_SQL)) {
            log.info("Populating is_current_log ...");
            int updateCount = 0;
            for (var isCurrentUpdate : isCurrentUpdates) {
                // TODO: insert in chunks
                addLogInsertToInsertBatch(insertIntoLogStmt, isCurrentUpdate);
                ++updateCount;
            }
            insertIntoLogStmt.executeBatch();
            log.info("Inserted {} records into is_current_log", updateCount);
        }

    }

    private void addLogInsertToInsertBatch(PreparedStatement insertStmt, IsCurrentUpdate isCurrentUpdate) throws SQLException {
        //claim_key, claim_id, old_is_current, is_current, etl_batch_id
        int i = 0;
        insertStmt.setLong(++i, isCurrentUpdate.isCurrentRecord().claimKey());
        insertStmt.setString(++i, isCurrentUpdate.isCurrentRecord().claimId());
        insertStmt.setString(++i, isCurrentUpdate.isCurrentRecord().isCurrentValue());
        insertStmt.setString(++i, isCurrentUpdate.isCurrentToSetStr());
        // etl_batch_id that initiated the update. For resubmissions, it's the batch_id of the next claim in the chain, i.e. the claim with the prior claim key pointing to the claim we're updating
        insertStmt.setString(++i, isCurrentUpdate.isCurrentRecord().updatingBatchId());
        // updated_etl_batch_id -- the batch_id of the claim being changed
        insertStmt.setString(++i, isCurrentUpdate.isCurrentRecord().updatedBatchId());
        insertStmt.setObject(++i, isCurrentUpdate.isCurrentRecord().nextSubmissionClaimKey());


        insertStmt.addBatch();
    }

    @SuppressWarnings("SqlResolve")
    private final static String DELETE_LOG_ENTRIES_FOR_BATCH_SQL = """
            delete from etl.is_current_log
            where etl_batch_id in ( %s ) and next_submission_claim_key %s
                                    """;

    private void deleteLogEntriesForBatches(Connection conn, IsCurrentBatchesContainer isCurrentBatchesContainer, boolean isForResubmissions) throws SQLException {
        var deleteSql = String.format(DELETE_LOG_ENTRIES_FOR_BATCH_SQL, JdbcUtils.genListForIn(isCurrentBatchesContainer.batchIds()), genNextKeyCond(isForResubmissions));
        log.debug(deleteSql);
        try (var deleteLogEntriesStmt = conn.prepareStatement(deleteSql)) {
            var numberOfUpdated = deleteLogEntriesStmt.executeUpdate();
            if (numberOfUpdated > 0) {
                log.info("Deleted {} records from is_current_log for batches {}", numberOfUpdated, isCurrentBatchesContainer.batchIds());
            }
        }
    }

    @SuppressWarnings("SqlResolve")
    private final static String UPDATE_IS_CURRENT_SQL = """
            update tblToUpdate set is_current=update_log.is_current, end_date=iif(update_log.is_current='Y', null, getdate()), last_updated_date=getdate(), last_updated_user=suser_name()
            from etl.is_current_log update_log
            join %s tblToUpdate on update_log.claim_key=tblToUpdate.claim_key
            where update_log.etl_batch_id in ( %s ) and update_log.next_submission_claim_key %s
                                    """;


    private void updateIsCurrent(Connection conn, IsCurrentBatchesContainer isCurrentBatchesContainer, boolean isForResubmissions) throws SQLException {
        updateIsCurrentForTable(conn, "dim_va_claim", isCurrentBatchesContainer.batchIds(), isForResubmissions);
        updateChildTables(conn, isCurrentBatchesContainer, isForResubmissions);
    }

    private void updateChildTables(Connection conn, IsCurrentBatchesContainer isCurrentBatchesContainer, boolean isForResubmissions) throws SQLException {
        for (var claimTypeWithBatchIds : isCurrentBatchesContainer.batchIdsByClaimType().entrySet()) {
            updateIsCurrentForTable(conn, claimTypeWithBatchIds.getKey().tableMeta().lineTable(), claimTypeWithBatchIds.getValue(), isForResubmissions);
        }
    }

    private void updateIsCurrentForTable(Connection conn, String tableName, Collection<String> batchIds, boolean isForResubmissions) throws SQLException {
        log.info("Updating {} to set is_current", tableName);
        var inClause = JdbcUtils.genListForIn(batchIds);
        var updateSql = String.format(UPDATE_IS_CURRENT_SQL, tableName, inClause, genNextKeyCond(isForResubmissions));
        log.info("\n" + updateSql);
        try (var updateIsCurrentStmt = conn.prepareStatement(updateSql)) {
            var numberOfUpdated = updateIsCurrentStmt.executeUpdate();
            log.info("Updated {} records in {} to set is_current", numberOfUpdated, tableName);
        }
    }

    private String genNextKeyCond(boolean isForResubmissions) {
        return isForResubmissions ? "is not null" : "is null";
    }

    private Connection obtainConnection() throws SQLException {
        var conn = DataSourceFactory.dataSource().getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

}