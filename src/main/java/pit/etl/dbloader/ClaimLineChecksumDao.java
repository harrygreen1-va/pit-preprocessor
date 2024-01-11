package pit.etl.dbloader;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.HashRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class ClaimLineChecksumDao {
    private final int commitChunkSize;

    public ClaimLineChecksumDao(Config config) {
        commitChunkSize = config.getInt("commit.chunk.size");
    }

    private final static String INSERT_INTO_CHECKSUM_SQL = """
            insert into etl.claim_line_checksum(claim_id, claim_line_id, file_row_num, raw_fields, checksum, etl_batch_id)
            values(?,?,?,?,?,?)
                        """;

    @SneakyThrows(SQLException.class)
    public void populate(BatchTracker batchEntry, List<HashRecord> hashRecords) {
        try (Connection conn = obtainConnection(); var stmt = conn.prepareStatement(INSERT_INTO_CHECKSUM_SQL)) {
            log.info("Populating claim_line_checksum ...");
            log.info("Prepared insert:\n{}", INSERT_INTO_CHECKSUM_SQL);
            int insertCount = 0;
            for (var hashRecord : hashRecords) {
                addToInsertBatch(stmt, hashRecord, batchEntry);
                ++insertCount;

                if (insertCount % commitChunkSize==0) {
                    stmt.executeBatch();
                    conn.commit();
                    log.info("Committed after inserting {} records of {}", insertCount, hashRecords.size());
                }
            }
            if (insertCount % commitChunkSize!=0) {
                stmt.executeBatch();
                conn.commit();
            }
            log.info("Inserted {} records into etl.claim_line_checksum", insertCount);
        }
    }

    private void addToInsertBatch(PreparedStatement insertStmt, HashRecord hashRecord, BatchTracker batch) throws SQLException {
        //claim_id, claim_line_id, raw_fields, checksum, etl_batch_id
        int i = 0;
        insertStmt.setString(++i, hashRecord.claimId());
        insertStmt.setString(++i, hashRecord.lineId());
        insertStmt.setInt(++i, hashRecord.rowNum());
        insertStmt.setString(++i, hashRecord.rawFieldsStr());
        insertStmt.setString(++i, hashRecord.hashValue());
        insertStmt.setString(++i, batch.batchId());

        insertStmt.addBatch();
    }

    private Connection obtainConnection() throws SQLException {
        return DataSourceFactory.dataSource().getConnection();
    }
}