package pit.etl.batchlog;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.util.JdbcUtils;

import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@SuppressWarnings({"WeakerAccess", "SqlInsertValues"})
public class BatchLogDao {

    public final static String PREPROCESSING_STATUS = "Preprocessing";
    public final static String IN_PROCESS_STATUS = "In Process";
    public final static String SUNBBED_STATUS = "SNUBBED";
    public final static String TERMINAL_STATUS = "TERMINAL";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    // Note: we don't use prepared statement caching as per
    // https://github.com/brettwooldridge/HikariCP (antipattern)

    @SuppressWarnings("SqlResolve")
    public void insertFileBatchEntry(BatchTracker batchEntry) {
        try (Connection conn = obtainConnection()) {

            String columns = "etl_batch_id, source_system, feed_date, start_date_time, last_updated_date, last_updated_user, file_size, file_name, batch_status, eci_id, to_score_indicator";
            String placeholders = JdbcUtils.placeholdersFromColumns(columns);

            String sql = "INSERT INTO claim_batch_log  (" + columns + ") " + "VALUES (" + placeholders
                    + ")";
            PreparedStatement stmt = conn.prepareStatement(sql);

            logger.info("Prepared insert:\n{}", sql + batchEntry.batchId());
            int i = 1;
            stmt.setString(i++, batchEntry.batchId());
            // source_system column -- actually, a form type
            stmt.setString(i++, batchEntry.fileInfo().normalizedFormType());

            stmt.setObject(i++, batchEntry.fileInfo().feedDate());

            Timestamp currentTimestamp = Timestamp.valueOf(batchEntry.startTimestamp());
            stmt.setTimestamp(i++, currentTimestamp);

            stmt.setTimestamp(i++, JdbcUtils.currentTimestamp());
            stmt.setString(i++, JdbcUtils.getUserName());
            //file_size
            stmt.setLong(i++, FileUtils.sizeOf(batchEntry.fileInfo().file()));
            // file name
            stmt.setString(i++, batchEntry.fileInfo().file().getName());
            stmt.setString(i++, PREPROCESSING_STATUS);

            if (batchEntry.eci() != null && batchEntry.fileInfo().isToScore()) {
                stmt.setLong(i++, batchEntry.eci().id());
            }
            else {
                stmt.setNull(i++, Types.INTEGER);
            }

            //noinspection UnusedAssignment
            stmt.setString(i++, JdbcUtils.boolToYesNo(batchEntry.fileInfo().isToScore()));

            stmt.executeUpdate();
            conn.commit();

            logger.info("Inserted batch log entry: " + batchEntry);

        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    public void updateBatchEntryWithSuccess(String batchId, int numberOfRows, int numberOfConformantRows, String validationReport) {
        try (Connection conn = obtainConnection()) {

            String sql = "update claim_batch_log set batch_status=?, number_of_rows=?, number_of_conformant_rows=?, error_text=?, " +
                    "last_updated_date=?, last_updated_user=? "
                    + "where etl_batch_id=?";
            PreparedStatement stmt = conn.prepareStatement(sql);

            int i = 1;

            String status = IN_PROCESS_STATUS;
            if (numberOfConformantRows == 0 || numberOfRows == 0) {
                status = SUNBBED_STATUS;
            }
            stmt.setString(i++, status);
            stmt.setInt(i++, numberOfRows);
            if (numberOfConformantRows >= 0) {
                stmt.setInt(i++, numberOfConformantRows);
            }
            else {
                stmt.setNull(i++, Types.INTEGER);
            }

            // could be null
            stmt.setString(i++, validationReport);

            doUpdate(stmt, i, batchId);

            logger.info("Updated batch log entry {} with the status '{}'", batchId, status);
        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    public void updateBatchEntryWithError(String batchId, String errorType, String errorMsg) {

        try (Connection conn = obtainConnection()) {

            String sql = "update claim_batch_log set batch_status=?, error_text=?, last_updated_date=?, last_updated_user=? "
                    + "where etl_batch_id=?";
            PreparedStatement updateBatchLogValidationStatement = conn.prepareStatement(sql);

            PreparedStatement stmt = updateBatchLogValidationStatement;
            int i = 1;
            String status = errorType;
            // status
            stmt.setString(i++, status);
            stmt.setString(i++, errorMsg);
            doUpdate(stmt, i, batchId);

            logger.info("Updated batch log entry {} with the status '{}'", batchId, status);
        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    private final static String BATCH_UPDATE_SQL = """
            update claim_batch_log set batch_status=?, end_date_time=?, last_updated_date=?, last_updated_user=?
            where etl_batch_id=?
            """;

    public void updateBatchEntryStatus(String batchId, String status) {
        try (Connection conn = obtainConnection(); var stmt = conn.prepareStatement(BATCH_UPDATE_SQL)) {

            int i = 1;
            stmt.setString(i++, status);
            var endTimestamp = JdbcUtils.currentTimestamp();
            if (IN_PROCESS_STATUS.equals(status)) {
                endTimestamp = null;
            }
            stmt.setObject(i++, endTimestamp);
            var numberOfUpdated = doUpdate(stmt, i, batchId);

            logger.info("Updated batch {} log entry {} with the status '{}'", numberOfUpdated, batchId, status);
        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    private int doUpdate(PreparedStatement stmt, int i, String batchId) throws SQLException {
        stmt.setTimestamp(i++, JdbcUtils.currentTimestamp());
        stmt.setString(i++, JdbcUtils.getUserName());

        stmt.setString(i, batchId);

        return stmt.executeUpdate();
    }


    @SuppressWarnings("SqlResolve")
    public final static String IN_PROCESS_ECI_STATUS = "In Process";

    public Eci selectInProcessEci() {
        Eci eci = null;
        if (!Config.isDataLoadingEnabled()) {
            return eci;
        }

        try (Connection conn = obtainConnection()) {
            String sql = "select max(eci_id) as eci_id from etl_cst_interface where eci_status='"
                    + IN_PROCESS_ECI_STATUS + "'";

            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                logger.warn("Found no " + IN_PROCESS_ECI_STATUS
                        + " ECIs in etl_cst_interface, will be loading with null ECI");
            }
            else {
                long eciId = rs.getLong(1);
                logger.info("Found '{}' eci_id {} for batch loading", IN_PROCESS_ECI_STATUS, eciId);

                eci = new Eci(eciId);
            }
            return eci;
        } catch (SQLException e) {
            // TODO: warning
            throw new UnrecoverableException(e);
        }
    }

    public BatchTracker selectFirstBatchInfoForFileName(String fileName) {
        List<BatchTracker> batchInfos = selectBatchInfoFromFileNames(Collections.singleton(fileName));
        if (batchInfos.isEmpty()) {
            return null;
        }
        return batchInfos.get(0);
    }


    public List<BatchTracker> selectBatchInfoFromFileNames(Set<String> fileNames) {
        try (Connection conn = obtainConnection()) {

            String sql =
                    "select etl_batch_id, file_name from claim_batch_log cbl where " +
                            "file_name in (" + JdbcUtils.genListForIn(fileNames) + ") " +
                            "and etl_batch_id in\n" +
                            "(select top 1 etl_batch_id from claim_batch_log cbl_latest\n" +
                            "where cbl_latest.file_name=cbl.file_name\n" +
                            "and batch_status != 'in process'\n" +
                            "order by last_updated_date desc)";

            PreparedStatement stmt = conn.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();

            List<BatchTracker> batchTrackers = new ArrayList<>();
            while (rs.next()) {
                String batchId = rs.getString(1);
                String fileName = rs.getString(2);

                DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(new File(fileName));
                BatchTracker batch = new BatchTracker(fileInfo, batchId);

                batchTrackers.add(batch);
            }

            return batchTrackers;

        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    private final static String SELECT_BATCHES_FOR_DATE_RANGE = """
            select etl_batch_id, file_name, number_of_conformant_rows
            from claim_batch_log
            where batch_status in ('TERMINAL')
            and end_date_time >= ? and CAST(end_date_time as DATE) <= ?
                    """;
    private final static String BATCH_PREFIX_COND = "and left(etl_batch_id, %d)='%s'";

    public List<BatchTracker> selectBatchInfoForDates(LocalDate fromDate, LocalDate toDate, String sourceSystemPrefix) {

        var sql = SELECT_BATCHES_FOR_DATE_RANGE;

        if (StringUtils.isNotBlank(sourceSystemPrefix)) {
            int prefixLength = sourceSystemPrefix.length();
            sql += String.format(BATCH_PREFIX_COND, prefixLength, sourceSystemPrefix);
        }
        logger.info(sql);
        //noinspection SqlSourceToSinkFlow
        try (Connection conn = obtainConnection(); var stmt = conn.prepareStatement(sql)) {
            int i = 0;
            stmt.setDate(++i, java.sql.Date.valueOf(fromDate));
            stmt.setDate(++i, java.sql.Date.valueOf(toDate));

            ResultSet rs = stmt.executeQuery();

            List<BatchTracker> batchTrackers = new ArrayList<>();
            while (rs.next()) {
                i = 0;
                String batchId = rs.getString(++i);
                String fileName = rs.getString(++i);
                int numberOfRows = rs.getInt(++i);

//                DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(new File(fileName));
                BatchTracker batch = new BatchTracker(null, batchId);
                batch.setLoadFile(new File(fileName));
                batch.setConformantLineCount(numberOfRows);
                batchTrackers.add(batch);
            }

            return batchTrackers;

        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }


    private Connection obtainConnection() throws SQLException {
        return DataSourceFactory.obtainConnection();
    }

}