package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.TableMeta;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.dbloader.validation.LineValidation;
import pit.etl.dbloader.validation.SourceDataValidator;
import pit.util.DateUtils;
import pit.util.SourceDataUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pit.etl.filecopytransform.fbcs.DelimitedFileInfo.DELIM;
import static pit.util.SourceDataUtils.valuesFromLine;

@Accessors(fluent = true)
@Getter
@Setter
public class DbFileLoader {

    private int loggingChunkSize;
    private int commitChunkSize;
    private String delimiter = DELIM;

    public DbFileLoader(Config config) {
        loggingChunkSize = config.getInt("logging.chunk.size");
        commitChunkSize = config.getInt("commit.chunk.size");
    }

    // separate unit tests for validators
    // Date validation
    // Store validation in a table
    // Same table, separate tables?
    // Notifications( including how many was inserted based on a table)

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public void process(BatchTracker batchEntry, File inputFile, List<String> jobConfigNames) {
        // TODO: process in parallel:
        for (String jobName : jobConfigNames) {
            process(batchEntry, inputFile, jobName);
        }
    }

    // This is used only to insert rejected_lines
    public void process(BatchTracker batch, Iterable<String> input, JobInfo jobInfo) {
        logger.info("* Started processing input from lines with the job {}, target table {}", jobInfo.name(), jobInfo.targetTable());
//        LineValidation(jobName=ecams_hcfa_conformant_validations, batchTracker=[batchId=CCRS_H190812121741 fileInfo=[File=CCRS-TerminalStatus-HCFA-CCRS-20190812.txt, dbId=CCRS, type=TerminalStatus, formType=HCFA, sourceSystem=CCRS, feedDateStr=20190812]], lineIndex=2, lineId=G210X12CK00000001, claimId=VA-CCN1-G210X12CK, criticalValidations=[], nonCriticalValidations=[ColumnValidation(colNames=[Rendering_Facility_NPI, RenderingProvider], message=Missing columns, severity=REQUIRED)])
        final List<String> colNames = jobInfo.mappings().stream().map(Mapping::sourceCol).toList();

        final Map<String, Integer> headers = SourceDataUtils.inferHeaders(colNames.toString().replaceAll("[\\[\\] ]", "").replaceAll(",", "^"));

        final SourceDataValidator validator = new SourceDataValidator(batch, jobInfo, headers, null);
        final JobStats stats = new JobStats(jobInfo);
        final FirstBy firstBy = new FirstBy(jobInfo.dedupeCols(), headers);
        try (Connection conn = obtainConnection(); StatementInfo stmt = prepStatement(conn, jobInfo)) {
            conn.setAutoCommit(false);
            int inputLineCount = 0;
            int totalInsertedCount = 0;
            int currentChunkSize = 0;
            for (String line : input) {
                if (doInsert(batch, jobInfo, validator, stats, headers, firstBy, stmt, line, ++inputLineCount, new HashMap<>())) {
                    totalInsertedCount++;
                    currentChunkSize++;
                }
                if (currentChunkSize == commitChunkSize) {
                    stmt.preparedStmt().executeBatch();
                    conn.commit();
                    logger.info("Committed after inserting {} records of {}", totalInsertedCount, inputLineCount);
                    currentChunkSize = 0;
                }
            }
            if (currentChunkSize > 0) {
                stmt.preparedStmt().executeBatch();
                conn.commit();
                logger.info("Committed after inserting {} records of {}", totalInsertedCount, inputLineCount);
            }

            logger.info("Completed job {}, batch '{}': {}", jobInfo.name(), batch.batchId(), stats);
        } catch (SQLException e) {
            throw new DbUpdateException(e);
        }
    }


    public JobStats process(BatchTracker batch, File inputFile, String jobConfigName) {
        JobInfo jobInfo = Config.jobInfoFromConfig(jobConfigName, batch);
        return process(batch, inputFile, jobInfo);
    }

    private JobStats process(BatchTracker batch, File inputFile, JobInfo jobInfo) {

        logger.info("* Started processing batch {}, file {}, job {}, target table {}", batch.batchId(), inputFile.getName(), jobInfo.name(), jobInfo.targetTable());
        JobStats stats = new JobStats(jobInfo);
        StopWatch sw = StopWatch.createStarted();

        int jobCommitChunkSize = jobInfo.commitChunkSize();
        if (jobCommitChunkSize <= 0) {
            jobCommitChunkSize = commitChunkSize;
        }

        try (BufferedReader sourceReader = Files.newBufferedReader(inputFile.toPath())) {

            // Headers should go to the level above, we can store them in batch
            String headerLine = sourceReader.readLine();
            headerLine = prependCommonHeaders(headerLine);
            Map<String, Integer> headers = SourceDataUtils.inferHeaders(headerLine, delimiter);
            SourceDataValidator validator = new SourceDataValidator(batch, jobInfo, headers, null);

            try (Connection conn = obtainConnection();
                 StatementInfo stmt = prepStatement(conn, jobInfo)) {

                conn.setAutoCommit(false);

                String inputLine;

                int inputLineCount = 0;
                int totalInsertedCount = 0;
                int currentChunkSize = 0;

                FirstBy firstBy = new FirstBy(jobInfo.dedupeCols(), headers);

                HashMap<String, String> carcLookup = new HashMap<>();
                try (var st = conn.createStatement()) {
                    @SuppressWarnings("SqlResolve")
                    ResultSet rs = st.executeQuery("select *  from lkup_carc");
                    while (rs.next()) {
                        carcLookup.put(rs.getString(1), rs.getString(2));
                    }

                }
                while ((inputLine = sourceReader.readLine()) != null) {

                    inputLine = prependCommonFields(batch, headers, inputLine);
                    ++inputLineCount;

                    boolean toInsert = doInsert(batch, jobInfo, validator, stats, headers, firstBy, stmt, inputLine, inputLineCount, carcLookup);
                    if (toInsert) {
                        ++currentChunkSize;
                        ++totalInsertedCount;
                    }

                    if (currentChunkSize == jobCommitChunkSize) {
                        stmt.preparedStmt().executeBatch();
                        conn.commit();
                        logger.info("Committed after inserting {} records of {}", totalInsertedCount, inputLineCount);
                        currentChunkSize = 0;
                    }

                    if (inputLineCount > 0 && inputLineCount % loggingChunkSize == 0) {
                        logger.info("Read {} records; inserted {}", inputLineCount, totalInsertedCount);
                    }
                }

                if (currentChunkSize > 0) {
                    stmt.preparedStmt().executeBatch();
                    conn.commit();
                    logger.info("Committed after inserting {} records of {}", totalInsertedCount, inputLineCount);
                }

                stats.inserted(totalInsertedCount);
                stats.inputLines(inputLineCount);

                if (validator.isFailed()) {
                    logger.warn("Encountered the following validation failures:\n{}", validator.genReport(100));
                }
                sw.stop();
                stats.elapsedTime = sw.getTime();

                logger.info("Completed job {}, batch '{}': {}", jobInfo.name(), batch.batchId(), stats);
            }

        } catch (SQLException sqlException) {
            throw new DbUpdateException(sqlException);
        } catch (IOException e) {
            throw new UnrecoverableException(e);
        }

        return stats;
    }

    private boolean doInsert(BatchTracker batch, JobInfo jobInfo, SourceDataValidator validator, JobStats stats,
                             Map<String, Integer> headers, FirstBy firstBy,
                             StatementInfo stmt,
                             String line, int lineIndex, HashMap<String, String> carcLookup) throws SQLException {

        boolean wasInserted = false;
        List<Mapping> mappings = jobInfo.mappings();
        List<String> sourceValues = valuesFromLine(line, delimiter);

        LineValidation lineValidation = validator.validate(sourceValues, lineIndex);


        if (lineValidation.isCritical()) {
            logger.warn("Line {} failed validation:\n{}", lineIndex, lineValidation);
            stats.incrFailedValidation();
            return false;
        }

        if (firstBy.checkForDupe(sourceValues)) {
            logger.debug("Found duplicate based on the /columns {}, skip inserting", jobInfo.dedupeCols());
            stats.incrDroppedByFirstBy();
            return false;
        }

        boolean isSourcePopulated = false;
        Derivator derivator = new Derivator(batch, headers, sourceValues, carcLookup);
        derivator.createDerivations(mappings);

//        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
//        // for debugging only for now
//        TreeMap<String, Object> insertedVals = new TreeMap<>();

        for (Mapping map : mappings) {
            String val;
            if (map.isDerived()) {
                val = derivator.getDerivedVal(map.sourceCol());
            }
            else {
                val = SourceDataUtils.valByHeader(headers, map.sourceCol(), sourceValues);
            }
//            insertedVals.put(map.sourceCol(), val);

            if (!isSourcePopulated && !StringUtils.isEmpty(val) && map.isSourceField()) {
                isSourcePopulated = true;
            }

            val = truncateIfNeeded(batch.fileInfo().tableMeta(), map, val);
            setValue(stmt, map, val);
        }

        if (isSourcePopulated || jobInfo.isInsertEmpty()) {
            // To debug trimming issues
//            logger.info(insertedVals.toString());
            stmt.preparedStmt().addBatch();
            wasInserted = true;
        }
        else {
            logger.debug("Line {} was not inserted because all data elements for this job were blank", lineIndex);
            stats.incrNoData();
        }

        return wasInserted;
    }

    private String truncateIfNeeded(TableMeta tableMeta, Mapping map, String val) {
        String newVal = val;

        if (StringUtils.isEmpty(val)) {
            return val;
        }
        String col = map.targetCol();

        int len = tableMeta.getColLen(col);
        if (len > 0 && len < val.length()) {
            logger.warn("Truncating  column {} value '{}' max length {}", col, val, len);
            newVal = StringUtils.truncate(val, len);
        }
        return newVal;
    }


    private String prependCommonFields(BatchTracker batchEntry, Map<String, Integer> headers, String line) {
        final String source = batchEntry.getSource(headers, line);
        String commonFields = StringUtils.joinWith(delimiter, batchEntry.fileInfo().databaseId(), batchEntry.batchId()
                , batchEntry.fileInfo().sourceSystem(), batchEntry.fileInfo().normalizedFormType(), source == null ? "" : source);
        return commonFields + delimiter + line;
    }

    private String prependCommonHeaders(String line) {
        String commonFields = StringUtils.joinWith(delimiter, "db_id", "etl_batch_id", "source_system", "source_entity", "ssn_match_source");
        return commonFields + delimiter + line;
    }


    private void setValue(StatementInfo stmtInfo, Mapping mapping, String rawVal) throws SQLException {
        int placeholderIndex = stmtInfo.getPlaceholderIndex(mapping.targetCol());
        rawVal = normalizeString(rawVal);

        Object val = convertVal(mapping, rawVal);
        stmtInfo.preparedStmt().setObject(placeholderIndex + 1, val);
    }

    private boolean removeQuotes = false;
    private boolean trim = false;

    private String normalizeString(String val) {
        String cleanedVal = val;
        if (removeQuotes) {
            cleanedVal = StringUtils.unwrap(cleanedVal, "\"");
        }
        if (trim) {
            cleanedVal = StringUtils.trim(cleanedVal);
        }
        if (StringUtils.isEmpty(cleanedVal)) {
            cleanedVal = null;
        }
        return cleanedVal;
    }

    private Object convertVal(Mapping mapping, String rawVal) {
        Object objVal = rawVal;
        if (rawVal == null) {
            return objVal;
        }

        if (StringUtils.endsWithAny(mapping.targetCol(), "_key", "_order")) {
            objVal = Long.parseLong(rawVal);
        }
        else if (mapping.targetCol().endsWith("_date")) {
            objVal = DateUtils.fromYMDNoDelim(rawVal);
        }
        return objVal;
    }

    private Connection obtainConnection() throws SQLException {
        return DataSourceFactory.dataSource().getConnection();
    }

    private StatementInfo prepStatement(Connection conn, JobInfo jobInfo) throws SQLException {

        InsertUpdateBuilder sqlBuilder = new InsertUpdateBuilder(jobInfo.targetTable());
        sqlBuilder.targetsFromMappings(jobInfo.mappings());
        sqlBuilder.addAuditFields();

        StatementInfo stmt = sqlBuilder.buildStatement();
        logger.info("Prepared insert:\n{}", stmt.sql());
        stmt.prepare(conn);
        return stmt;
    }

}