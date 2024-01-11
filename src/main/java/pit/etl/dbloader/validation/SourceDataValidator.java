package pit.etl.dbloader.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.dbloader.DbFileLoader;
import pit.etl.dbloader.Derivator;
import pit.etl.dbloader.FirstBy;
import pit.etl.dbloader.JobInfo;
import pit.etl.filecopytransform.FilesUtils;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.filecopytransform.fbcs.SourceLine;
import pit.util.SourceDataUtils;


import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static pit.util.SourceDataUtils.valuesFromLine;

public class SourceDataValidator {

    @SuppressWarnings("WeakerAccess")
    public final static String VALIDATION_LOGGER_NAME = "file-validation";
    public static final String DUPLICATE_IDS = "Duplicate IDs";
    public static final String MISSING_COLUMNS = "Missing columns";
    public static final String INVALID_STATUS = "Invalid Status";
    public static final String INVALID_DATE = "Invalid Date";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public final static Logger validationLogger = LoggerFactory.getLogger(VALIDATION_LOGGER_NAME);

    private final List<LineValidation> validations = new ArrayList<>();
    private final BatchTracker batch;
    private final JobInfo validationInfo;
    private final FirstBy firstBy;
    private final Map<String, Integer> headers;

    private ProcessingStats vldStats = new ProcessingStats();

    public SourceDataValidator(BatchTracker batch, JobInfo validationInfo, Map<String, Integer> headers, ProcessingStats stats) {
        this.batch = batch;
        this.validationInfo = validationInfo;
        this.headers = headers;
        firstBy = new FirstBy(validationInfo.dupeCols(), headers);
        if (stats != null) {
            vldStats = stats;
        }
    }

    public Set<String> required() {
        return validationInfo.requiredCols();
    }

    public List<LineValidation> validations() {
        return validations;
    }

    public ProcessingStats stats() {
        return vldStats;
    }

    private List<LineValidation> getCriticalValidations() {
        return validations.stream().filter(LineValidation::isCritical).collect(Collectors.toList());
    }

    private List<LineValidation> getWarningOnlyValidations() {
        return validations.stream().filter(LineValidation::isWarningOnly).collect(Collectors.toList());
    }

    public int criticalCount() {
        return (int) validations.stream().filter(LineValidation::isCritical).count();
    }

    public LineValidation validate(String line, int lineIndex) {
        List<String> sourceValues = valuesFromLine(line);
        return validate(sourceValues, lineIndex);
    }

    public LineValidation validate(List<String> sourceValues, int lineIndex) {
        LineValidation vld = validateValues(sourceValues, lineIndex);
        if (vld.isCritical() || vld.isWarning()) {
            validations.add(vld);
        }
        return vld;
    }

    private LineValidation createLineValidation(List<String> vals, int lineIndex) {
        String claim_key = null;
        if (headers.containsKey("claim_key"))
            claim_key = SourceDataUtils.valByHeader(headers, "claim_key", vals);

        String claim_detail_key = null;
        if (headers.containsKey("claim_detail_key"))
            claim_detail_key = SourceDataUtils.valByHeader(headers, "claim_detail_key", vals);

        return new LineValidation(batch, validationInfo.name(), lineIndex, claim_key, claim_detail_key);
    }

    private LineValidation validateValues(List<String> vals, int lineIndex) {

        LineValidation lineValidation = createLineValidation(vals, lineIndex);

        Set<String> missingCols = validateValues(TypeValidators::isNotEmpty, headers, validationInfo.requiredCols(), vals);
        if (!missingCols.isEmpty()) {
            lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, MISSING_COLUMNS, missingCols));
            vldStats.incr("missing");
            vldStats.incr("critical");
        }

        missingCols = validateValues(TypeValidators::isNotEmpty, headers, validationInfo.requiredNonCritCols(), vals);
        if (!missingCols.isEmpty()) {
            lineValidation.addValidation(new ColumnValidation(Severity.REQUIRED, MISSING_COLUMNS, missingCols));
            vldStats.incr("missing-warn");
            vldStats.incr("warn");
        }

        Set<String> invalidDateCols = validateValues(TypeValidators::isValidDate, headers, validationInfo.dateCols(), vals);
        if (!invalidDateCols.isEmpty()) {
            lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, INVALID_DATE, invalidDateCols));
            vldStats.incr("missing");
            vldStats.incr("critical");
        }

        if ("TerminalStatus".equals(batch.fileInfo().type()) && validationInfo.insert() != null) {
            Set<String> invalidStatuses = validateValues(TypeValidators::isStatusValid, headers, Collections.singleton(validationInfo.insert()), vals);
            if (!invalidStatuses.isEmpty()) {
                lineValidation.addValidation(new ColumnValidation(Severity.WARNING, INVALID_STATUS, invalidStatuses));
                vldStats.incr("invalid-status");
                vldStats.incr("warn");
            }
        }

        if ("TerminalStatus".equals(batch.fileInfo().type()) &&
                batch.fileInfo().isCCRS() &&
                validationInfo.checkValuesOfCols() != null) {
            Set<String> invalidStatuses = validateValues(TypeValidators::isFieldValid, headers, validationInfo.checkValuesOfCols(), vals);
            if (!invalidStatuses.isEmpty()) {
                lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, INVALID_STATUS, invalidStatuses));
                vldStats.incr("invalid-status");
                vldStats.incr("critical");
            }

            // validate Box1A (SSN) and ICN
            if (validationInfo.ssnChooseICN != null) {
                if (validationInfo.ssnChooseICN.size() != 2) {
                    logger.debug(">>>>>>>>validationInfo.ssnChooseICN.size() is not 2");
                    //throw new UnrecoverableException(">>>>>>>>validationInfo.ssnChooseICN.size() is zero");
                }
                invalidStatuses = validateValuesChoose(TypeValidators::isNotEmpty, headers, validationInfo.ssnChooseICN, vals);
                if (!invalidStatuses.isEmpty()) {
                    lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, INVALID_STATUS, invalidStatuses));
                    vldStats.incr("invalid-status");
                    vldStats.incr("critical");
                }
            }

            if (validationInfo.ssn1ChooseICN != null) {
                if (validationInfo.ssn1ChooseICN.size() != 2) {
                    logger.debug(">>>>>>>>validationInfo.ssn1ChooseICN.size() is not 2");
                    //throw new UnrecoverableException(">>>>>>>>validationInfo.ssnChooseICN.size() is zero");
                }
                invalidStatuses = validateValuesChoose(TypeValidators::isNotEmpty, headers, validationInfo.ssn1ChooseICN, vals);
                if (!invalidStatuses.isEmpty()) {
                    lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, INVALID_STATUS, invalidStatuses));
                    vldStats.incr("invalid-status");
                    vldStats.incr("critical");
                }
            }
        }

        if (firstBy.checkForDupe(vals)) {
            validationLogger.warn("{}:{}: dupe based on the following columns: {}", batch.fileInfo().file().getName(), lineIndex, firstBy.dedupeCols());
            lineValidation.addValidation(new ColumnValidation(Severity.WARNING, DUPLICATE_IDS, firstBy.dedupeCols()));
            vldStats.incr("dupes");
            vldStats.incr("warn");
        }

        if (!lineValidation.isEmpty()) {
            lineValidation.lineId(Derivator.getClaimLineId(batch, headers, vals));
            lineValidation.claimId(Derivator.getClaimId(batch, headers, vals));
        }

        return lineValidation;
    }

    public void addDupeFailure(Set<String> dedupeCols, SourceLine sourceLine) {
        List<String> vals = valuesFromLine(sourceLine.line());
        LineValidation lineValidation = createLineValidation(vals, sourceLine.lineNumber());
        validationLogger.warn("{}:{}: dupe based on the following columns: {}", batch.fileInfo().file().getName(), sourceLine.lineNumber(), dedupeCols);
        lineValidation.addValidation(new ColumnValidation(Severity.CRITICAL, DUPLICATE_IDS, dedupeCols));
        lineValidation.lineId(sourceLine.lineIdOrKey());
        lineValidation.claimId(sourceLine.claimIdOrKey());
        vldStats.incr("dupes");
        vldStats.incr("critical");
        validations.add(lineValidation);
    }

    private Set<String> validateValues(ValueValidator validator, Map<String, Integer> headers, Set<String> cols, List<String> rawFields) {
        Set<String> failed = new HashSet<>();
        for (String col : cols) {
            String choices = "";
            if (col.contains("[")) {
                choices = col.substring(col.indexOf("[") + 1, col.indexOf("]"));
                col = col.substring(0, col.indexOf("["));
            }
            String val = SourceDataUtils.valByHeader(headers, col, rawFields);

            if (!validator.isValid((choices.isEmpty() ? col : choices), val)) {
                failed.add(col);
            }
        }
        return failed;
    }

    /***
     * box1A_ICN.cols=Box1A,ICN
     * if both ssn (box1A) and ICN are null then report
     * @param validator
     * @param headers
     * @param cols
     * @param rawFields
     * @return
     */
    private Set<String> validateValuesChoose(ValueValidator validator, Map<String, Integer> headers, ArrayList<String> cols, List<String> rawFields) {
        Set<String> failed = new HashSet<>();
        if (cols.size() != 2) {
            return failed;
        }
        String[] columns = cols.toArray(new String[cols.size()]);
        String column1 = columns[0];
        String column2 = columns[1];
        String val1 = SourceDataUtils.valByHeader(headers, column1, rawFields);
        String val2 = SourceDataUtils.valByHeader(headers, column2, rawFields);

        logger.debug("validateValuesChoose() column1=" + column1 + ":: column2=" + column2 + ":: val1=" +  val1 + ":: val2=" + val2 + "<<");
        if (!validator.isValid((column1), val1) && !validator.isValid((column2), val2)) {
            failed.add(column1);
            failed.add(column2);
        }
        return failed;
    }



    public boolean isFailed() {
        return !validations.isEmpty();
    }

    public String genReport(int numberOfValidations) {
        StringBuilder buf = new StringBuilder();
        if (numberOfValidations > validations.size()) {
            numberOfValidations = validations.size();
        }

        List<LineValidation> vldsToReport = new ArrayList<>(getCriticalValidations());
        vldsToReport.addAll(getWarningOnlyValidations());
        vldsToReport = validations.subList(0, numberOfValidations);

        for (LineValidation vld : vldsToReport) {
            buf.append(vld.toStringForReport());
        }

        int remainder = validations.size() - numberOfValidations;
        if (remainder > 0) {
            buf.append(remainder).append(" more ...").append("\n");
        }

        return buf.toString();
    }

    public void saveValidationsToDb(Config config) {
        final DbFileLoader loader = new DbFileLoader(config);
        final JobInfo rejected_lines = Config.jobInfoFromConfig("rejected_lines", batch);
        List<String> list = new ArrayList<>();
        for (LineValidation validation : validations) {
            boolean loaded_to_db = true;
            for (ColumnValidation critical : validation.criticalValidations()) {
                if (!DUPLICATE_IDS.equals(critical.message())) {
                    for (String colName : critical.colNames()) {
                        list.add(join(validation, critical, colName, loaded_to_db = false));
                    }
                } else {
                    list.add(join(validation, critical, critical.colNames().toString().replaceAll("[\\[\\] ]", ""), loaded_to_db = false));
                }
            }
            for (ColumnValidation nonCritical : validation.nonCriticalValidations()) {
                for (String colName : nonCritical.colNames()) {
                    list.add(join(validation, nonCritical, colName, loaded_to_db));
                }
            }
        }
        if (Config.isDataLoadingEnabled())
            loader.process(batch, list, rejected_lines);
    }

    private String join(LineValidation line, ColumnValidation column, String colName, boolean loaded_to_db) {
        return String.join(DelimitedFileInfo.DELIM,
                column.severity().toString(),
                line.lineId(),
                column.message(),
                batch.fileInfo().databaseId(),
                batch.fileInfo().sourceSystem(),
                line.claimId(),
                loaded_to_db ? "Y" : "N",
                String.valueOf(line.lineIndex()),
                batch.batchId(),
                colName,
                batch.fileInfo().formType()
        );
    }

    // TODO: generate the file in the agreed upon format
    public void saveValidationsToFile() {
        File rejectFile = batch.fileInfo().getRejectedFileName();
        String report = genReport(validations.size());
        FilesUtils.saveFile(rejectFile, report);
        logger.info("Saved rejects to {}", rejectFile.getAbsolutePath());

    }

}