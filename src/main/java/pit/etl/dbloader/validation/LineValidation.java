package pit.etl.dbloader.validation;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import pit.etl.batchlog.BatchTracker;

import java.util.ArrayList;
import java.util.List;

import static pit.etl.dbloader.validation.Severity.CRITICAL;
import static pit.etl.dbloader.validation.SourceDataValidator.DUPLICATE_IDS;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class LineValidation {
    private String jobName;
    private BatchTracker batchTracker;
    private int lineIndex;
    private final String claim_key;
    private final String claim_detail_key;

    private String lineId;
    private String claimId;

    private List<ColumnValidation> criticalValidations = new ArrayList<>();
    private List<ColumnValidation> nonCriticalValidations = new ArrayList<>();

    private final static String VALIDATION_FIELDS_DELIM=": ";

    LineValidation(BatchTracker batchTracker, String jobName, int lineIndex, String claim_key, String claim_detail_key) {
        this.batchTracker = batchTracker;
        this.jobName = jobName;
        this.lineIndex = lineIndex;
        this.claim_key = claim_key;
        this.claim_detail_key = claim_detail_key;
    }

    void addValidation(ColumnValidation vld) {
        if (vld.severity() == CRITICAL) {
            criticalValidations.add(vld);
        }
        else {
            nonCriticalValidations.add(vld);
        }
    }

    public boolean isCritical() {
        return !(criticalValidations.isEmpty());
    }

    @SuppressWarnings("unused")
    public boolean isWarning() {
        return !(nonCriticalValidations.isEmpty());
    }

    boolean isWarningOnly() {
        return criticalValidations.isEmpty() && !nonCriticalValidations.isEmpty();
    }

    public boolean isSucceeded() {
        return !isCritical();
    }

    boolean isEmpty() {
        return criticalValidations.isEmpty() && nonCriticalValidations.isEmpty();
    }

    public boolean isDupe() {
        for (var validation : criticalValidations) {
            // TODO: replace message with enum
            if (DUPLICATE_IDS.equals(validation.message())) {
                return true;
            }
        }
        return false;
    }

    String toStringForReport() {
        StringBuilder report = new StringBuilder();
        for (ColumnValidation validation : criticalValidations) {
            report.append(toFriendlyString("Critical", validation));
        }

        for (ColumnValidation validation : nonCriticalValidations) {
            report.append(toFriendlyString("Warning", validation));
        }
        return report.toString();
    }

    private String toFriendlyString(String severity, ColumnValidation vld) {
        StringBuilder report=new StringBuilder();
        report.append(lineIdToFriendlyString());
        report.append(severity).append(VALIDATION_FIELDS_DELIM);

        report.append(vld.message()).append(VALIDATION_FIELDS_DELIM).append(vld.colNames());

        if (batchTracker.fileInfo().databaseId().equals("CCRS")) {
            report.append(": ").append(claim_key).append(": ").append(claim_detail_key);
        }
        report.append("\n");
        return report.toString();

    }

    private String lineIdToFriendlyString() {
        String idStr = StringUtils.joinWith(VALIDATION_FIELDS_DELIM, lineIndex, StringUtils.stripToEmpty(claimId), StringUtils.stripToEmpty(lineId));
        return idStr + VALIDATION_FIELDS_DELIM;
    }


}