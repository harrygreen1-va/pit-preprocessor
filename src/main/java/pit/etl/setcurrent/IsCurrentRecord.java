package pit.etl.setcurrent;

import org.apache.commons.lang3.StringUtils;

public record IsCurrentRecord(long claimKey, String claimId, String isCurrentValue, String updatedBatchId,
                              String updatingBatchId, Long nextSubmissionClaimKey, String isCurrentFromLogValue) {

    public boolean isCurrent() {
        return StringUtils.equalsIgnoreCase(isCurrentValue, "Y");
    }

    public boolean isNonCurrentInLog() {
        return StringUtils.equalsIgnoreCase(isCurrentFromLogValue, "N");
    }

    public boolean isCurrentInLog() {
        return StringUtils.equalsIgnoreCase(isCurrentFromLogValue, "Y");
    }
}