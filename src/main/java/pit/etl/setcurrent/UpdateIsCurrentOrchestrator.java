package pit.etl.setcurrent;

import lombok.extern.slf4j.Slf4j;
import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.util.BatchIdUtils;

import java.time.LocalDate;
import java.util.Collection;

@Slf4j
public class UpdateIsCurrentOrchestrator {

    private final UpdateIsCurrentResubmissionJob updateIsCurrentResubmissionJob;
    private final UpdateIsCurrentJob updateIsCurrentJob;
    private final BatchLogDao batchLogDao = new BatchLogDao();

    public UpdateIsCurrentOrchestrator(Config config) {
        updateIsCurrentResubmissionJob = new UpdateIsCurrentResubmissionJob(config);
        updateIsCurrentJob = new UpdateIsCurrentJob(config);
    }

    public void runUpdateIsCurrent(LocalDate fromDate, LocalDate toDate, String sourceSystemPrefix, boolean isResubmissionOnly) {
        log.info("* Starting is_current update with dates {} - {}, Source system: {}, isResubmissionOnly: {}", fromDate, toDate, sourceSystemPrefix, isResubmissionOnly);
        var batches = batchLogDao.selectBatchInfoForDates(fromDate, toDate, sourceSystemPrefix);
        runUpdateIsCurrent(batches, isResubmissionOnly);

    }

    public void runUpdateIsCurrent(Collection<BatchTracker> batches, boolean isOnlyResubmission) {
        if (!isOnlyResubmission) {
            updateIsCurrentJob.process(batches);
        }
        var ccrsBatches=BatchIdUtils.getCCRSBatches(batches);
        if (!ccrsBatches.isEmpty()) {
            updateIsCurrentResubmissionJob.process(ccrsBatches);
        }
    }

}