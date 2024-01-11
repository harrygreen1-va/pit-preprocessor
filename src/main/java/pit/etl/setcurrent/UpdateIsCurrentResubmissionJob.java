package pit.etl.setcurrent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.util.BatchIdUtils;
import pit.util.JdbcUtils;

import java.util.Collection;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class UpdateIsCurrentResubmissionJob {

    private final Config config;
    private final UpdateIsCurrentDao updateIsCurrentDao = new UpdateIsCurrentDao();


    public void process(Collection<BatchTracker> batches) {
        var batchIds = batches.stream().map(BatchTracker::batchId).toList();
        log.info("* Starting is_current update for resubmissions. Batches: {}", batchIds);

        var isCurrentRecords = fetchPreviousSubmissions(batchIds);
        if (!isCurrentRecords.isEmpty()) {
            var isCurrentUpdates = isCurrentRecords.stream().map(isCurrentRecord -> new IsCurrentUpdate(isCurrentRecord, false)).toList();
            var isCurrentBatchContainer = IsCurrentBatchesContainer.fromIsCurrentRecords(isCurrentRecords);
            log.info("Found {} claims that have been resubmitted. They will be set to is_current=N. Batches containing resubmitted claims: {}", isCurrentUpdates.size(), isCurrentBatchContainer.batchIds());

            updateIsCurrentDao.populateIsCurrentLogAndPerformUpdate(isCurrentUpdates, isCurrentBatchContainer, true, config.isDryRun());
        }
        else {
            log.info("Found no current claims that have been resubmitted");
        }
    }

    private final static String SELECT_CLAIMS = """
            select distinct claim.claim_key, claim.source_claim_PK, 'Y', claim.etl_batch_id as updated_etl_batch_id, next_claim.claim_key as next_claim_key, next_claim.etl_batch_id as updating_etl_batch_id
            from dim_va_claim claim
            join dim_va_claim next_claim on claim.source_claim_pk=CAST(next_claim.source_system_prior_claim_key AS varchar)
            and claim.source_entity=next_claim.source_entity
            where claim.is_current='Y'
            and claim.source_system='%s' and next_claim.source_system='%s'
            and next_claim.claim_key in
            (select top 1 claim_key from dim_va_claim where next_claim.source_system_prior_claim_key=source_system_prior_claim_key order by source_claim_PK desc, claim_key desc)
            and next_claim.etl_batch_id in ( %s )
            order by claim.claim_key desc
                        """;


    private List<IsCurrentRecord> fetchPreviousSubmissions(Collection<String> batchIds) {
        var sql = String.format(SELECT_CLAIMS, BatchIdUtils.CCRS_SOURCE_SYSTEM, BatchIdUtils.CCRS_SOURCE_SYSTEM, JdbcUtils.genListForIn(batchIds));
        return updateIsCurrentDao.fetchClaimsToProcess(sql, true);
    }
}