package pit.etl.setcurrent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.util.JdbcUtils;

import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class UpdateIsCurrentJob {

    private final Config config;
    private final UpdateIsCurrentDao updateIsCurrentDao = new UpdateIsCurrentDao();

    public void process(Collection<BatchTracker> batches) {
        var batchIds = batches.stream().map(BatchTracker::batchId).toList();
        log.info("* Starting is_current update for batches {}", batchIds);

        var isCurrentBatchContainer = IsCurrentBatchesContainer.fromBatchIds(batchIds);

        var isCurrentRecords = fetchClaimsToProcess(isCurrentBatchContainer);
        if (!isCurrentRecords.isEmpty()) {

            var isCurrentUpdates = determineIsCurrentUpdates(isCurrentRecords);
            if (!isCurrentUpdates.isEmpty()) {

                updateIsCurrentDao.populateIsCurrentLogAndPerformUpdate(isCurrentUpdates, isCurrentBatchContainer, false, config.isDryRun());
            }
            else {
                log.info("No is_current updates for {}", batchIds);
            }
            log.info("Completed is_current update for batch {}", batchIds);
        }

        else {
            log.warn("Found no claims in the batch {}", batchIds);
        }
    }

    private final static String SELECT_CLAIMS_CLAIM_ID_STMT = """
            select claim.claim_key, claim.claim_id, claim.is_current, claim.etl_batch_id, claim_batch.etl_batch_id, null
            from dim_va_claim claim
            join dim_va_claim claim_batch on claim_batch.claim_id=claim.claim_id and claim_batch.db_id=claim.db_id and claim.source_entity=claim_batch.source_entity
            join claim_batch_log cbl on cbl.etl_batch_id=claim.etl_batch_id
            where claim_batch.etl_batch_id in (%s)
            order by claim.claim_id, claim.created_date desc, claim.source_claim_pk desc, cbl.feed_date desc, claim.claim_key desc
            """;

    private final static String SELECT_CLAIMS_CLAIM_PK_STMT = """
            select claim.claim_key, claim.source_claim_pk, claim.is_current, claim.etl_batch_id, claim_batch.etl_batch_id, ic_log.is_current as is_current_log_is_current
            from dim_va_claim claim
            join dim_va_claim claim_batch on claim_batch.source_claim_PK=claim.source_claim_PK and claim_batch.source_system=claim.source_system and claim_batch.source_entity=claim.source_entity
            join claim_batch_log cbl on cbl.etl_batch_id=claim.etl_batch_id
            left join etl.is_current_log ic_log on ic_log.claim_id=claim.source_claim_PK and ic_log.next_submission_claim_key is not null
            and ic_log.is_current_log_key in
                (select max(is_current_log_key) from etl.is_current_log where claim_id=claim.source_claim_PK )
            where claim_batch.etl_batch_id in (%s)
            order by claim.source_claim_PK, claim.created_date desc,
                (case when claim.status='paid' then 10 when claim.status='approved' then 0 else 5 end) desc,
                cbl.feed_date desc, claim_key desc
                        """;

    private List<IsCurrentRecord> fetchClaimsToProcess(IsCurrentBatchesContainer batchContainer) {
        List<IsCurrentRecord> isCurrentRecords = new ArrayList<>();

        var ccrsBatches = batchContainer.ccrsBatchIds();
        if (!ccrsBatches.isEmpty()) {
            var sql = String.format(SELECT_CLAIMS_CLAIM_PK_STMT, JdbcUtils.genListForIn(ccrsBatches));
            isCurrentRecords = updateIsCurrentDao.fetchClaimsToProcess(sql, false);
        }

        var nonCcrsBatches = batchContainer.nonCCRSBatchIds();
        if (!nonCcrsBatches.isEmpty()) {
            if (!ccrsBatches.isEmpty()) {
                log.warn("The batch IDs submitted to IsCurrent job contain both CCRS and non-CCRS batches");
            }
            var sql = String.format(SELECT_CLAIMS_CLAIM_ID_STMT, JdbcUtils.genListForIn(nonCcrsBatches));
            isCurrentRecords.addAll(updateIsCurrentDao.fetchClaimsToProcess(sql, false));
        }

        return isCurrentRecords;
    }


    private List<IsCurrentUpdate> determineIsCurrentUpdates(List<IsCurrentRecord> isCurrentRecords) {
        List<IsCurrentUpdate> isCurrentUpdates = new ArrayList<>();
        String curClaimId = null;
        Set<Long> processedKeys = new HashSet<>();
        for (var isCurrentRecord : isCurrentRecords) {
            if (!isCurrentRecord.claimId().equals(curClaimId)) {
                curClaimId = isCurrentRecord.claimId();
                processedKeys.clear();
                // first claim, set to current
                if (!isCurrentRecord.isCurrent() && !isCurrentRecord.isNonCurrentInLog()) {
                    isCurrentUpdates.add(new IsCurrentUpdate(isCurrentRecord, true));
                }
            }
            else if (isCurrentRecord.isCurrent() && !processedKeys.contains(isCurrentRecord.claimKey())) {
                isCurrentUpdates.add(new IsCurrentUpdate(isCurrentRecord, false));
            }

            processedKeys.add(isCurrentRecord.claimKey());
        }
        log.info("Identified {} is_current updates", isCurrentUpdates.size());

        return isCurrentUpdates;
    }

}