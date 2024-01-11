package pit.etl.setcurrent;

import lombok.Getter;
import pit.etl.batchlog.ClaimType;
import pit.util.BatchIdUtils;

import java.util.*;

@Getter
public class IsCurrentBatchesContainer {

    private final Set<String> batchIds = new LinkedHashSet<>();
    private final Map<ClaimType, Set<String>> batchIdsByClaimType = new EnumMap<>(ClaimType.class);

    public static IsCurrentBatchesContainer fromIsCurrentRecords(Collection<IsCurrentRecord> isCurrentRecords) {
        var container = new IsCurrentBatchesContainer();
        for (var isCurrentRecord : isCurrentRecords) {
            container.batchIds.add(isCurrentRecord.updatingBatchId());
        }

        container.populateBatchIdByClaimType(container.batchIds);
        return container;
    }

    public static IsCurrentBatchesContainer fromBatchIds(Collection<String> batchIds) {
        var container = new IsCurrentBatchesContainer();
        container.batchIds.addAll(batchIds);

        container.populateBatchIdByClaimType(container.batchIds);
        return container;
    }

    public Collection<String> ccrsBatchIds() {
        return batchIds.stream().filter(BatchIdUtils::isCCRS).toList();
    }

    public Collection<String> nonCCRSBatchIds() {
        return batchIds.stream().filter(batchId -> !BatchIdUtils.isCCRS(batchId)).toList();
    }

    private void populateBatchIdByClaimType(Collection<String> batchIds) {
        for (var batchId : batchIds) {
            var claimType = ClaimType.fromBatchId(batchId);
            var batchIdsForClaimType = batchIdsByClaimType.computeIfAbsent(claimType, k -> new LinkedHashSet<>());
            batchIdsForClaimType.add(batchId);
        }
    }


}