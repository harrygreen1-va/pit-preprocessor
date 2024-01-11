package pit.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import pit.etl.batchlog.BatchTracker;

import java.util.Collection;

/**
 * Methods to identifying batches based on naming convention
 */
@UtilityClass
public class BatchIdUtils {
    public final static String CCRS_SOURCE_SYSTEM = "CCRS";
    public Collection<BatchTracker> getCCRSBatches(Collection<BatchTracker> batches) {
        return batches.stream().filter(batch->isCCRS(batch.batchId())).toList();
    }

    public Collection<String> getCCRSBatchIds(Collection<String> batchIds) {
        return batchIds.stream().filter(BatchIdUtils::isCCRS).toList();
    }

    public boolean isCCRS(String batchId) {
        return StringUtils.startsWith(batchId, CCRS_SOURCE_SYSTEM);
    }
}