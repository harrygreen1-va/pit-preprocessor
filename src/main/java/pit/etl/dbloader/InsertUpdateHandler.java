package pit.etl.dbloader;

import pit.etl.batchlog.BatchTracker;

import java.util.List;
import java.util.Map;

public interface InsertUpdateHandler {
    void beforeFile(BatchTracker batch, Map<String, Integer> headers );
    // TODO: return derived values
    void beforeLine(Map<String, Integer> headers, List<String> sourceValues);
    void onCommit();
}
