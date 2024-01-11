package pit.etl.dbloader;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;

import java.io.File;

@Accessors(fluent = true)
@Getter
@Setter
@RequiredArgsConstructor
public class ClaimLoader {
    private @NonNull Config globalConfig;

    public final static String HCFA_CLAIM_LOAD_JOB="claim/hcfa_claim_load";

    public void process(BatchTracker batchEntry, File inputFile, int commitChunksize) {
        DbFileLoader fileLoader = new DbFileLoader(globalConfig);
        fileLoader.commitChunkSize(commitChunksize);
        String insertJobName = null;
        if (batchEntry.fileInfo().isHcfa()) {
            insertJobName = HCFA_CLAIM_LOAD_JOB;
        } else {
            throw new UnrecoverableException("No job for %s", batchEntry.fileInfo());
        }

        fileLoader.process(batchEntry, inputFile, insertJobName);
    }
}
