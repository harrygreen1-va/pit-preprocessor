package pit.etl.dbloader;

import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.batchlog.Eci;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;

import java.io.File;
import java.time.LocalDateTime;

public class BatchTrackerManager {
    private BatchLogDao batchLogDao = new BatchLogDao();

    public BatchTracker createBatchTracker(File file, Eci eci, LocalDateTime startTimestamp) {
        DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(file);
        BatchTracker batch = new BatchTracker(fileInfo);
        batch.setEci(eci);
        batch.setStartTimestamp(startTimestamp);
        batchLogDao.insertFileBatchEntry(batch);

        return batch;
    }

    public BatchTracker findFirstOrCreate(File file) {
        BatchTracker tracker = batchLogDao.selectFirstBatchInfoForFileName(file.getName());
        if (tracker == null) {
            tracker = createBatchTracker(file, null, LocalDateTime.now());
        }
        return tracker;
    }

    public BatchTracker findFirst(String fileName) {
        return batchLogDao.selectFirstBatchInfoForFileName(fileName);
    }

}