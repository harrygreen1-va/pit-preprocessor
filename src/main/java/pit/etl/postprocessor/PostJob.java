package pit.etl.postprocessor;

import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.dbloader.JobInfo;

public interface PostJob {

    void postprocess(JobInfo jobInfo, BatchTracker batch);

    default void setEnvConfig(Config config) {

    }
}