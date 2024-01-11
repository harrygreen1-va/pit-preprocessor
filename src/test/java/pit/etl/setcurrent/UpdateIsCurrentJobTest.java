package pit.etl.setcurrent;

import org.junit.Before;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.postprocessor.PostprocessorMain;

import java.time.LocalDate;
import java.util.List;

public class UpdateIsCurrentJobTest implements FileEtlTest {
    Config config = Config.load();

    @Before
    public void init() {
        DataSourceFactory.init(config);
    }

    @Test
    public void runSetIsCurrent() {
        var testBatchId = "CXM_8I_21";
        var job = new UpdateIsCurrentJob(config);
        var batch = new BatchTracker(null, testBatchId);
        job.process(List.of(batch));

    }


    @Test
    public void runSetIsCurrentForResubmissionsFromCli() {
        runPostprocessor("-" + PostprocessorMain.IS_CURRENT_MODE,
                "-" + PostprocessorMain.IS_CURRENT_FROM_DATE, "2022-01-01",
                "-" + PostprocessorMain.IS_CURRENT_TO_DATE, LocalDate.now().toString()
//                "-" + PostprocessorMain.IS_RESUBMISSION_ONLY
                //"-" + PostprocessorMain.IS_CURRENT_SOURCE_SYSTEM, "CCRS"
        );
    }

}