package pit.etl.perftest;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.dbloader.BatchTrackerManager;
import pit.etl.dbloader.DbFileLoader;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;

import java.io.File;

public class FileLoaderPerfTest implements FileEtlTest {

    private Config config=Config.load( FileProcessorTestUtils.getEnv() );
    private BatchTrackerManager trackerManager = new BatchTrackerManager();

//    String fileForTestBatch="CCNN-ClaimsToScore-HCFA-CCNNC-20190325.txt";
    String fileForTestBatch="CCNN-TerminalStatus-HCFA-CCNNC-20200625.txt";
    @Before
    public void init() {
        DataSourceFactory.init( config );
    }


    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void claimLineDiagLoad() {
        //var preprodFile = "claim_line_diagnosis_92k_rows.csv";
        String smallerFile = "claim_line_diagnosis_31K.csv";
        runLoader(testDir, smallerFile, "line/line_diag_load");

    }

    private void runLoader(File inDir, String fileName, String jobConfigFileName) {
        File testFile=new File(inDir, fileName);
        DbFileLoader loader = new DbFileLoader(config);
        loader.delimiter(",")
            .removeQuotes(true)
            .trim(true);
        BatchTracker tracker = trackerManager.findFirst(fileForTestBatch);

        loader.process(tracker, testFile, jobConfigFileName);
    }

}
