package pit.etl.dbloader;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;

import java.io.File;
import java.util.LongSummaryStatistics;

public class ClaimLoaderTest implements FileEtlTest {

    private final Config config=Config.load( FileProcessorTestUtils.getEnv() );
    private final BatchTrackerManager trackerManager = new BatchTrackerManager();


    @Before
    public void init() {
        DataSourceFactory.init( config );
    }

    @Ignore("DJH 11/18/23 Need deidentified file from QA?")
    @Test
    public void claimLoadDevFile() {
        runLoader(ccnnDir, "CCNN-ClaimsToScore-HCFA-CCNNC-20190325.txt");
    }

    @Ignore("DJH 11/18/23 Need deidentified file from QA?")
    @Test
    public void claimLoadCcnnHcfa() {
        int nTimes = 50;
        LongSummaryStatistics stats = new LongSummaryStatistics();

        for (int i = 0; i < nTimes; ++i) {
            StopWatch sw = StopWatch.createStarted();

            runLoader(testDir, "CCNN-TerminalStatus-HCFA-CCNNC-20200625.txt");
            sw.stop();
            stats.accept(sw.getTime());
        }
        System.err.println("Claim load: " + stats);

    }

    @SuppressWarnings("SameParameterValue")
    private void runLoader(File inDir, String fileName) {
        File testFile=new File(inDir, fileName);
        ClaimLoader loader = new ClaimLoader(config);
        BatchTracker tracker =  trackerManager.findFirstOrCreate(testFile);
        loader.process(tracker, testFile, 10000);
    }
}