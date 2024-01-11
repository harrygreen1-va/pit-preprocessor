package pit.etl.dbloader;

import org.junit.Before;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;

import java.io.File;
import java.time.LocalDateTime;

public class FileLoaderTest implements FileEtlTest {

    private final Config config = Config.load(FileProcessorTestUtils.getEnv());
    private final BatchTrackerManager trackerManager = new BatchTrackerManager();
    
    @Before
    public void init() {
        DataSourceFactory.init( config );
    }
    
   
    @Test
    public void ohiLoad() {
        runLoader(ccnnDir, "CCNN-TerminalStatus-UB04-CCNNC-20190111.txt", "ub_ohi_load");
    }

    @Test
    public void ohiLoadTruncate() {
        runLoader(ccnnDir, "CCNN-TerminalStatus-HCFA-CCNNC-20190621.txt", "hcfa_ohi_load");
    }

    // TODO: automate testing
    // Expected results: 1 record, because the second one does not have edits
    @Test
    public void sourceEditLoad() {
        runLoader(ccnnDir, "CCNN-ClaimsToScore-HCFA-CCNNC-20190325.txt", "source_edits_load");
    }

    @Test
    public void sourceEditTpaLoad() {
        runLoader(ccnnDir, "CCRS-ClaimsToScore-HCFA-CCRS-20220610.txt", "source_edits_tpa_load");
    }

    @Test
    public void claimProviderLoad() {
        runLoader(ccnnDir, "CCNN-ClaimsToScore-HCFA-CCNNC-20190325.txt", "provider/ecams_hcfa_claim_provider_load");
    }

    @Test
    public void lineProviderLoadEcams() {
        runLoader(ccnnDir, "CCNN-TerminalStatus-HCFA-CCNNC-20190625.txt", "provider/hcfa_line_provider_load");
    }

    @Test
    public void lineProviderLoadFbcs() {
        runLoader(fbcsInDir, "FBCS-TerminalStatus-HCFA-R4V5-20190627.txt", "provider/hcfa_line_provider_load");
    }

    private void runLoader(File inDir, String fileName, String jobConfigFileName) {
        File testFile=new File(inDir, fileName);
        DbFileLoader loader = new DbFileLoader(config);
        BatchTracker tracker = trackerManager.createBatchTracker(testFile, null, LocalDateTime.now());

        loader.process(tracker, testFile, jobConfigFileName);

    }

}