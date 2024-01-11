package pit.etl.filecopytransform.fbcs;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class LineValidationTest implements FileEtlTest {

    private final Config config = Config.load(FileProcessorTestUtils.getEnv());

    @BeforeClass
    public static void setupGlobal() {
        System.setProperty("etl_log_home", "./logs");
        System.setProperty(Config.DISABLE_LOADING_SYSPROP, "true");
    }

    @Test
    public void processInvalidDatesHcfa_Rejected() {
        String fileMask = "VACS-TerminalStatus-HCFA-VACDB-20231116-invalid-date.txt";
        runPreprocessor(vacsDir, fileMask);
        File concatFile = new File(config.get("concat.file.location"), "VACS-HCFA-concatenated.txt");
        var concatCount = LineColumnCounter.count(concatFile);
        assertEquals(1, concatCount.lineCount);
    }

    // TODO: redo with deidentified data
    @Ignore
    @Test
    public void processInvalidDatesUB_Rejected() {
        String fileMask = "CCNN-TerminalStatus-UB04-CCNNC-20190411.txt";
        runPreprocessor(ccnnDir, fileMask);
    }

}