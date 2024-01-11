package pit.etl.filecopytransform.fbcs;

import org.junit.BeforeClass;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class ChecksumTest implements FileEtlTest {

    private final Config config = Config.load(FileProcessorTestUtils.getEnv());

    @BeforeClass
    public static void setupGlobal() {
        System.setProperty("etl_log_home", "./logs");
        System.setProperty(Config.DISABLE_LOADING_SYSPROP, "false");
    }

    @Test
    public void processHcfa_Loaded() {
        String fileMask = "VACS-TerminalStatus-HCFA-VACDB-20231117.txt";
        runPreprocessor(vacsDir, fileMask);
        File concatFile = new File(config.get("concat.file.location"), "VACS-HCFA-concatenated.txt");
        var concatCount = LineColumnCounter.count(concatFile);
        assertEquals(5, concatCount.lineCount);
    }


}