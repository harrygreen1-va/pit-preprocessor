package pit.etl.filecopytransform.fbcs;

import org.apache.commons.io.FilenameUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DupeRejectTest implements FileEtlTest {


    private final Config config = Config.load(FileProcessorTestUtils.getEnv());

    @BeforeClass
    public static void setupGlobal() {
        System.setProperty("etl_log_home", "./logs");
        System.setProperty(Config.DISABLE_LOADING_SYSPROP, "true");
    }

    @Test
    public void processFileWithDupes() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20200123-dupes.txt";
        var sourceFile = new File(ccrsDir, fileMask);
        runPreprocessor(ccrsDir, fileMask);
        // TODO: test 1 line less in the concat
        File concatFile = new File(config.getConcatFileLocation(), "CCRS-HCFA-concatenated.txt");
        assertTrue(concatFile.exists());
        var sourceCount = LineColumnCounter.count(sourceFile);
        var concatCount = LineColumnCounter.count(concatFile);
        assertEquals(sourceCount.lineCount - 2, concatCount.lineCount);

        // Rejects location: // in/{source system}/rejects
        String sourceFileName = sourceFile.getName();
        sourceFileName = FilenameUtils.removeExtension(sourceFileName);
        String rejectFileName = sourceFileName + "-Dupes.txt";
        File rejectsDir = new File(ccrsDir, DelimitedFileInfo.REJECTS_DIR_NAME);
        File dupesFile = new File(rejectsDir, rejectFileName);
        assertTrue(dupesFile.exists());

        var dupesCount = LineColumnCounter.count(dupesFile);
        assertEquals(2, dupesCount.lineCount);

    }

    @Ignore
    @Test
    public void processFileDetectDupes() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20200123-dupes.txt";
        var sourceFile = new File(ccrsDir, fileMask);
        runPreprocessor(ccrsDir, fileMask);
    }


}