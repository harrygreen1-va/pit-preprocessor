package pit.etl.filecopytransform.fbcs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileCopyTransformTest implements FileEtlTest {

    // batch_id, source system/db_id, date, claim line number, line line number, status
    private static final int NUMBER_OF_PREPENDED_FIELDS = 7;

    private static final int EXPECTED_COLUMN_COUNT_CCRS_HCFA = 269;

    private final Config config = Config.load(FileProcessorTestUtils.getEnv());

    @BeforeClass
    public static void setupGlobal(){
        System.setProperty("etl_log_home", "./logs");
        System.setProperty(Config.DISABLE_LOADING_SYSPROP, "true");
    }

    @Test
    public void processMultipleFile_CorrectCounts() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-2023*";
        runPreprocessor(ccrsDir, fileMask);

        var file1 = new File(ccrsDir, "CCRS-TerminalStatus-HCFA-CCRS-20230123.txt");
        var file2 = new File(ccrsDir, "CCRS-TerminalStatus-HCFA-CCRS-20230124.txt");
        File concatFile = new File(config.getConcatFileLocation(), "CCRS-HCFA-concatenated.txt");
        assertTrue(concatFile.exists());

        // Make sure all lines have been transferred to the concat file
        var sourceCount1 = LineColumnCounter.count(file1);
        var sourceCount2 = LineColumnCounter.count(file2);
        var concatCount = LineColumnCounter.count(concatFile);

        // concat without header
        assertEquals(sourceCount1.lineCount + sourceCount2.lineCount - 2, concatCount.lineCount);
        int expectedColumnCount = EXPECTED_COLUMN_COUNT_CCRS_HCFA + NUMBER_OF_PREPENDED_FIELDS;
        // validate columns
        assertEquals(expectedColumnCount, concatCount.columnCount);
    }


    @Test
    public void processEcamsDentFile() {
        String fileMask = "CCNN-TerminalStatus-DENT-CCNNC-20230306-1.txt";
        runPreprocessor(ccnnDir, fileMask);
        var sourceFile = new File(ccnnDir, fileMask);
        var sourceCount = LineColumnCounter.count(sourceFile);
        File concatFile = new File(config.getConcatFileLocation(), "CCNN-DENT-concatenated.txt");
        var concatCount = LineColumnCounter.count(concatFile);
        assertEquals(sourceCount.lineCount, concatCount.lineCount + 1);
        assertEquals(282 + NUMBER_OF_PREPENDED_FIELDS, concatCount.columnCount);

        var loadFile = getLoadFile(sourceFile);
        var loadCount = LineColumnCounter.count(loadFile);
        assertEquals(sourceCount.columnCount + 2, loadCount.columnCount);
    }

    @Test
    public void processSingleFile() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20230123.txt";
        runPreprocessor(ccrsDir, fileMask);
    }

    @Test
    public void processBlankFile() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20221214-empt.txt";
        var sourceFile = new File(ccrsDir, fileMask);
        runPreprocessor(ccrsDir, fileMask);
        File concatFile = new File(config.getConcatFileLocation(), "CCRS-HCFA-concatenated.txt");
        var concatCount = LineColumnCounter.count(concatFile);
        System.err.println(concatCount.lineCount);
        assertEquals(0, concatCount.lineCount);

        var loadFile = getLoadFile(sourceFile);
        var loadCount = LineColumnCounter.count(loadFile);
        assertEquals(1, loadCount.lineCount);
    }

    private File getLoadFile(File sourceFile) {
        var sourceFileName = FilenameUtils.removeExtension(sourceFile.getName());
        String loadFileName = sourceFileName + "-load.txt";
        var loadDir = new File(config.get("data.in"), "load");
        return new File(loadDir, loadFileName);
    }

    @Test
    public void emptyFolder_BlankConcatFile() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS*";

        File emptyInDir = inDir.getParentFile();

        File concatFile = new File(config.getConcatFileLocation(), "CCRS-HCFA-concatenated.txt");

        runPreprocessor(emptyInDir, fileMask);

        assertTrue(concatFile.exists());
        LineColumnCounter.Counts counts = LineColumnCounter.count(concatFile);

        assertEquals(0, counts.lineCount);
    }

    @Test
    public void processUnmatchedFile_Unchanged() {
        // make sure that the files are not transformed
        String fileMask = "FBCS-ClaimsSummary-*";
        runPreprocessor(fbcsInDir, fileMask);

        assertUnchanged(fbcsInDir, outDir, "FBCS-ClaimsSummary-CMS0-20120816.txt");
        assertUnchanged(fbcsInDir, outDir, "FBCS-ClaimsSummary-CMS0-C-20120816.txt");
    }


    @SuppressWarnings("SameParameterValue")
    private void assertUnchanged(File inDir, File outDir, String name) {
        File source = new File(inDir, name);
        File dest = new File(outDir, name);
        assertTrue("Output file must be same size", isFileSameSize(source, dest));
        assertTrue("Output file must have the same timestamp", isSameLastModified(source, dest));
    }

    private static boolean isFileSameSize(File f1, File f2) {
        boolean isSameSize = f1.length() == f2.length();
        if (!isSameSize) {
            System.err.println("Source file size: " + f1.length() + " Dest file size: " + f2.length());
        }
        return isSameSize;
    }

    private static boolean isSameLastModified(File f1, File f2) {
        return (f1.lastModified() == f2.lastModified());
    }

    @Before
    public void deleteOutputFiles() throws IOException {
        FileUtils.deleteDirectory(new File(testFilesDir, "concat"));
        FileUtils.deleteDirectory(new File(testFilesDir, "file_lists"));
        FileUtils.deleteDirectory(new File(inDir, "load"));
        FileUtils.deleteDirectory(new File(testFilesDir, "out"));
    }

}