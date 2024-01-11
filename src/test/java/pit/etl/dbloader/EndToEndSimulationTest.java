package pit.etl.dbloader;

import org.apache.commons.io.FilenameUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.filecopytransform.fbcs.JdbcTestUtils;
import pit.etl.filecopytransform.fbcs.LineColumnCounter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class EndToEndSimulationTest implements FileEtlTest {


    @BeforeClass
    public static void setupGlobal(){
        System.setProperty("etl_log_home", "./logs");
    }



    @Test
    public void ccrsTerminalHcfa() {
        String fileMask = "CCRS-TerminalStatus-HCFA*";
        runPreprocessor(ccrsDir, fileMask);
    }

    @Test
    public void ccrsTerminalHcfa20230123() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20230123.txt";
        runPreprocessor(ccrsDir, fileMask);
    }

    @Test
    public void ccrsTerminalHcfa20230123CheckLog() {
        System.out.println("ccrsTerminalHcfa20230123()");
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20230123.txt";
        runPreprocessor(ccrsDir, fileMask);
        String out = outContent.toString();
        assertTrue( out.contains("INSERT INTO claim_batch_log"));
        assertTrue( out.contains("insert into claim_provider"));
        assertTrue( out.contains("insert into line_provider"));
        assertTrue( out.contains("insert into claim_insurance_raw"));
        assertTrue( out.contains("insert into source_edits"));
        assertTrue( out.contains("insert into claim_patient"));
        assertTrue( out.contains("insert into etl.claim_line_checksum"));
        System.setOut(System.out);
        System.out.println("Exit success");
    }

    @Test
    public void ccrsTerminalHcfaMissingBox24AFrom() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20231210-Box1A-ICN-missing.txt";
        File ccrsDirPITDEV2445 = new File(ccrsDir, "PITDEV-2445"); // Replace with your directory path
        runPreprocessor(ccrsDirPITDEV2445, fileMask);
    }

    @Ignore ("12/6/2023 OBE, now that we have QA files")
    @Test
    public void ccrsTerminalHcfaBox24BMissingBox1AICNMissingDTReopenedMissingNoDB() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-21230123-Box24B-missing-Box1A-ICN-missing-DTReopened-missing.txt";
        runPreprocessor(ccrsDir, fileMask);
        // Rejects location: // in/{source system}/rejects
        var sourceFile = new File(ccrsDir, fileMask);
        String sourceFileName = sourceFile.getName();
        sourceFileName = FilenameUtils.removeExtension(sourceFileName);
        String rejectFileName = sourceFileName + "-Rejects.txt";
        File rejectsDir = new File(ccrsDir, DelimitedFileInfo.REJECTS_DIR_NAME);
        File rejectsFile = new File(rejectsDir, rejectFileName);
        assertTrue(rejectsFile.exists());
        var rejectsCount = LineColumnCounter.count(rejectsFile);
        assertEquals(5, rejectsCount.lineCount);
    }

    @Ignore ("12/6/2023 OBE, now that we have QA files")
    @Test
    public void ccrsTerminalHcfaBox24BMissingBox1AICNMissingDTReopenedMissing() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-21230123-Box24B-missing-Box1A-ICN-missing-DTReopened-missing.txt";

        String select = "SELECT MAX(rejected_key) FROM [PITEDR].[etl].[rejected_lines]";
        int max_rejected_key = 0;
        try (Connection conn = DataSourceFactory.dataSource().getConnection()) {
            ResultSet rs = JdbcTestUtils.selectSingleRow(conn, select);
            System.out.println(("MAX(rejected_key): " + rs.getInt(1)));
            max_rejected_key = rs.getInt(1);
        } catch (SQLException sqlException) {
            System.out.println(sqlException.getMessage());
        }

        runPreprocessor(ccrsDir, fileMask);
        // Rejects location: // in/{source system}/rejects
        var sourceFile = new File(ccrsDir, fileMask);
        String sourceFileName = sourceFile.getName();
        sourceFileName = FilenameUtils.removeExtension(sourceFileName);
        String rejectFileName = sourceFileName + "-Rejects.txt";
        File rejectsDir = new File(ccrsDir, DelimitedFileInfo.REJECTS_DIR_NAME);
        File rejectsFile = new File(rejectsDir, rejectFileName);
        assertTrue(rejectsFile.exists());
        var rejectsCount = LineColumnCounter.count(rejectsFile);
        assertEquals(5, rejectsCount.lineCount);


        int max_rejected_key_after = 0;
        try (Connection conn = DataSourceFactory.dataSource().getConnection()) {
            ResultSet rs = JdbcTestUtils.selectSingleRow(conn, select);
            System.out.println(("MAX(rejected_key): " + rs.getInt(1)));
            max_rejected_key_after = rs.getInt(1);
        } catch (SQLException sqlException) {
            System.out.println(sqlException.getMessage());
        }
        assertEquals(7, max_rejected_key_after - max_rejected_key);
    }

    @Test
    public void ccrsTerminalDent() {
        String fileMask = "CCRS-TerminalStatus-DENT-CCRS-21231025-DJH-HACK.txt";
        runPreprocessor(ccrsDir, fileMask);
    }

    @Test
    public void ccrsTerminalHcfa245() {
        File ccrsDirPITDEV2445 = new File(ccrsDir, "PITDEV-2445");
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20231205*";
        runPreprocessor(ccrsDirPITDEV2445, fileMask);
    }

    @Ignore("need deindentified TerminalStatus files") @Test
    public void vacsTerminalHcfa() {
        String fileMask = "VACS-TerminalStatus-HCFA*";
        runPreprocessor(vacsDir, fileMask);
    }

    @Test
    public void processFileWithDupes() {
        String fileMask = "CCRS-TerminalStatus-HCFA-CCRS-20200123-dupes.txt";
        runPreprocessor(ccrsDir, fileMask);

    }
}