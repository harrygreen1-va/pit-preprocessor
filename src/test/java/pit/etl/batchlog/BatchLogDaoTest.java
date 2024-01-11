package pit.etl.batchlog;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;
import pit.etl.filecopytransform.fbcs.JdbcTestUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class BatchLogDaoTest implements FileEtlTest {

    // TODO: dev env, separate folder for tests
    Config config = Config.load(FileProcessorTestUtils.getEnv());

    @Before
    public void init() {
        DataSourceFactory.init(config);
    }

    @Test
    public void createBatchLogEntry_PopulatedCorrectly() throws SQLException {
        BatchLogDao dao = new BatchLogDao();

        String testFileName = "CCRS-TerminalStatus-HCFA-CCRS-20230123.txt";
        File testFile = new File(ccrsDir, testFileName);
        DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(testFile);
        BatchTracker batch = new BatchTracker(fileInfo);
        System.out.println(batch);
        assertTrue(batch.batchId().startsWith("CCRS"));
        dao.insertFileBatchEntry(batch);

        String select = "select * from claim_batch_log where etl_batch_id='" + batch.batchId() + "'";
        try (Connection conn = DataSourceFactory.dataSource().getConnection()) {
            ResultSet rs = JdbcTestUtils.selectSingleRow(conn, select);
            assertEquals(batch.fileInfo().normalizedFormType(), rs.getString("source_system"));
            assertEquals(testFileName, rs.getString("file_name"));
            assertEquals(BatchLogDao.PREPROCESSING_STATUS, rs.getString("batch_status"));
            assertEquals("2023-01-23", rs.getDate("feed_date").toString());
            assertNull(rs.getObject("eci_id"));

            dao.updateBatchEntryWithSuccess(batch.batchId(), 101, 101, null);
            rs = JdbcTestUtils.selectSingleRow(conn, select);
            assertEquals(BatchLogDao.IN_PROCESS_STATUS, rs.getString("batch_status"));
            assertEquals(101, rs.getInt("number_of_rows"));
            assertEquals("N", rs.getString("to_score_indicator"));


            dao.updateBatchEntryWithSuccess(batch.batchId(), 0, 0, null);
            rs = JdbcTestUtils.selectSingleRow(conn, select);
            assertEquals(BatchLogDao.SUNBBED_STATUS, rs.getString("batch_status"));
            assertEquals(0, rs.getInt("number_of_rows"));
            //assertEquals( "N", rs.getString( "to_score_indicator" ) );
        }
        /*
        testFileName = "FBCS-TerminalStatus-HCFA-R4V5-20120605.txt";
        testFile=new File(fbcsInDir,testFileName);
        fileInfo = FbcsFileInfo.fromFile(testFile);
        batch = new BatchTracker(fileInfo);
        System.out.println(batch);
        dao.insertFileBatchEntry(batch);
        select = "select * from claim_batch_log where etl_batch_id='" + batch.batchId() + "'";
        try (Connection conn = DataSourceFactory.dataSource().getConnection()) {
            ResultSet rs = JdbcTestUtils.selectSingleRow(conn, select);
            assertEquals("N", rs.getString("to_score_indicator"));
        }

         */

    }

    // TODO test for max ECI_Id, no ECI
    @Ignore("DJH 11/18/23 FIX ME?")
    @Test
    public void testSelectEci() throws SQLException {
        populateEci();
        BatchLogDao dao = new BatchLogDao();
        Eci eci = dao.selectInProcessEci();
        assertNotNull(eci);
    }

    private void populateEci() throws SQLException {
        try (Connection conn = DataSourceFactory.dataSource().getConnection()) {
            String sql = "INSERT INTO ETL_CST_INTERFACE  ( eci_status ) VALUES ( 'In Process')";
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
            conn.commit();
        }
    }


}