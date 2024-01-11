package pit.etl.perftest;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LongSummaryStatistics;
import java.util.Set;

@Slf4j
public class QueryTest {
    private final Config config = Config.load(FileProcessorTestUtils.getEnv());
    private final QueryHelper queryHelper = new QueryHelper();
    private final CstDao cstDao = new CstDao();

    @Before
    public void init() {
        DataSourceFactory.init(config);
    }

    @Test
    public void dsProviderQuery() {
        queryHelper.logChunkSize(5000);
        String providerDsQry = "SELECT NPI, tax_id, provider_name, PROVIDER_KEY FROM PITEDR.dbo.DIM_PROVIDER WHERE NPI IS NOT NULL and IS_CURRENT = 'Y' ORDER BY NPI, tax_id";
        queryHelper.runSelect(providerDsQry);
    }

    @Test
    public void dsPatientsQuery() {
        queryHelper.logChunkSize(5000);
        String qry = "SELECT patient_key, member_id FROM  PITEDR.dbo.DIM_PATIENT WHERE is_current='Y'";
        queryHelper.runSelect(qry);
    }

    private String batchId = "CCNNC_H191030052836";
    private File idFile = new File("./test_files/member_ids.txt");

    @Test
    public void prepIds() throws IOException {
        int howMany = 500;
        Set<String> ids = cstDao.fetchPatients(batchId, howMany);

        FileUtils.writeStringToFile(idFile, StringUtils.join(ids, ","), Charset.defaultCharset());
    }

    /*
    500 patients SP with temp:
    Read 58341 rows in 42541 ms
    Read 58341 rows in 8490 ms

    Azure:
    Read 55523 rows in 19829 ms
    Read 55523 rows in 4044 ms

without temp table
2020-07-27 18:25:43.195 INFO CstDao                 - Read 58341 rows in 217167 ms

     */
    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void cstProfQuery() throws IOException {
        int nTimes = 50;
        String idsStr = FileUtils.readFileToString(idFile, Charset.defaultCharset());
        queryHelper.logChunkSize(10000);
        LongSummaryStatistics stats = new LongSummaryStatistics();
        for (int i = 0; i < nTimes; ++i) {
            StopWatch sw = StopWatch.createStarted();
            cstDao.readLinesForDupesProcessingForTest(idsStr, batchId);
            sw.stop();
            long time = sw.getTime();
            stats.accept(time);
        }
        System.err.println("CST Prof Query: " + stats);
    }


    String ps = "";

    @Test
    public void countPs() {
        String[] n = StringUtils.split(ps, ",");
        System.err.println(n.length);
    }

}
