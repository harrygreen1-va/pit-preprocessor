package pit.etl.perftest;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LongSummaryStatistics;

@Slf4j
public class CstDupesNewApproachTest {
    //    private final Config config = Config.load(FileProcessorTestUtils.getEnv());
    private final Config config = Config.load("preprod-azure");
    private final QueryHelper queryHelper = new QueryHelper();
    private final CstDao cstDao = new CstDao();

    @Before
    public void init() {
        DataSourceFactory.init(config);
    }

    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void dsProviderQuery() {
        queryHelper.logChunkSize(5000);
        String providerDsQry = "SELECT NPI, tax_id, provider_name, PROVIDER_KEY FROM PITEDR.dbo.DIM_PROVIDER WHERE NPI IS NOT NULL and IS_CURRENT = 'Y' ORDER BY NPI, tax_id";
        queryHelper.runSelect(providerDsQry);
    }

    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void dsPatientsQuery() {
        queryHelper.logChunkSize(5000);
        String qry = "SELECT patient_key, member_id FROM  PITEDR.dbo.DIM_PATIENT WHERE is_current='Y'";
        queryHelper.runSelect(qry);
    }

    /*
    CCNNC_H200811182807
CCNNC_U200811183954
     */

    int eciId = 5940;
    private String profBatchId = "CCNNC_H200811182807";
    private String instBatchId = "CCNNC_U200811183954";
    private File profIdFile = new File("./test_files/member_ids_prof.txt");
    private File instIdFile = new File("./test_files/member_ids_inst.txt");

    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void prepIds() {
        int howMany = 3000;
        cstDao.fetchPatientsAndSave(profBatchId, howMany, profIdFile);
        cstDao.fetchPatientsAndSave(instBatchId, howMany, instIdFile);

    }

    @Ignore("DJH 11/18/23 FIXME? ")
    @Test
    public void cstInstQuery() throws IOException {
        //prepIds();
        runQuery(1, "INSTITUTIONAL", instIdFile, eciId);
    }


    public void runQuery(int nTimes, String benefitType, File idFile, int eciId) throws IOException {
        String idsStr = FileUtils.readFileToString(idFile, Charset.defaultCharset());
        queryHelper.logChunkSize(10000);
        LongSummaryStatistics stats = new LongSummaryStatistics();
        for (int i = 0; i < nTimes; ++i) {
            StopWatch sw = StopWatch.createStarted();
            cstDao.readLinesForDupesProcessingNewApproach(benefitType, idsStr, eciId);
            sw.stop();
            long time = sw.getTime();
            stats.accept(time);
        }
        System.err.println("CST Query: " + stats);
    }


}
