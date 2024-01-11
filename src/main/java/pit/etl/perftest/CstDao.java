package pit.etl.perftest;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import pit.UnrecoverableException;
import pit.etl.config.DataSourceFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class CstDao {

    @SneakyThrows(SQLException.class)
    public Set<String> fetchPatients(String batchId, int howMany) {
        String sql = "select distinct top " + howMany + " member_id\n" +
                "from dim_patient pat\n" +
                "join dim_va_claim c on c.patient_key=pat.patient_key\n" +
                "where c.etl_batch_id= ?";
        log.info("Selecting {} member ids for batch {}", howMany, batchId);
        Set<String> memberIds = new HashSet<>();
        try (Connection conn = obtainConnection()) {
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, batchId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String mId = rs.getString(1);
                memberIds.add(mId);
            }
        }
        log.info("Found {} member ids", memberIds.size());
        return memberIds;
    }

    @SneakyThrows
    public Set<String> fetchPatientsAndSave(String batchId, int howMany, File file) {
        Set<String> ids = fetchPatients(batchId, howMany);

        FileUtils.writeStringToFile(file, StringUtils.join(ids, ","), Charset.defaultCharset());

        return ids;
    }


    @SneakyThrows(SQLException.class)
    public Date fetchEarliestDate(String batchId) {

//        var sql = "select min(service_date_from) from f_professional_medical_claim_details where etl_batch_id= ?";
        // for whatever reason it is slow with parameter
        String sql = "select min(service_date_from) from f_professional_medical_claim_details where etl_batch_id='" + batchId + "\'";
        log.info("Finding min visit date for batch: {}, sql:\n{}", batchId, sql);

        Date earliestDate;
        try (Connection conn = obtainConnection()) {
            PreparedStatement stmt = conn.prepareStatement(sql);
//            stmt.setString(1, batchId);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new UnrecoverableException("No service date for " + batchId);
            }
            earliestDate = rs.getDate(1);

        }

        return earliestDate;
    }

    public List<List<Object>> readLinesForDupesProcessingForTest(String idString, String batchId) {

        String benefitTypeStr = "Professional";

        Date minDate = fetchEarliestDate(batchId);
//        var ids = fetchPatients(batchId, howManyPatients);
        return readLinesForDupesProcessing(benefitTypeStr, idString, minDate);
    }

    @SneakyThrows(SQLException.class)
    private List<List<Object>> readLinesForDupesProcessing(String benefitTypeStr, String idsStr, Date minDate) {
        String spCall = "selectClaimsForPatients (?,?,?)";
        String sql = "{call " + spCall + "}";
        log.info("Min date: {}, sql:\n{}", minDate, sql);
        List<List<Object>> data;
        try (Connection c = obtainConnection()) {
            StopWatch sw = StopWatch.createStarted();

            PreparedStatement stmt = c.prepareCall(sql);
            stmt.setString(1, benefitTypeStr);
            stmt.setString(2, idsStr);
            stmt.setDate(3, new java.sql.Date(minDate.getTime()));

            QueryHelper helper = new QueryHelper();
            ResultSet rs = stmt.executeQuery();
            data = helper.readResultSet(rs);

            sw.stop();
            log.info("Read {} rows in {} ms", data.size(), sw.getTime());
        }

        return data;
    }

    @SneakyThrows(SQLException.class)
    public List<List<Object>> readLinesForDupesProcessingNewApproach(String benefitTypeStr, String idsStr, int eciId) {
        String spCall = "selectClaimsForPatientsDupeCand (?,?,?)";
        String sql = "{call " + spCall + "}";
        log.info("Eci {}, sql:\n{}", eciId, sql);
        List<List<Object>> data;
        try (Connection c = obtainConnection()) {
            StopWatch sw = StopWatch.createStarted();

            PreparedStatement stmt = c.prepareCall(sql);
            stmt.setString(1, benefitTypeStr);
            stmt.setString(2, idsStr);
            stmt.setInt(3, eciId);

            QueryHelper helper = new QueryHelper();
            ResultSet rs = stmt.executeQuery();
            data = helper.readResultSet(rs);

            sw.stop();
            log.info("Read {} rows in {} ms", data.size(), sw.getTime());
        }

        return data;
    }


    private Connection obtainConnection() throws SQLException {
        return DataSourceFactory.obtainConnection();
    }

}
