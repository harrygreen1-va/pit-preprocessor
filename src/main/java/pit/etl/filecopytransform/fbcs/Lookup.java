package pit.etl.filecopytransform.fbcs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.config.DataSourceFactory;
import pit.etl.dbloader.JobInfo;
import pit.util.SourceDataUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static pit.util.SourceDataUtils.valuesFromLine;

public class Lookup {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    String src;
    public String dst;
    String claimCol;
    private final Map<String, String> resolved = new HashMap<>();
    private final Map<String, String> resolvedBy = new HashMap<>();
    private final Map<String, Set<String>> claimsByIcn = new HashMap<>();
    public static final int STEP = 10_000;

    public Lookup(JobInfo jobInfo) {
        if (!jobInfo.lookup().isEmpty()) {
            src = jobInfo.lookup().get(0);
            dst = jobInfo.lookup().get(1);
            claimCol = jobInfo.claimIdBypass();
        }
    }

    /***
     * DJH 11/18/2023 if absent Box1A and ICN looks up in dim_patient and mvi_veteran table
     * and populates resolve for later use
     * @param inputFile
     */
    public void scan(File inputFile) {
        if (src == null)
            return;
        try (
                BufferedReader sourceReader = Files.newBufferedReader(inputFile.toPath(), StandardCharsets.UTF_8)) {

            Map<String, Integer> headers = null;
            Integer srcIdx = null;
            Integer dstIdx = null;
            Integer claimIdx = null;
            for (String line; (line = sourceReader.readLine()) != null; ) {
                if (headers == null) {
                    headers = SourceDataUtils.inferHeaders(line);
                    srcIdx = headers.get(src);
                    dstIdx = headers.get(dst);
                    claimIdx = claimCol == null ? null : headers.get(claimCol);
                }
                else if (StringUtils.isNotBlank(line)) {
                    List<String> values = valuesFromLine(line);
                    String s = values.get(srcIdx);
                    if (s.isEmpty() && dstIdx != null) {
                        String icn = values.get(dstIdx);
                        resolved.put(icn, null);
                        if (claimIdx != null) {
                            String claim = values.get(claimIdx);
                            claimsByIcn.computeIfAbsent(icn, k -> new HashSet<>()).add(claim);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UnrecoverableException(e);
        }

        try (Connection connection = DataSourceFactory.obtainConnection()) {
            List<String> tables = Arrays.asList(
                    "dim_patient",
                    "mvi_veteran");
            List<String> statements = Arrays.asList(
                    "select member_id, icn " +
                            "from (select row_number() over (partition by icn order by ICN_status, patient_key desc) rn, member_id, icn from dim_patient where icn in (?) and member_id is not null and icn is not null) upd " +
                            "where upd.rn = 1",
                    "select ssn, icn_full " +
                            "from (select row_number() over (partition by icn_full order by person_modified_date_time desc) rn, ssn, icn_full from mvi_veteran where icn_full in (?) and icn_full is not null) upd  " +
                            "where upd.rn = 1"
            );
            for (int i = 0; i < statements.size(); i++) {
                List<String> list = new ArrayList<>();

                for (Map.Entry<String, String> entry : resolved.entrySet()) {
                    if (entry.getValue() == null) {
                        logger.debug(">>>>>> entry.getKey()" + entry.getKey() + "::");
                        list.add(entry.getKey());
                    }
                }

                if (list.isEmpty()) {
                    break;
                }

                for (int x = 0; x < list.size(); x += STEP) {
                    StringBuilder sb = new StringBuilder();
                    String comma = "";
                    for (int j = x; j < Math.min(x + STEP, list.size()); j++) {
                        sb.append(comma).append('\'').append(list.get(j)).append('\'');
                        comma = ",";
                    }
                    if (sb.toString().isEmpty() || sb.toString().isBlank() || sb.toString().contains("''")) {
                        logger.debug("lookup in dim_patient, mvi_veteran found empty or blank or '' <two single quotes> which could potentially randomize, SSN, ICN, Box1A or Box60A or similar.  Skipping.");
                        continue;
                    }
                    logger.debug(">>>>>> replacing sb.toString() = ::" + sb.toString() + "::");
                    try (PreparedStatement statement = connection.prepareStatement(statements.get(i).replace("?", sb.toString()))) {
                        try (ResultSet resultSet = statement.executeQuery()) {
                            while (resultSet.next()) {
                                final String r = resultSet.getString(1);
                                final String d = resultSet.getString(2);
                                logger.debug(">>>>>> r=" + r + "d= " + d + "::");
                                resolved.put(d, r);
                                resolvedBy.put(d, tables.get(i));
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new UnrecoverableException(e);
        }
    }

    /***
     * DJH 11/18/2023 sets value based upon resolve populated by scan() above
     * @param values
     * @param headers
     */
    public void resolve(List<String> values, Map<String, Integer> headers) {
        if (src == null)
            return;
        final int index = headers.get(src);
        String s = values.get(index);
        final Integer dstIdx = headers.get(dst);
        if (s.isEmpty() && dstIdx != null) {
            String d = values.get(dstIdx);
            values.set(index, resolved.get(d));
        }
    }

    public String getSource(Map<String, Integer> headers, String line) {
        if (src == null) {
            return null;
        }
        if (dst == null) {
            return null;
        }
        List<String> values = valuesFromLine(line);
        String icn = values.get(headers.get(dst));
        Set<String> claims = claimsByIcn.get(icn);
        if (claims == null) {
            return null;
        }
        String claim = values.get(headers.get(claimCol));
        if (claims.contains(claim)) {
            return resolvedBy.get(icn);
        }
        return null;
    }
}