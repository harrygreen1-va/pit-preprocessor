package pit.etl.perftest;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import pit.etl.config.DataSourceFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Accessors(fluent = true)
@Getter
@Setter
@Slf4j
public class QueryHelper {

    private int logChunkSize = 100;

    @SneakyThrows(SQLException.class)
    public void runSelect(String sql) {
        log.info("Running select:\n{}", sql);
        try (Connection c = createConnection()) {
            StopWatch sw = StopWatch.createStarted();
            int nRows = 0;
            try (PreparedStatement stmt = c.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
                nRows = readResultSet(rs).size();
            }
            sw.stop();
            log.info("Read {} rows in {} ms", nRows, sw.getTime());
        }
    }

    public List<List<Object>> readResultSet(ResultSet resultSet) throws SQLException {
        int i = 0;
        ResultSetMetaData resMeta = resultSet.getMetaData();
        int nColumns = resMeta.getColumnCount();
        StringBuilder buf = new StringBuilder();
        List<List<Object>> vals = new ArrayList<>();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int iCol = 1; iCol <= nColumns; ++iCol) {
                Object val = resultSet.getObject(iCol);
                row.add(val);
            }
            vals.add(row);
            if (i>0 && i % logChunkSize == 0) {
                log.info("Read {} records", i);
            }
//            log.info(vals.toString());
            ++i;
        }

        return vals;
    }

    private Connection createConnection() throws SQLException {
        return DataSourceFactory.obtainConnection();
    }


    public void execSqlFromResource(Connection connection, String resourceName)
            throws IOException, SQLException {
        try (InputStream sqlInput = getClass().getClassLoader().getResourceAsStream(resourceName);) {
            if (sqlInput == null) {
                throw new IOException("Resource " + resourceName + " not found");
            }
            String sql = IOUtils.toString(sqlInput, Charset.defaultCharset());
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
        }

    }

}
