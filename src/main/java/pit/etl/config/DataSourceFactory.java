package pit.etl.config;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    private static HikariDataSource ds;
    
    public static DataSource init(Config config) {
        ds = new HikariDataSource();

        String jdbcUrl=config.get("db.url");
        logger.info("Connecting using the JDBC url:\n{}", jdbcUrl);
        ds.setJdbcUrl(config.get("db.url"));
        if (config.contains( "db.user" )) {
            ds.setUsername(config.get("db.user"));
        }
        if (config.contains( "db.password" )) {
            ds.setPassword(config.get("db.password") );
        }
        return ds;
    }
    
    public static DataSource dataSource() {
        if (ds==null) {
            throw new UnrecoverableException( "No data source, call createDataSource first" );
        }
        return ds;
    }

    public static Connection obtainConnection() throws SQLException {
        return dataSource().getConnection();
    }


}