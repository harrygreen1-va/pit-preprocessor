package pit.util;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.config.DataSourceFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionTester {

    private final Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    // Connection testing using Kerberous
    @Ignore("DJH 11/18/23 FIXME?")
    @Test
    public void testKerbConnection() {
        String kerbIniPath="./krb_test/krb5.ini";
        System.setProperty("java.security.krb5.conf", kerbIniPath);
        String jaasConfigPath="./krb_test/SQLJDBCDriver.conf";
        System.setProperty("java.security.auth.login.config",jaasConfigPath);
        
        String user="aacpcimonitor";
        
        String pitDev="jdbc:sqlserver://vaauspcisql81.aac.dva.va.gov;databaseName=PITEDR;integratedSecurity=true;authenticationScheme=JavaKerberos";
        String dbUrl = pitDev;
        try (HikariDataSource ds = new HikariDataSource()) {
            ds.setUsername(user);
            String password=System.getProperty("db.password");
            ds.setPassword(password);
            
            logger.info("Connecting using the JDBC url:\n{}", dbUrl);
            ds.setJdbcUrl(dbUrl);
    
            try (Connection conn = ds.getConnection()) {
                logger.info("Connection established!");
                if (conn.isClosed()){
                    logger.warn("But it's closed!");
                }
            } catch (SQLException e) {
                logger.error("Failed to establish connection using connection string\n{}", e, dbUrl );
                throw new UnrecoverableException(e);
            }
        }
    }

    @Ignore
    @Test
    public void testEncrConnection() {
         
        String dbUrl="jdbc:sqlserver://VAAUSSQLPCI902.aac.dva.va.gov;databaseName=ccrsdb;schema=dbo;encrypt=true;trustServerCertificate=true";
        String user="ccrs_rw";
        try (HikariDataSource ds = new HikariDataSource()) {
            ds.setUsername(user);
            String password=System.getProperty("db.password");
            ds.setPassword(password);

            logger.info("Connecting using the JDBC url:\n{}", dbUrl);
            ds.setJdbcUrl(dbUrl);

            try (Connection conn = ds.getConnection()) {
                logger.info("Connection established!");
                if (conn.isClosed()){
                    logger.warn("But it's closed!");
                }
            } catch (SQLException e) {
                logger.error("Failed to establish connection using connection string\n{}", e, dbUrl );
                throw new UnrecoverableException(e);
            }
        }
    }


}
