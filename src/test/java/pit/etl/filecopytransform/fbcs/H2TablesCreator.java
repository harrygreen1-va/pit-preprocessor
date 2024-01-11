package pit.etl.filecopytransform.fbcs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;

public class H2TablesCreator {
    private final Logger logger=LoggerFactory.getLogger( getClass() );
    private DataSource ds;

    @Before
    public void setUp() {
        Config config=Config.load( "local" );
        ds=DataSourceFactory.init( config );
    }

    @Ignore("DJH 11/18/23 No H2 support yet")
    @Test
    public void createTables() throws IOException, SQLException {
        try (Connection conn=ds.getConnection()) {
            execSqlFromResource( conn, "ddl/h2/cbl.sql");
            execSqlFromResource( conn, "ddl/h2/eci.sql");
        }
    }

    public void execSqlFromResource( Connection connection, String resourceName )
                    throws IOException, SQLException {
        try (InputStream sqlInput=getClass().getClassLoader().getResourceAsStream( resourceName );) {
            if (sqlInput == null) {
                throw new IOException( "Resource " + resourceName + " not found" );
            }
            String sql=IOUtils.toString( sqlInput, Charset.defaultCharset() );
            logger.info( "Executing SQL:\n{}", sql );
            Statement stmt=connection.createStatement();
            stmt.executeUpdate( sql );
        }

    }

}
