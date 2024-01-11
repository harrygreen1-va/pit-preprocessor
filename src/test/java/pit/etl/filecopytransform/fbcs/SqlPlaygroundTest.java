package pit.etl.filecopytransform.fbcs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;

public class SqlPlaygroundTest {

    Config config=Config.load("local");

    @Ignore("DJH 11/18/23 not sure what this is for")
    @Test
    public void testSimpleSelect() throws ClassNotFoundException, SQLException, IOException {
        try (Connection c=createConnection()) {
            createTable(c);
            String sql="select * from claim_batch_log";
            try (PreparedStatement stmt=c.prepareStatement( sql ); ResultSet rs=stmt.executeQuery()) {

            }
        }
    }

    private Connection createConnection() throws ClassNotFoundException, SQLException {
        DataSource ds=DataSourceFactory.init( config );
        
//        Class.forName( "org.h2.Driver" );
//        Connection conn=DriverManager.getConnection( "jdbc:h2:~/pitdev/h2/pit", "sa", "" );
        Connection conn=ds.getConnection();
        return conn;
    }
    
    private void createTable(Connection connection) throws IOException, SQLException {
        execSqlFromResource(connection, "ddl/h2/cbl.sql");
    }

    public void execSqlFromResource( Connection connection, String resourceName )
                    throws IOException, SQLException {
        try (InputStream sqlInput=getClass().getClassLoader().getResourceAsStream( resourceName );) {
            if (sqlInput==null){
                throw new IOException("Resource "+resourceName+" not found");
            }
            String sql=IOUtils.toString( sqlInput, Charset.defaultCharset() );
            Statement stmt=connection.createStatement();
            stmt.executeUpdate( sql );
        }

    }

}
