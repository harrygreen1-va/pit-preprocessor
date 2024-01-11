package pit.etl.filecopytransform.fbcs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pit.UnrecoverableException;

public class JdbcTestUtils {
    @SuppressWarnings("unused")
    private final Logger logger=LoggerFactory.getLogger( getClass() );

    public static ResultSet selectSingleRow( Connection conn, String sql ) {
        try {
            PreparedStatement stmt=conn.prepareStatement( sql );
            ResultSet rs=stmt.executeQuery();
            if (!rs.next())
                throw new UnrecoverableException( "No results found for " + sql );

            return rs;
        }
        catch (SQLException e) {
            throw new UnrecoverableException( e );
        }
    }

}
