package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class StatementInfo implements AutoCloseable {
    private PreparedStatement preparedStmt;
    private Map<String, Integer> columnPlaceholderIndex = new HashMap<>();
    private String sql;

    public void addPlaceholderIndex(String col, int i) {
        columnPlaceholderIndex.put(col, i);
    }

    public int getPlaceholderIndex(String col) {
        Integer i = columnPlaceholderIndex.get(col);
        if (i == null) {
            throw new IllegalStateException("Didn't find column " + col + " in the list of placeholders for statement " + sql);

        }
        return i;
    }

    public void prepare(Connection conn) throws SQLException {
        preparedStmt = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws SQLException {
        if (preparedStmt != null && !preparedStmt.isClosed()) {
            preparedStmt.close();
        }

    }
}