package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import pit.util.JdbcUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class InsertUpdateBuilder {

    private StatementInfo statementInfo = new StatementInfo();
    private String table;
    private List<String> cols = new ArrayList<>();
    private Map<String, String> expressions = new HashMap<>();

    public InsertUpdateBuilder(String table) {
        this.table = table;
    }

    public InsertUpdateBuilder targetsFromMappings(List<Mapping> mappings) {
        List<String> colsFromMappings = mappings.stream().map(Mapping::targetCol).toList();
        cols.addAll(colsFromMappings);

        return this;
    }

    public InsertUpdateBuilder addAuditFields() {
        expressions.put("last_updated_date", "getdate()");
        expressions.put("last_updated_user", JdbcUtils.wrap(System.getProperty("user.name")));
        return this;
    }

    public StatementInfo buildStatement() {
        statementInfo.sql(buildInsert());
        return statementInfo;
    }

    private String buildInsert() {
        String sql = "insert into " + table;
        List<String> allCols = new ArrayList<>(cols);
        allCols.addAll(expressions.keySet());

        String colsStr = StringUtils.join(allCols, ",");

        colsStr = encloseInParens(colsStr);

        List<String> values = new ArrayList<>();
        int i = 0;
        for (String col : allCols) {
            if (expressions.containsKey(col)) {
                values.add(expressions.get(col));
            } else {
                values.add("?");
                statementInfo.addPlaceholderIndex(col, i);
                ++i;
            }
        }

        String valuesStr = StringUtils.join(values, ",");
        valuesStr = encloseInParens(valuesStr);

        sql = sql + colsStr + " values " + valuesStr;
        return sql;

    }

    private String encloseInParens(String s) {
        return " (" + s + ") ";
    }

}