package pit.querygen;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import pit.etl.config.Config;
import pit.util.FilePathUtils;

import java.io.File;
import java.util.Collection;

@SuppressWarnings("NewClassNamingConvention")
public class CdwGen {

    private final static String CDW_SQL_TEMPLATE = """
            select top 10 tbl.*
            from %s tbl
            join completed_batches cbl on tbl.etl_batch_id=cbl.etl_batch_id
            and tbl.last_updated_date >= '%s'
            """;

    private final static String CDW_LOOKUP_SQL_TEMPLATE = """
            select top 10 tbl.*
            from %s tbl
            """;
/* Exception
select top 10 tbl.*
from SCORE_REASON_RELATED_ITEMS tbl
join dim_va_claim claim on tbl.claim_key=claim.claim_key
join completed_batches cbl on claim.etl_batch_id=cbl.etl_batch_id
and tbl.last_updated_date >= '2023-11-23'
 */

    private final static String DATE_TO_CLAUSE = "and tbl.last_updated_date <= '%s'";

    public final static String DATE_FROM_PARAM = "2023-11-23";
    public final static String DATE_TO_PARAM = null;

    @Test
    public void genSql() {
        var config = loadConfig();

        var claimTables = config.getStringSet("tables.claim");
        var claimSql = genTables(claimTables, CDW_SQL_TEMPLATE, DATE_FROM_PARAM, DATE_TO_PARAM);
        System.err.println(claimSql);
        var lookupTables = config.getStringSet("tables.lookup");
        var lookupSql = genTables(lookupTables, CDW_LOOKUP_SQL_TEMPLATE, null, null);
        System.err.println("-- *** Lookup tables\n");
        System.err.println(lookupSql);

    }


    private String genTables(Collection<String> tables, String template, String dateFromParam, String dateToParam) {
        StringBuilder combinedSql = new StringBuilder();
        for (var table : tables) {
            if (!combinedSql.isEmpty()) {
                combinedSql.append("\n");
            }
            var sql = genSql(template, table, dateFromParam, dateToParam);
            combinedSql.append(sql).append("\n");
        }

        return combinedSql.toString();
    }


    private final static String CONFIG_FILE_NAME = "pit/querygen/cdw_querygen.properties";

    private Config loadConfig() {

        File configFile = new File(FilePathUtils.getAbsoluteFilePath(CONFIG_FILE_NAME));

        return new Config(configFile);
    }

    private String genSql(String template, String tableName, String dateFrom, String dateTo) {
        String sql = null;
        if (StringUtils.isNotBlank(dateFrom)) {
            sql = String.format(template, tableName, dateFrom);
        }
        else {
            sql = String.format(template, tableName);
        }
        if (StringUtils.isNotBlank(dateTo)) {
            var dateToCondition = String.format(DATE_TO_CLAUSE, dateTo);
            sql += dateToCondition;
        }

        return sql;
    }
}