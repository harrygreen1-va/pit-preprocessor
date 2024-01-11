package pit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import pit.etl.config.Config;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("WeakerAccess")
@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class TableMeta {

    private String name;

    private String lineTable;
    private String lineFk;
    private String lineId;

    private Set<String> excludePatterns = new HashSet<>();
    private Set<String> lineLevelTables = new HashSet<>();
    private Set<String> claimLevelTables = new HashSet<>();

    private List<ColLen> colLens = new ArrayList<>();

    public static TableMeta fromConfig(Config config) {
        TableMeta meta = new TableMeta();
        meta.name = config.get("name");
        meta.lineTable = config.get("line.table");
        meta.lineFk = config.get("line.fk");
        meta.lineId = config.get("line.id");

        meta.lineLevelTables.addAll(config.getStringList("line.level.tables"));
        meta.claimLevelTables.addAll(config.getStringList("claim.level.tables"));

        meta.excludePatterns.addAll(config.getStringList("exclude.patterns"));

        Map<String, Integer> props = config.getIntProps("len");

        meta.colLens = props.entrySet().stream()
                .map(entry -> new ColLen(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        return meta;
    }

    public boolean isLineLevel(String table) {
        return lineLevelTables.contains(table);
    }


    public boolean isTableExcluded(String table) {
        for(String pattern:excludePatterns) {
            if (StringUtils.containsIgnoreCase(table, pattern)) {
                return true;
            }
        }

        return false;
    }

    public int getColLen(String col) {
        for (ColLen len : colLens) {
            if (len.matches(col)) {
                return len.len;
            }
        }

        return -1;
    }

    @Accessors(fluent = true)
    @Getter
    @ToString
    public static class ColLen {
        private String pattern;
        private int len;

        private ColLen(String pattern, int len) {
            this.pattern = pattern;
            this.len = len;
        }

        public boolean matches(String colName) {
            return StringUtils.containsIgnoreCase(colName, pattern);
        }
    }
}
