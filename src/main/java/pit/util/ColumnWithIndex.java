package pit.util;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import pit.etl.dbloader.JobConfigException;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class ColumnWithIndex {
    private String colWithIndexName;
    private String colWithoutIndexName;
    private int index = -1;


    public static ColumnWithIndex fromColWithIndexName(String colWithIndexName) {
        ColumnWithIndex colInd = new ColumnWithIndex();
        colInd.colWithIndexName = colWithIndexName;
        String[] colAndNum = StringUtils.splitByCharacterTypeCamelCase(colWithIndexName);

        StringBuilder col = new StringBuilder();

        for (String colFragment : colAndNum) {
            if (StringUtils.isAlpha(colFragment)) {
                col.append(colFragment);
            } else if (StringUtils.isNumeric(colFragment)) {
                colInd.index = Integer.parseInt(colFragment);
                break;
            }
        }
        colInd.colWithoutIndexName = col.toString();

        if (colInd.index < 0) {
            throw new JobConfigException("Invalid column name '%s', must consist of name and number, e.g., edit5", colWithIndexName);
        }

        return colInd;
    }

}
