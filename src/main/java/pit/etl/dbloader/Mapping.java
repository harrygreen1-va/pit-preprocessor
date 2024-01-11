package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("WeakerAccess")
@Accessors(fluent = true)
@Getter @Setter
@ToString
public class Mapping {
    private String sourceCol;
    private String targetCol;
    private boolean isDerived;

    private int sourceLineInd;


    public Mapping(String sourceCol, String targetCol, boolean isDerived) {
        super();
        this.sourceCol = sourceCol;
        if (StringUtils.isBlank(targetCol)) {
            targetCol=sourceCol;
        }
        this.targetCol = targetCol;
        this.isDerived= isDerived;
    }

    public Mapping(String sourceCol, String targetCol) {
        this(sourceCol, targetCol, false);
    }
    
    public boolean isSourceField() {
        return !StringUtils.equalsAnyIgnoreCase(targetCol, "etl_batch_id", "claim_id", "source_claim_line_id", "source_edit_id", "file_row_num");
    }
    
}