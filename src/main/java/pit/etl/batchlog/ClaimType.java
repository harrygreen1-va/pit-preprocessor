package pit.etl.batchlog;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import pit.TableMeta;
import pit.etl.config.Config;

@Getter
public enum ClaimType {
    PROF("claim_prof"),
    INST("claim_inst"),
    DENT("claim_dent"),
    PHARM("claim_pharm"),
    ;

    private final TableMeta tableMeta;

    ClaimType(String tableMetaName) {
        tableMeta = Config.tableMetaFromConfig(tableMetaName);
    }


    public static ClaimType fromBatchId(String batchId) {
        // CCRS_H230123092207
        //CXM_8I_20
        if (StringUtils.contains(batchId, "_H") || StringUtils.contains(batchId, "_8P")) {
            return PROF;
        }
        else if (StringUtils.contains(batchId, "_U") || StringUtils.contains(batchId, "_8I")) {
            return INST;
        }
        else if (StringUtils.contains(batchId, "_D") || StringUtils.contains(batchId, "_8D")) {
            return DENT;
        }
        else if (StringUtils.contains(batchId, "_N")) {
            return PHARM;
        }
        throw new IllegalStateException("Unable to deduce claim type from batch id: " + batchId);
    }
}