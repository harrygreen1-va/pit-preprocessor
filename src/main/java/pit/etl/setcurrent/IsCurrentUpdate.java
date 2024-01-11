package pit.etl.setcurrent;

import pit.util.JdbcUtils;

public record IsCurrentUpdate(IsCurrentRecord isCurrentRecord, boolean isCurrentToSet) {
    public String isCurrentToSetStr() {
        return JdbcUtils.boolToYesNo(isCurrentToSet);
    }
}