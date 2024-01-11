package pit.etl.dbloader.validation;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Set;


@Accessors(fluent = true)
@Getter
@Setter
@ToString
class ColumnValidation {

    private Set<String> colNames;
    private String message;
    private Severity severity;

    ColumnValidation(Severity severity, String message, Set<String> cols) {
        this.severity=severity;
        this.message=message;
        this.colNames=cols;
    }

}
