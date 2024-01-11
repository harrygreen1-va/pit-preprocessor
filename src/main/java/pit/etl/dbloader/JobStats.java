package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class JobStats {
    @ToString.Exclude
    private JobInfo jobInfo;
    long elapsedTime;

    public JobStats(JobInfo jobInfo) {
        this.jobInfo=jobInfo;
    }

    private int inputLines=-1;
    private int failedValidation=0;
    private int inserted=0;
    private int droppedByFirstBy=0;
    private int noData=0;

    public void incrFailedValidation() {
        ++failedValidation;
    }

    public void incrDroppedByFirstBy() {
        ++droppedByFirstBy;
    }

    public void incrNoData() {
        ++noData;
    }

}
