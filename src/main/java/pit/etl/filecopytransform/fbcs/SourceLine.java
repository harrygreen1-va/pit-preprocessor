package pit.etl.filecopytransform.fbcs;

import pit.etl.dbloader.JobInfo;
import pit.util.SourceDataUtils;

import java.util.List;
import java.util.Map;

import static pit.util.SourceDataUtils.valuesFromLine;

public record SourceLine(String claimIdOrKey, String lineIdOrKey, String sourceClaimId, String sourceLineId,
                         String status, int lineNumber, String line) {

    public static SourceLine parseLine(JobInfo config, Map<String, Integer> headers, int lineNumber, String line) {

        List<String> vals = valuesFromLine(line);


        return new SourceLine(
                SourceDataUtils.valByHeaders(headers, config.claimIdCols(), vals),
                SourceDataUtils.valByHeaders(headers, config.lineIdCols(), vals),
                SourceDataUtils.valByHeader(headers, config.sourceClaimIdCol(), vals),
                SourceDataUtils.valByHeader(headers, config.sourceLineIdCol(), vals),

                SourceDataUtils.valByHeader(headers, config.statusCol(), vals),

                lineNumber,
                line
        );
    }
}