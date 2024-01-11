package pit.etl.dbloader;

import pit.util.SourceDataUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FirstBy {

    private final Set<String> dedupedValues = new HashSet<>();
    private final Set<String> dedupeCols;
    private final Map<String, Integer> headers;

    public FirstBy(Set<String> dedupeCols, Map<String, Integer> headers) {
        this.dedupeCols = dedupeCols;
        this.headers = headers;
    }

    public boolean checkForDupe(List<String> rawFields) {
        return checkForDupeGetKey(rawFields) != null;
    }

    private String checkForDupeGetKey(List<String> rawFields) {
        if (dedupeCols.isEmpty()) {
            return null;
        }
        String dedupeKey = concatDedupeKey(rawFields);
        if (dedupedValues.contains(dedupeKey)) {
            return dedupeKey;
        }

        dedupedValues.add(dedupeKey);
        return null;
    }

    public Set<String> dedupeCols() {
        return dedupeCols;
    }

    public String concatDedupeKey(List<String> rawFields) {
        return SourceDataUtils.valByHeaders(headers, dedupeCols, rawFields);
    }
}