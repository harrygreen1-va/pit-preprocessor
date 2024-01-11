package pit.etl.dbloader.validation;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Accessors(fluent = true)
@Getter
@Setter
public class ProcessingStats {
    private Map<String, Integer> statMap = new HashMap<>();

    private String fileName;

    public void incr(String prop) {
        int metric = statMap.getOrDefault(prop, 0);
        statMap.put(prop, metric + 1);
    }

    public void set(String prop, int metric) {
        statMap.put(prop, metric);
    }

    public String toString() {
        return statMap.toString();
    }
}
