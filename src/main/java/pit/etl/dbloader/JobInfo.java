package pit.etl.dbloader;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;

import java.util.*;

@SuppressWarnings("WeakerAccess")
@Accessors(fluent = true)
@Getter
@Setter
@ToString
public class JobInfo {

    @SuppressWarnings("FieldCanBeLocal")
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    private final static String DERIVED_PREFIX = "derived.";
    private final static String SOURCE_CLAIM_ID_COL = "sourceClaimId.col";
    private final static String SOURCE_LINE_ID_COL = "sourceLineId.col";
    private final static String CLAIM_ID_COLS = "claimId.cols";
    private final static String LINE_ID_COLS = "lineId.cols";
    private final static String STATUS_COL = "status.col";
    private final static String HASH_COLS = "hash.cols";

    private String name;

    private boolean isEtlBatchId = true;
    private boolean isAuditFields = true;
    private boolean isInsertEmpty = false;

    private String targetTable;
    private List<Mapping> mappings = new ArrayList<>();
    private List<Mapping> descriptions = new ArrayList<>();
    private List<Mapping> transformCopy = new ArrayList<>();

    private String sourceClaimIdCol;
    private String sourceLineIdCol;

    private List<String> keyColumns;
    private Set<String> dedupeCols = new LinkedHashSet<>();
    private Set<String> dupeCols = new LinkedHashSet<>();

    private Set<String> requiredCols = new LinkedHashSet<>();
    private Set<String> requiredNonCritCols = new LinkedHashSet<>();
    private Set<String> dateCols = new LinkedHashSet<>();
    private Set<String> numericCols = new LinkedHashSet<>();
    private Set<String> checkValuesOfCols = new LinkedHashSet<>();
    private Set<String> hashCols = new LinkedHashSet<>();

    private Map<String, Integer> truncations = new HashMap<>();

    private int commitChunkSize = -1;
    // for postprocessing
    private Set<String> tables = new LinkedHashSet<>();
    private List<String> lookup = new ArrayList<>();

    private String insert;
    private String function;
    public ArrayList<String> ssnChooseICN = new ArrayList<>();
    public ArrayList<String> ssn1ChooseICN = new ArrayList<>();
    private String claimIdBypass;
    private boolean isSequenceNumbersEnabled;

    private String statusCol;
    private Set<String> claimIdCols = new LinkedHashSet<>();
    private Set<String> lineIdCols = new LinkedHashSet<>();

    public JobInfo(String targetTable) {
        super();
        this.targetTable = targetTable;
        this.name = targetTable;
    }

    public JobInfo map(String srcAndTargetCol) {
        mappings.add(new Mapping(srcAndTargetCol, srcAndTargetCol));
        return this;
    }

    private final String[] UB_PREFIXES = new String[]{"UB"};
    private final String[] HCFA_PREFIXES = new String[]{"HCFA"};
    private final String[] NCPDP_PREFIXES = new String[]{"NCPD"};

    public JobInfo map(BatchTracker batch, String srcCol, String targetCol) {
        boolean isDerived = false;
        if (StringUtils.startsWithIgnoreCase(srcCol, DERIVED_PREFIX)) {
            srcCol = StringUtils.removeStart(srcCol, DERIVED_PREFIX);
            isDerived = true;
        }

        String[] excludedPrefixes = batch.fileInfo().isHcfa() || batch.fileInfo().isDent() ? UB_PREFIXES : HCFA_PREFIXES;
        if (!StringUtils.startsWithAny(srcCol, excludedPrefixes)) {
            mappings.add(new Mapping(srcCol, targetCol, isDerived));
        }

        return this;
    }


    public static JobInfo fromConfig(Config config, BatchTracker batch) {
        JobInfo job = new JobInfo(config.get("target.table", null));
        job.name = config.get("job.name");
        job.isInsertEmpty = config.isTrue("insert.empty.records");

        job.dedupeCols.addAll(config.getStringList("dedupe.by"));
        job.dupeCols.addAll(config.getStringList("dupe.cols"));
        job.dateCols.addAll(config.getStringList("date.cols"));
        job.hashCols.addAll(config.getStringList(HASH_COLS));
        job.requiredCols.addAll(config.getStringList("required.cols"));
        job.requiredNonCritCols.addAll(config.getStringList("required.non.crit.cols"));
        job.numericCols.addAll(config.getStringList("numeric.cols"));
        job.lookup.addAll(config.getStringList("lookup"));
        job.checkValuesOfCols.addAll(config.getStringList("check.values.cols"));

        job.insert = config.get("insert", null);
        job.function = config.get("function", null);
        job.claimIdCols.addAll(config.getStringList("claimId.cols"));
        job.ssnChooseICN.addAll(config.getStringList("ssnChooseICN.cols"));
        job.ssn1ChooseICN.addAll(config.getStringList("ssn1ChooseICN.cols"));
        job.sourceClaimIdCol = config.get(SOURCE_CLAIM_ID_COL, null);
        job.sourceLineIdCol = config.get(SOURCE_LINE_ID_COL, null);
        job.statusCol = config.get(STATUS_COL, null);
        job.claimIdCols = config.getStringSet(CLAIM_ID_COLS);
        job.lineIdCols = config.getStringSet(LINE_ID_COLS);

        Map<String, String> mappings = config.getProps("map");
        for (Map.Entry<String, String> prop : mappings.entrySet()) {
            job.map(batch, prop.getKey(), prop.getValue());
        }

        job.truncations = config.getIntProps("trunc");

        job.tables.addAll(config.getStringList("tables"));

        if (config.contains("commit.size")) {
            job.commitChunkSize = config.getInt("commit.size");
        }
        job.isSequenceNumbersEnabled = config.isTrue("sequence.numbers");


        return job;
    }


    public void ensureValidationPropertiesSet() {
        validateRequiredProp(SOURCE_CLAIM_ID_COL, sourceClaimIdCol);
        validateRequiredProp(SOURCE_LINE_ID_COL, sourceLineIdCol);
        validateRequiredProp(STATUS_COL, statusCol);
        validateRequiredProp(SOURCE_CLAIM_ID_COL, claimIdCols);
        validateRequiredProp(LINE_ID_COLS, lineIdCols);
        validateRequiredProp(HASH_COLS, hashCols);
    }

    private void validateRequiredProp(String name, String val) {
        if (StringUtils.isBlank(val)) {
            throw new UnrecoverableException("Required property %s is undefined", name);
        }
    }

    private void validateRequiredProp(String name, Collection<String> vals) {
        if (vals == null || vals.isEmpty()) {
            throw new UnrecoverableException("Required property %s is undefined", name);
        }
    }
}