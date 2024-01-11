package pit.etl.dbloader;

import org.apache.commons.lang3.StringUtils;
import pit.etl.batchlog.BatchTracker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pit.util.SourceDataUtils.valByHeader;

@SuppressWarnings("WeakerAccess")
public class Derivator {

    private final BatchTracker batch;
    private final Map<String, Integer> headers;
    private final List<String> sourceValues;

    private final HashMap<String, String> carcLookup;

    public Derivator(BatchTracker batch, Map<String, Integer> headers, List<String> sourceValues, HashMap<String, String> carcLookup) {
        this.batch = batch;
        this.headers = headers;
        this.sourceValues = sourceValues;
        this.carcLookup = carcLookup;
    }

    private final Map<String, String> derivedVals = new HashMap<>();

    public void createDerivations(List<Mapping> mappings) {
        for (Mapping mapping : mappings) {
            if (mapping.isDerived()) {
                String val = deriveCol(mapping);
                if (val != null) {
                    derivedVals.put(mapping.sourceCol(), val);
                }
            }
        }
    }

    public String getDerivedVal(String colName) {
        return derivedVals.get(colName);
    }

    private String deriveCol(Mapping mapping) {
        String derivedVal = null;

        String sourceCol = mapping.sourceCol();

        if (StringUtils.equalsIgnoreCase(sourceCol, "claim_id")) {
            derivedVal = getClaimId(batch, headers, sourceValues);
        }
        else if (StringUtils.equalsIgnoreCase(sourceCol, "source_claim_line_id")) {
            derivedVal = getClaimLineId(batch, headers, sourceValues);
        }
        else if (StringUtils.equalsAnyIgnoreCase(sourceCol, "y", "n")) {
            derivedVal = sourceCol.toUpperCase();
        }
        else if (StringUtils.equalsAnyIgnoreCase(sourceCol, "patient_key")) {
            derivedVal = "-1";
        }
        else if (StringUtils.equalsAnyIgnoreCase(sourceCol, "EditCode", "EditSubCode")) {
            derivedVal = processCarcRarc(mapping, null);
        }
        else if (StringUtils.equalsAnyIgnoreCase(sourceCol, "TPA_CARC_Code")) {
        	derivedVal = processCarcRarc(mapping, "carc");
        }
        else if (StringUtils.equalsAnyIgnoreCase(sourceCol, "CAGC_Code")) {
            derivedVal = processCarcRarc(mapping, "cagc");
        }
        return derivedVal;
    }

    private String processCarcRarc(Mapping mapping, String prefix) {

        String listVal = valByHeader(headers, mapping.sourceCol(), sourceValues);
        String[] codes = StringUtils.splitPreserveAllTokens(listVal, ',');

        if (prefix == null) {
        	prefix = StringUtils.left(mapping.targetCol(), 4);
        }

        int i = 0;
        boolean isListBlank = true;
        if (codes != null) {
            for (String code : codes) {
                String codeColName = prefix + (i + 1);
                if (StringUtils.isNotBlank(code)) {
                    derivedVals.put(codeColName, code.trim());
                    if (StringUtils.equalsIgnoreCase(codeColName, "carc1")) {
                        if (carcLookup.containsKey(code)) {
                            derivedVals.put("TPA_CARC1_DESC", carcLookup.get(code));
                        }
                    }
                    else if (StringUtils.equalsIgnoreCase(codeColName, "cagc1")) {
                        HashMap<String, String> map = getCagcLoolkup();
                        if (map.containsKey(code)) {
                            derivedVals.put("CAGC_DESC", map.get(code));
                        }
                    }
                    isListBlank = false;
                }

                ++i;
            }
        }

        if (isListBlank) {
            listVal = null;
        }
        else {
            listVal = StringUtils.stripEnd(listVal, ",");
        }

        return listVal;
    }

    private HashMap<String, String> getCagcLoolkup() {
        HashMap<String, String> map = new HashMap<>();
        map.put("CO", "Contractual Obligation");
        map.put("OA", "Other Adjustment");
        map.put("PI", "Payor Initiated Reduction");
        map.put("PR", "Patient Responsibility");
        return map;
    }
    public static String getClaimId(BatchTracker batchTracker, Map<String, Integer> headers, List<String> rawFields) {
        String claimIdName = batchTracker.fileInfo().claimIdFieldName();
        return valByHeader(headers, claimIdName, rawFields);
    }

    public static String getClaimLineId(BatchTracker batchTracker, Map<String, Integer> headers, List<String> rawFields) {
        String claimIdName = batchTracker.fileInfo().claimLineIdFieldName();
        return valByHeader(headers, claimIdName, rawFields);
    }

}