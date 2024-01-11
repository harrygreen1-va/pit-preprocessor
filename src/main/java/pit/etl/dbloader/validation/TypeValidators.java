package pit.etl.dbloader.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import pit.etl.filecopytransform.fbcs.StatusTransformations;

import java.text.ParseException;

@Slf4j
public class TypeValidators {

    @SuppressWarnings("unused")
    public static boolean isValidNumber(String header, String val) {
        return StringUtils.isBlank(val) || StringUtils.containsOnly(val, "0123456789.");
    }

    @SuppressWarnings("unused")
    public final static FastDateFormat SOURCE_FILE_DATE_FORMAT = FastDateFormat.getInstance("yyyyMMdd");

    public static boolean isValidDate(String header, String val) {
        if (StringUtils.isBlank(val)) return true;
        return canParseDate(header, val);
    }

    private static boolean canParseDate(String header, String val) {
        try {
            SOURCE_FILE_DATE_FORMAT.parse(val);
        } catch (ParseException parseException) {
            log.warn("Invalid date Field: {} Value: {}", header, val);
            return false;
        }
        return true;
    }


    @SuppressWarnings("unused")
    public static boolean isNotEmpty(String header, String val) {
        return StringUtils.isNotBlank(val);
    }

    @SuppressWarnings("unused")
    public static boolean isStatusValid(String header, String val) {
        return StatusTransformations.VALID_STATUSES.contains(val);
    }

    @SuppressWarnings("unused")
    public static boolean isFieldValid(String possibleValues, String val) {
    	String[] entries = possibleValues.split(",");
    	for (String entry : entries) {
    		if (entry.startsWith("!")) {
    			String realEntry = entry.substring(1);
    			if (!realEntry.equalsIgnoreCase(val)) {
    				return true;
    			}
    		}
    		else {
    			if (entry.equalsIgnoreCase(val)) {
    				return true;
    			}
    		}
    	}
    	return false;
    }
}