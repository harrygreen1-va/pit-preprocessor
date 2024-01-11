package pit.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.dbloader.InvalidInputException;
import pit.etl.dbloader.validation.SourceDataValidator;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

import static pit.etl.filecopytransform.fbcs.DelimitedFileInfo.DELIM;

public class SourceDataUtils {

    private static final Logger logger = LoggerFactory.getLogger(SourceDataUtils.class);

    public static String valByHeader(Map<String, Integer> headers, String header, List<String> rawFields) {
        Integer rawValIndex = headers.get(header);

        if (rawValIndex == null) {
//            throw new UnrecoverableException("Source col %s is not part of the input file", header);
            SourceDataValidator.validationLogger.warn("Source col {} is not part of the input file", header);
            return null;
        }

        // no such column in this line and this line was not rejected (dropped from the input)
        if (rawValIndex >= rawFields.size()) {
            return null;
        }

        return rawFields.get(rawValIndex);
    }

    // Returns concatenated value
    public static String valByHeaders(Map<String, Integer> allHeaders, Collection<String> headers, List<String> rawFields) {
        StringBuilder buf = new StringBuilder();
        for (String col : headers) {
            String val = SourceDataUtils.valByHeader(allHeaders, col, rawFields);
            buf.append(val);
        }

        return buf.toString();
    }


    public static void updateVal(Map<String, Integer> headers, String header, List<String> rawFields, String newVal) {
        Integer rawValIndex = headers.get(header);

        if (rawValIndex == null) {
            throw new InvalidInputException("Source col %s is not part of the input file", header);
        }
        rawFields.set(rawValIndex, newVal);
    }

    public static Map<String, Integer> inferHeaders(String line) {
        return inferHeaders(line, DELIM);
    }

    public static Map<String, Integer> inferHeaders(String line, String delim) {
        Map<String, Integer> headerIndexMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String[] rawHeaderNames = line.split(Pattern.quote(delim));
        int headerI = 0;
        for (String headerName : rawHeaderNames) {
            if (StringUtils.isBlank(headerName)) {
                logger.warn("Empty header in the position {} in line ..{}", headerI, StringUtils.right(line, 100));
                headerName = Integer.toString(headerI);
            }
            headerIndexMap.put(StringUtils.strip(headerName), headerI);
            ++headerI;
        }

        return headerIndexMap;
    }


    public static List<String> valuesFromLine(String line) {
        return valuesFromLine(line, DELIM);
    }

    public static List<String> valuesFromLine(String line, String delim) {
        return Arrays.asList(StringUtils.splitPreserveAllTokens(line, delim));
    }

    public static String lineFromValues(List<String> vals) {
        return StringUtils.join(vals, DELIM);
    }

    public final static String ROW_NUM_HEADER = "file_row_num";
    public final static String CLAIM_ROW_NUM_HEADER = "file_claim_row_num";

    public static String prependLineNumberHeaders(String headersLine) {
        String commonFields = StringUtils.joinWith(DELIM, CLAIM_ROW_NUM_HEADER, ROW_NUM_HEADER);
        return commonFields + DELIM + headersLine;
    }

    public static String prependColumns(String line, List<String> fieldsToPrepend) {
        String delim = DelimitedFileInfo.DELIM;
        String toPrepend = StringUtils.join(fieldsToPrepend, delim);
        return toPrepend + delim + line;
    }

    public static Map<String, String> toHeaderVal(Map<String, Integer> headers, List<String> rawFields) {
        Map<String, String> nameVal = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Integer> entry : headers.entrySet()) {
            nameVal.put(entry.getKey(), rawFields.get(entry.getValue()));
        }

        return nameVal;
    }

    public static String createHash(String input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new UnrecoverableException(e);
        }
        // digest() method is called to calculate message digest
        // of an input digest() return array of byte
        byte[] messageDigest = md.digest(input.getBytes());
        // Convert byte array into signed representation
        BigInteger no = new BigInteger(1, messageDigest);

        // Convert message digest into hex value
        String hashtext = no.toString(16);
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        hashtext = "0x" + hashtext.toUpperCase();

        return hashtext;

    }

}