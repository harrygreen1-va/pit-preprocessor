package pit.etl.filecopytransform.fbcs;

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class FbcsFileNameParser {

    public final static String FILE_NAME_DELIM = "-";

    public static Optional<String> getFileNamePiece(String fileName, int i) {
        // TODO: memoize
        Optional<String> fileNamePiece = Optional.empty();
        String[] fileNamePieces = fileName.split(FILE_NAME_DELIM);
        if (i < fileNamePieces.length) {
            // get rid of glob patterns in case if it's a file mask
            String piece = StringUtils.removeEnd(fileNamePieces[i].trim(), "*");
            fileNamePiece = Optional.of(piece);
        }

        return fileNamePiece;
    }

    //FBCS-Type-Stream-Server-Date
    private final static int SYSTEM_INDEX = 0;
    private final static int TYPE_INDEX = 1;
    private final static int FORM_TYPE_INDEX = 2;
    private final static int DB_ID_INDEX = 3;
    private final static int FEED_DATE_INDEX = 4;

    public static Optional<String> getSystem(String fileName) {
        return getFileNamePiece(fileName, SYSTEM_INDEX);
    }

    public static Optional<String> getType(String fileName) {
        return getFileNamePiece(fileName, TYPE_INDEX);
    }

    public static Optional<String> getFormType(String fileName) {
        return getFileNamePiece(fileName, FORM_TYPE_INDEX);
    }

    public static Optional<String> getDbId(String fileName) {
        return getFileNamePiece(fileName, DB_ID_INDEX);
    }

    public static Optional<String> getFeedDateStr(String fileName) {
        return getFileNamePiece(fileName, FEED_DATE_INDEX);
    }

    public static boolean isFileNameValid(String fileName) {
        return getFeedDateStr(fileName).isPresent();
    }


    public static boolean isToScore(String fileName) {
        return StringUtils.containsIgnoreCase(fileName, "toScore");
    }

    /**
     * FUT, FHT, FUC, FHC, VUC, VHT
     */
    public static String getProcessingCode(String fileName) {
        StringBuilder code = new StringBuilder();
        if (getSystem(fileName).get().startsWith("CCRS")) {
            code.append("S");
        }
        else {
            code.append(getSystem(fileName).orElse("X").charAt(0));
        }

        code.append(getFormType(fileName).orElse("X").charAt(0));
        if (isToScore(fileName)) {
            code.append('C');
        }
        else {
            code.append('T');
        }

        return code.toString().toUpperCase();
    }


}