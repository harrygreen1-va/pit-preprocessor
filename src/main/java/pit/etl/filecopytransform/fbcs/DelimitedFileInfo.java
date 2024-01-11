package pit.etl.filecopytransform.fbcs;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import pit.TableMeta;
import pit.etl.config.Config;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

// TODO validation methods

final public class DelimitedFileInfo {

    public final static String DELIM = "^";


    // sourceSystem-type-formType-database-feedDate
    // FBCS-ClaimsToScore-HCFA-R5V5-20121006.txt

    private String databaseId;
    //
    private String type;
    private String formType;
    private String sourceSystem;
    private String feedDateStr;
    private LocalDateTime feedDate;
    private File file;

    public static DelimitedFileInfo fromFile(File file) {
        DelimitedFileInfo fileInfo = parseFileName(FilenameUtils.getBaseName(file.getName()));
        fileInfo.file = file;

        return fileInfo;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<String> fileNameValidationError = Optional.empty();

    private TableMeta tableMeta;

    private static DelimitedFileInfo parseFileName(String fileName) {
        DelimitedFileInfo delimitedFileInfo = new DelimitedFileInfo();

        if (!FbcsFileNameParser.isFileNameValid(fileName)) {
            delimitedFileInfo.fileNameValidationError = Optional.of(
                    String.format("Invalid file name '%s'; it must be in the format of "
                            + "Source-Type-Stream-Server-Date", fileName));
        }
        // Attempt to parse regardless of the file name issues
        delimitedFileInfo.sourceSystem = FbcsFileNameParser.getSystem(fileName).orElse(null);
        delimitedFileInfo.type = FbcsFileNameParser.getType(fileName).orElse(null);
        delimitedFileInfo.formType = FbcsFileNameParser.getFormType(fileName).orElse(null);
        delimitedFileInfo.databaseId = FbcsFileNameParser.getDbId(fileName).orElse(null);

        delimitedFileInfo.feedDateStr = null;
        Optional<String> feedDateStr = FbcsFileNameParser.getFeedDateStr(fileName);
        if (feedDateStr.isPresent()) {

            delimitedFileInfo.feedDateStr = feedDateStr.get();
            try {
                delimitedFileInfo.feedDate = dateFromString(delimitedFileInfo.feedDateStr);
            } catch (DateTimeParseException e) {
                delimitedFileInfo.fileNameValidationError = Optional
                        .of(String.format("Feed date '%s' is in invalid format: %s ",
                                delimitedFileInfo.feedDateStr, e.getMessage()));
            }
        }
        delimitedFileInfo.tableMeta = delimitedFileInfo.loadTableMeta();
        return delimitedFileInfo;
    }

    private TableMeta loadTableMeta() {
        String tableMetaName;
        if (isDent()) {
            tableMetaName = "claim_dent";
        }
        else if (isProf()) {
            tableMetaName = "claim_prof";
        }
        else if (isInst()) {
            tableMetaName = "claim_inst";
        }
        else if (isPharmacy()) {
            tableMetaName = "claim_pharm";
        }
        else {
            throw new IllegalStateException("Unsupported claim/payment type");
        }

        return Config.tableMetaFromConfig(tableMetaName);
    }


    Optional<String> fileNameValidationError() {
        return fileNameValidationError;
    }

    public boolean isFileNameValid() {
        return fileNameValidationError.isEmpty();
    }


    public boolean isPharmacy() {
        return StringUtils.equalsIgnoreCase(formType, "NCPDP");
    }

    public boolean isHcfa() {
        return StringUtils.equalsIgnoreCase(formType, "HCFA");
    }

    public boolean isDent() {
        return StringUtils.equalsIgnoreCase(formType, "DENT");
    }

    public boolean isProf() {
        // TODO: extend for EDI
        return isHcfa();
    }

    public boolean isUb() {
        return StringUtils.startsWithIgnoreCase(formType, "UB");
    }


    private boolean isInst() {
        // TODO: extend for EDI
        return isUb();
    }

    public TableMeta tableMeta() {
        return tableMeta;
    }

    public String claimIdFieldName() {
        return isHcfa() || isDent() ? "HCFALines_HCFAID" : "UB92UniqueClaimId";
    }

    // Unique HCFALines_HCFAID
    // UBL_UB92ID
    public String claimLineIdFieldName() {
        return isHcfa() || isDent() ? "HCFALines_ID" : "UBL_ID";
    }

/*
From the report:
    public final static int HCFA_CLAIM_LINE_ID_IND=99; //  HCFALines_ID
    public final static int HCFA_CLAIM_ID_IND=97; //HCFALines_HCFAID

    public final static int UB_CLAIM_ID_IND=268; //UB92UniqueClaimID
    public final static int UBCLAIM_LINE_ID_IND=279; //  UBL_ID
*/

    /**
     * FBCS-TerminalStatus-UB04-R1V18T-20170221.txt
     * FBCS-TerminalStatus-UB04-R1V18T-C-20170221.txt
     */

    File companionFile() {
        String fileName = String.join(FbcsFileNameParser.FILE_NAME_DELIM, sourceSystem, type, formType, databaseId, "C",
                feedDateStr);
        fileName += "." + FilenameUtils.getExtension(file.getName());

        return new File(fileName);
    }

    private final static String DATE_FORMAT = "yyyyMMdd";
    private final static DateTimeFormatter FEED_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATE_FORMAT + "HHmmss");

    private static LocalDateTime dateFromString(String dateStr) {
        if (dateStr.length() < FEED_DATETIME_FORMATTER.toString().length()) {
            // default
            dateStr = StringUtils.left(dateStr, 8) + "000001";
        }
        return LocalDateTime.parse(dateStr, FEED_DATETIME_FORMATTER);
    }

    /**
     * FUT, FHT, FUC, FHC, VUC, VHT, CUC, CHT, SUC, SHT
     */
    String processingCode() {
        if (!isFileNameValid()) {
            return null;
        }
        StringBuilder code = new StringBuilder();

        if (isCCRS()) {
            code.append("S");
        }
        else {
            code.append(sourceSystem().charAt(0));
        }
        code.append(formType.charAt(0));
        if (isToScore()) {
            code.append('C');
        }
        else {
            code.append('T');
        }

        return code.toString().toUpperCase();
    }

    public boolean isCCRS() {
        return sourceSystem().equalsIgnoreCase("CCRS");
    }

    public File file() {
        return file;
    }

    public String databaseId() {
        return databaseId;
    }

    public String type() {
        return type;
    }

    public String formType() {
        return formType;
    }


    private final static String UB_FORM_NAME = "UB92";

    /**
     * Translate UB04 to UB92
     */
    public String normalizedFormType() {
        String normalized = formType;
        if (StringUtils.startsWithIgnoreCase(formType, "UB")) {
            normalized = UB_FORM_NAME;
        }
        return normalized;
    }

    public String sourceSystem() {
        return sourceSystem;
    }

    public String feedDateStr() {
        return feedDateStr;
    }

    public LocalDateTime feedDate() {
        return feedDate;
    }

    public boolean isToScore() {
        return FbcsFileNameParser.isToScore(file.getName());
    }

    private final static String REJECTS_FILE_NAME_SUFFIX = "-Rejects.txt";
    private final static String DUPES_FILE_NAME_SUFFIX = "-Dupes.txt";
    public final static String REJECTS_DIR_NAME = "rejects";


    public File getRejectedFileName() {
        return createFileNameWithSuffix(getRejectDir(), REJECTS_FILE_NAME_SUFFIX);
    }

    public File getDupesFileName() {
        return createFileNameWithSuffix(getRejectDir(), DUPES_FILE_NAME_SUFFIX);
    }

    private File getRejectDir() {
        // in/{source system}/rejects in the dropzone
        return new File(file.getParent(), REJECTS_DIR_NAME);
    }

    public File createFileNameWithSuffix(File dir, String fileNameSuffix) {
        String sourceFileName = file().getName();
        sourceFileName = FilenameUtils.removeExtension(sourceFileName);
        String rejectFileName = sourceFileName + fileNameSuffix;
        return new File(dir, rejectFileName);
    }

    @Override
    public String toString() {
        return "[File=" + file.getName() + ", dbId=" + databaseId + ", type=" + type + ", formType=" + formType
                + ", sourceSystem=" + sourceSystem + ", feedDateStr=" + feedDateStr + "]";
    }

}