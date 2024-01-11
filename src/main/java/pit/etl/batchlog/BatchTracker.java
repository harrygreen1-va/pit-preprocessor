package pit.etl.batchlog;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import pit.TableMeta;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.filecopytransform.fbcs.Lookup;

import java.io.File;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class BatchTracker {
    private final DelimitedFileInfo fileInfo;
    private Eci eci;
    private final String batchId;
    private Lookup lookup;
    private File loadFile;
    private LocalDateTime startTimestamp = LocalDateTime.now();
    private int lineCount = -1;
    private int conformantLineCount = -1;

    public BatchTracker(DelimitedFileInfo fileInfo) {
        this.fileInfo = fileInfo;
        batchId = generateBatchId(fileInfo);
    }

    public BatchTracker(DelimitedFileInfo fileInfo, String batchId) {
        this.fileInfo = fileInfo;
        this.batchId = batchId;
    }

    public String getSource(Map<String, Integer> headers, String line) {
        if (lookup != null)
            return lookup.getSource(headers, line);
        else
            return "CCRS";
    }

    public String batchId() {
        return batchId;
    }

    public DelimitedFileInfo fileInfo() {
        return fileInfo;
    }

    public void setEci(Eci eci) {
        this.eci = eci;
    }

    public Eci eci() {
        return eci;
    }

    public void setLoadFile(File file) {
        this.loadFile = file;
    }

    public File getLoadFile() {
        return loadFile;
    }
    public TableMeta getTableMeta() {
        return fileInfo.tableMeta();
    }

    // Batch ID format: DBID H/U feeddate Timestamp
    // example: R4V4XX_H170321 165558
    // with file date:

    private final static int DB_ID_MAX_NUM_OF_CHARS = 6;
    private final static DateTimeFormatter NO_CENTURY_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyMMdd");

    @SneakyThrows
    private static String generateBatchId(DelimitedFileInfo delimitedFileInfo) {
//        sleep(1000);  // DJH 11/28 band-aid so that tests pass
        if (delimitedFileInfo == null || !delimitedFileInfo.isFileNameValid()) {
            return generateBatchIdWithoutFile();
        }
        // dbId_hcfa_feeddate_timestamp
        String batchIdFormat = "%s_%s%s%s";
        String feedDateNoCentury = NO_CENTURY_DATE_FORMATTER.format(delimitedFileInfo.feedDate());
        // FIXME: timestamp is added for uniqueness- not very robust


        return String.format(batchIdFormat, StringUtils.left(delimitedFileInfo.databaseId(), DB_ID_MAX_NUM_OF_CHARS),
                StringUtils.left(delimitedFileInfo.formType(), 1), feedDateNoCentury, produceBatchTimestamp());
    }

    private final static DateTimeFormatter MS_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyMMddHHmmssSSS");

    private static String generateBatchIdWithoutFile() {
        return "E_" + MS_TIMESTAMP_FORMATTER.format(LocalDateTime.now());

    }

    public LocalDateTime startTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(LocalDateTime startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public int lineCount() {
        return lineCount;
    }

    public int lineCountNoHeader() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public int conformantLineCount() {
        return conformantLineCount;
    }

    public void setConformantLineCount(int conformantLineCount) {
        this.conformantLineCount = conformantLineCount;
    }


    private final static DateTimeFormatter BATCH_ID_TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmss");

    private static String produceBatchTimestamp() {
        return BATCH_ID_TIME_FORMATTER.format(LocalTime.now());
    }

    @Override
    public String toString() {
        return "[batchId=" + batchId + " fileInfo=" + fileInfo + "]";
    }

    public void setLookup(Lookup lookup) {
        this.lookup = lookup;
    }

    public Lookup getLookup() {
        return lookup;
    }


}