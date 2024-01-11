package pit.etl.filecopytransform.fbcs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.batchlog.Eci;
import pit.etl.config.Config;
import pit.etl.config.FilePatternConfig;
import pit.etl.dbloader.ClaimLineChecksumDao;
import pit.etl.dbloader.DbFileLoader;
import pit.etl.dbloader.JobInfo;
import pit.etl.dbloader.Mapping;
import pit.etl.dbloader.validation.LineValidation;
import pit.etl.dbloader.validation.ProcessingStats;
import pit.etl.dbloader.validation.SourceDataValidator;
import pit.etl.filecopytransform.FileProcessingException;
import pit.etl.filecopytransform.FilesUtils;
import pit.etl.filecopytransform.LineProcessor;
import pit.util.SourceDataUtils;
import pit.util.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.*;

import static pit.etl.filecopytransform.fbcs.StatusTransformations.FUNCTIONS;
import static pit.util.SourceDataUtils.*;

public class FbcsFileProcessor {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final BatchLogDao batchLogDao = new BatchLogDao();

    private final Config config;
    ClaimLineChecksumDao hashRecordDao;

    public FbcsFileProcessor(Config config) {
        this.config = config;
        this.hashRecordDao = new ClaimLineChecksumDao(config);
    }


    BatchTracker processFile(FilePatternConfig filePatternConfig, File inputFile,
                             File outputFile, Eci eci, BufferedWriter concatenateToFile,
                             LocalDateTime startTimestamp) {

        BatchTracker batch = null;

        try {
            DelimitedFileInfo fileInfo = DelimitedFileInfo.fromFile(inputFile);
            batch = createBatchLogEntry(fileInfo, eci, startTimestamp);

            if (!fileInfo.isFileNameValid()) {
                //noinspection OptionalGetWithoutIsPresent
                throw new FileProcessingException(fileInfo.file(),
                        fileInfo.fileNameValidationError().get());
            }
            // don't attempt to process if the file name is invalid since we
            // derive dbid from the filename
            else {
                ProcessingStats processingStats = new ProcessingStats();
                processingStats.fileName(inputFile.getName());
                LineProcessor lineProcessorForConcatenated = createLineProcessor(filePatternConfig, filePatternConfig.getNumberOfColumns(), processingStats);


                copyFileProcessLines(filePatternConfig, batch, lineProcessorForConcatenated, inputFile, outputFile,
                        concatenateToFile, processingStats);

                logger.info("Processing stats for file {}: {}", inputFile.getName(), processingStats);
                if (Config.isDataLoadingEnabled()) {

                    if (batch.conformantLineCount() > 0) {
                        // Here we can start loading raw data
                        loadData(batch, batch.getLoadFile(), filePatternConfig);
                    }
                    else {
                        logger.warn("File {} does not contain any conformant lines, no loading jobs will be executed", inputFile.getName());
                    }
                }
                else {
                    logger.warn("Data loading is disabled, loading jobs will not be executed");
                }
            }
        } catch (FileProcessingException e) {
            String errorMsg = e.getMessage();
            logger.error("Error processing input file {}\n{}", inputFile.getAbsolutePath(), errorMsg);
            //noinspection ConstantConditions
            batchLogDao.updateBatchEntryWithError(batch.batchId(), "invalid_file", errorMsg);
        }

        return batch;
    }


    private void loadData(BatchTracker batch, File fileToLoad, FilePatternConfig filePatternConfig) {
        DbFileLoader loader = new DbFileLoader(config);
        List<String> jobNames = filePatternConfig.getStringList("load");
        if (!jobNames.isEmpty()) {
            logger.info("{}/{} will be processed by these db jobs: {}", fileToLoad.getName(), batch.batchId(), jobNames);
            loader.process(batch, fileToLoad, jobNames);
        }

    }

    private LineProcessor createLineProcessor(FilePatternConfig filePatternConfig, int targetNumberOfCols, ProcessingStats stats) {

        return new ColumnNormalizer(targetNumberOfCols,
                filePatternConfig.getStringToAppend(),
                filePatternConfig.getStringToAppendToHeader(), stats);
    }

    private BatchTracker createBatchLogEntry(DelimitedFileInfo fileInfo, Eci eci,
                                             LocalDateTime startTimestamp) {
        BatchTracker batch = new BatchTracker(fileInfo);
        batch.setEci(eci);
        batch.setStartTimestamp(startTimestamp);

        if (Config.isDataLoadingEnabled())
            batchLogDao.insertFileBatchEntry(batch);

        return batch;
    }

    private void copyFileProcessLines(FilePatternConfig filePatternConfig,
                                      BatchTracker batch,
                                      LineProcessor lineProcForConcat,
                                      File inputFile,
                                      File copyToFile,
                                      BufferedWriter concatOutput,
                                      ProcessingStats stats) {

        var sw = StopWatch.createStarted();

        int lineCounter = 0;
        SourceDataValidator validator = null;

        try (
                BufferedWriter destWriter = FilesUtils.createBufferedWriter(copyToFile);
                BufferedReader sourceReader = Files.newBufferedReader(inputFile.toPath())) {

            logger.info("Copying file {} to file {}", inputFile.getAbsolutePath(),
                    copyToFile.getAbsolutePath());

            String inputLine;
            Map<String, Integer> headers = new HashMap<>();
            JobInfo vldTransformConfig = null;
            Lookup lookup = null;
            // Claim_id with the list of lines
            Map<String, SourceClaim> claims = new LinkedHashMap<>();
            List<String> duplicateLines = new ArrayList<>();
            String headersLine = null;
            int nonBlankLinesCount = 0;
            // Write to the output file on the local drive and validate every line
            while ((inputLine = sourceReader.readLine()) != null) {

                if (lineCounter == 0) {
                    // Save the header for the dupe file
                    headersLine = inputLine;
                    headers = SourceDataUtils.inferHeaders(inputLine);
                    String vldConfigName = filePatternConfig.getConformantValidationConfig();

                    if (vldConfigName != null) {
                        vldTransformConfig = Config.jobInfoFromConfig(vldConfigName, batch);
                        vldTransformConfig.ensureValidationPropertiesSet();
                        if (Config.isDataLoadingEnabled()) {
                            batch.setLookup(lookup = new Lookup(vldTransformConfig));
                            lookup.scan(inputFile);
                        }
                        validator = new SourceDataValidator(batch, vldTransformConfig, headers, stats);
                        logger.info("Required columns {}", validator.required());
                    }
                }

                if (StringUtils.isNotBlank(inputLine)) {
                    ++nonBlankLinesCount;
                    String transformedInputLine = inputLine;
                    if (vldTransformConfig != null && lineCounter > 0) {
                        transformedInputLine = transform(vldTransformConfig, headers, inputLine, lineCounter, lookup);
                    }
                    // Add columns if needed
                    String outputLineForConcatDsFile = lineProcForConcat.processLine(transformedInputLine, lineCounter);

                    LineValidation lvld = null;
                    if (validator != null && lineCounter > 0) {
                        lvld = validator.validate(outputLineForConcatDsFile, lineCounter);
                    }

                    if (lvld == null || lvld.isSucceeded()) {
                        if (lineCounter > 0 && StringUtils.isNotBlank(inputLine)) {

                            assert vldTransformConfig != null;

                            var sourceLine = SourceLine.parseLine(vldTransformConfig, headers, lineCounter, outputLineForConcatDsFile);

                            final String claimId = sourceLine.claimIdOrKey();
                            var sourceClaim = claims.get(claimId);
                            if (sourceClaim == null) {
                                sourceClaim = new SourceClaim(claimId, sourceLine);
                                claims.put(claimId, sourceClaim);
                            }
                            else {
                                var dupedSourceLine = sourceClaim.addSourceLineWithDupeDetection(sourceLine);
                                if (dupedSourceLine != null) {
                                    validator.addDupeFailure(vldTransformConfig.lineIdCols(), dupedSourceLine);
                                    duplicateLines.add(dupedSourceLine.line());
                                }
                            }
                        }
                    }
                }
                destWriter.write(inputLine);
                destWriter.newLine();
                ++lineCounter;
            }

            List<HashRecord> hashRecords = new ArrayList<>();
            saveDataFiles(batch, concatOutput, headersLine, headers, claims, vldTransformConfig, hashRecords);

            if (!duplicateLines.isEmpty()) {
                saveDupesFile(batch, headersLine, duplicateLines);
            }

            if (validator != null && (validator.isFailed())) {
                logger.warn("{} lines failed validation: {}", validator.validations().size(), validator.stats());
            }

            if (Config.isDataLoadingEnabled() && !hashRecords.isEmpty()) {
                hashRecordDao.populate(batch, hashRecords);
            }

            updateBatch(batch, nonBlankLinesCount, validator);

        } catch (IOException ioex) {
            throw new UnrecoverableException(ioex);
        }

        // preserve the timestamp
        //noinspection ResultOfMethodCallIgnored
        copyToFile.setLastModified(inputFile.lastModified());
        logger.info("Read {} conformant lines out of {} total lines from {} in {}", batch.conformantLineCount(),
                batch.lineCountNoHeader(), batch.fileInfo().file().getName(), sw);

        stats.set("total-lines", batch.lineCountNoHeader());
        stats.set("conformant-lines", batch.conformantLineCount());

    }

    private void saveDataFiles(BatchTracker batch, BufferedWriter concatOutput, String headersLine, Map<String, Integer> headers, Map<String, SourceClaim> claims, JobInfo vldTransformConfig, List<HashRecord> hashRecords) throws IOException {

        try (var loadFileWriter = FilesUtils.createBufferedWriter(createLoadFile(batch))) {
            int claimCounter = 0;

            loadFileWriter.write(SourceDataUtils.prependLineNumberHeaders(headersLine));
            loadFileWriter.newLine();

            for (var sourceClaim : claims.values()) {
                // Write first few lines to the log, this is to help with testing
                if (claimCounter < 10) {
                    logger.info("Batch: {} claim_id: {} Line count: {}", batch.batchId(), sourceClaim.claimId(), sourceClaim.lineCount());
                }
                writeClaimLinesToConcatFile(batch, concatOutput, loadFileWriter, headers, vldTransformConfig, sourceClaim, hashRecords);
                ++claimCounter;
            }
            logger.info("Batch: {} Claim count: {}", batch.batchId(), claimCounter);
        }
    }

    private File createLoadFile(BatchTracker batch) throws IOException {
        var inDirName = config.get("data.in");
        var loadDir = new File(inDirName, "load");
        FileUtils.forceMkdir(loadDir);
        var loadFile = batch.fileInfo().createFileNameWithSuffix(loadDir, "-load.txt");
        batch.setLoadFile(loadFile);
        return loadFile;
    }

    public HashRecord createHashRecord(JobInfo config, Map<String, Integer> headers, SourceLine sourceLine) {
        var hashCols = config.hashCols();
        var values = valuesFromLine(sourceLine.line());
        StringBuilder rawFieldsStr = new StringBuilder();
        for (var col : hashCols) {
            var val = valByHeader(headers, col, values);
            if (StringUtils.isNotBlank(val)) {
                rawFieldsStr.append(val);
                rawFieldsStr.append(DelimitedFileInfo.DELIM);
            }
        }
        return new HashRecord(sourceLine.sourceClaimId(), sourceLine.sourceLineId(), sourceLine.lineNumber(), rawFieldsStr.toString(), Utils.createHash(rawFieldsStr.toString()));
    }

    private void writeClaimLinesToConcatFile(BatchTracker batch, BufferedWriter concatOutput, BufferedWriter loadFileWriter, Map<String, Integer> headers, JobInfo config, SourceClaim sourceClaim, List<HashRecord> hashRecords) throws IOException {
        final List<String> statuses = new ArrayList<>();
        // This is done to insert the status
        final String insert = config.insert();

        if (insert != null) {
            // collect all statuses from the line level
            for (var line : sourceClaim.sourceLines()) {
                statuses.add(valByHeader(headers, insert, valuesFromLine(line.line())));
            }
        }
        int i = 0;
        for (var sourceLine : sourceClaim.sourceLines()) {
            var line = sourceLine.line();
            if (insert != null && "TerminalStatus".equals(batch.fileInfo().type())) {
                // infer claims status from line statuses
                String newVal = FUNCTIONS.get(config.function()).apply(statuses);

                final List<String> rawFields = valuesFromLine(line);
                updateVal(headers, insert, rawFields, newVal); //rewrite
                line = lineFromValues(rawFields);
            }

            var commonColsToPrepend = List.of(
                    batch.batchId(),
                    batch.fileInfo().databaseId(),
                    batch.fileInfo().feedDateStr(),
                    batch.fileInfo().file().getName()
            );

            var lineNumberCols = List.of(
                    Integer.toString(sourceClaim.claimLineNumber()),
                    Integer.toString(sourceLine.lineNumber())
            );

            List<String> colsToPrepend = new ArrayList<>(commonColsToPrepend);
            if (config.isSequenceNumbersEnabled()) {
                colsToPrepend.addAll(lineNumberCols);
            }
            colsToPrepend.add(insert == null ? "" : statuses.get(i));

            String lineWithBatchInfo = prependColumns(line, colsToPrepend);
            concatOutput.write(lineWithBatchInfo);
            concatOutput.newLine();

            String lineWithLineNumbers = prependColumns(line, lineNumberCols);
            loadFileWriter.write(lineWithLineNumbers);
            loadFileWriter.newLine();

            var hashRecord = createHashRecord(config, headers, sourceLine);
            hashRecords.add(hashRecord);

            ++i;
        }
    }

    private void saveDupesFile(BatchTracker batch, String headersLine, List<String> duplicateLines) {
        File dupesFile = batch.fileInfo().getDupesFileName();
        logger.warn("Found {} duplicate lines, saving them to {}", duplicateLines.size(), dupesFile.getPath());
        List<String> linesWithHeaders = new ArrayList<>();
        linesWithHeaders.add(headersLine);
        linesWithHeaders.addAll(duplicateLines);

        FilesUtils.saveFile(dupesFile, linesWithHeaders);

    }

    private String transform(JobInfo jobInfo, Map<String, Integer> headers, String line, int lineCounter, Lookup lookup) {
        List<Mapping> mappings = jobInfo.mappings();
        if (mappings.isEmpty() && jobInfo.truncations().isEmpty()) {
            return line;
        }

        List<String> vals = valuesFromLine(line);

        for (Mapping map : mappings) {
            String val = valByHeader(headers, map.sourceCol(), vals);
            String origVal = valByHeader(headers, map.targetCol(), vals);
            SourceDataUtils.updateVal(headers, map.targetCol(), vals, val);
            logger.debug("Replaced {}:{} with {} from {}", map.targetCol(), origVal, val, map.sourceCol());
        }

        for (Map.Entry<String, Integer> truncation : jobInfo.truncations().entrySet()) {
            truncate(headers, vals, truncation.getKey(), truncation.getValue(), lineCounter);
        }

        if (Config.isDataLoadingEnabled()) {
            lookup.resolve(vals, headers);
        }

        return SourceDataUtils.lineFromValues(vals);
    }

    private void truncate(Map<String, Integer> headers, List<String> vals, String col, int maxLength, int lineCounter) {
        String val = valByHeader(headers, col, vals);

        if (StringUtils.isNotBlank(val) && StringUtils.length(val) > maxLength) {
            val = StringUtils.truncate(val, maxLength);
            SourceDataUtils.updateVal(headers, col, vals, val);
            logger.info("Truncated {}:{} to {}", lineCounter, col, maxLength);
        }
    }

    private void updateBatch(BatchTracker batch, int inputLinesCount, SourceDataValidator validator) {
        // get rid of header
        if (inputLinesCount > 0) {
            --inputLinesCount;
        }

        int conformantLineCount = -1;
        if (validator != null) {
            conformantLineCount = inputLinesCount - validator.criticalCount();
        }

        batch.setLineCount(inputLinesCount);
        batch.setConformantLineCount(conformantLineCount);

        String validationReport = null;
        if (validator != null && !validator.validations().isEmpty()) {
            validationReport = validator.genReport(100);
            validator.saveValidationsToFile();
            validator.saveValidationsToDb(config);
        }

        if (Config.isDataLoadingEnabled())
            batchLogDao.updateBatchEntryWithSuccess(batch.batchId(), inputLinesCount, conformantLineCount, validationReport);
    }


}