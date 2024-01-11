package pit.etl.filecopytransform.fbcs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.batchlog.Eci;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.config.FilePatternConfig;
import pit.etl.filecopytransform.FilesUtils;
import pit.util.FilePathUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class FbcsFileSetProcessor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Config config;

    public FbcsFileSetProcessor(Config config) {
        this.config = config;
    }

    public void process(FilePatternConfig filePatternConfig, File inputDir, String includeFilePattern,
                        File targetDir) {

        if (Config.isDataLoadingEnabled()) {
            DataSourceFactory.init(config);
        }

        String companionPattern = includeFilePattern + "-C-*";
        FileSetInfo fileSetInfo = new FileSetInfo(includeFilePattern);

        Path concatenatedFile = constructConcatenatedPath(config, fileSetInfo);
        // Required by DataStage's validate sequence -- must exist
        FilePathUtils.touchOrTruncate(concatenatedFile.toFile());

        Path fileListFile = constructFileListPath(config, fileSetInfo);
        // Required by DataStage's archive process -- must exist
        FilePathUtils.touchOrTruncate(fileListFile.toFile());

        List<File> dataFiles = findInputFiles(inputDir, includeFilePattern, companionPattern);
        if (dataFiles.isEmpty()) {
            logger.warn("Did not find any files in {} using pattern {}",
                    inputDir.getAbsolutePath(), includeFilePattern);
            return;
        }

        processDataFiles(fileSetInfo, filePatternConfig, dataFiles, targetDir, concatenatedFile,
                fileListFile);

        List<File> companionFiles = findInputFiles(inputDir, companionPattern, null);
        copyCompanionFiles(companionFiles, targetDir);
    }

    private void processDataFiles(FileSetInfo fileSetInfo, FilePatternConfig processingConfig,
                                  List<File> files, File targetDir, Path concatenatedFile, Path fileListFile) {

        logger.info("File list location: {}", fileListFile.toAbsolutePath());
        logger.info("All files are concatenated in {}", concatenatedFile.toAbsolutePath());

        Eci eci = obtainEci();
        int lineCount = 0;
        int fileCount = 0;

        try (
                BufferedWriter concatWriter = Files.newBufferedWriter(concatenatedFile,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                BufferedWriter fileListWriter = Files.newBufferedWriter(fileListFile,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            LocalDateTime startTimestamp = LocalDateTime.now();

            for (File dataFile : files) {

                BatchTracker batch = processFile(processingConfig, dataFile, targetDir, eci,
                        concatWriter, startTimestamp);
                if (batch.lineCount() > 0) {
                    lineCount += batch.lineCountNoHeader();
                }
                appendToFiledList(fileListWriter, batch);
                ++fileCount;
            }
        } catch (IOException ioex) {
            throw new UnrecoverableException(ioex);
        }

        logger.info("* Processed {} files. {} lines total have been written to the concatenated file {}", fileCount, lineCount,
                concatenatedFile.toAbsolutePath());
    }

    private Eci obtainEci() {
        BatchLogDao dao = new BatchLogDao();
        return dao.selectInProcessEci();
    }

    private Path constructConcatenatedPath(Config config, FileSetInfo fileSetInfo) {
        String location = config.get("concat.file.location");
        String concatFileName = fileSetInfo.sourceSystem() + "-" + fileSetInfo.formType()
                + "-concatenated.txt";

        return Paths.get(location, concatFileName);
    }

    /*
     * File name/location: \\vaauspciapp22\ibm$\Data\out\FBCS\TEST_mergedFiles.lst Format: Date
     * DataFileName CompFileName Batch Id 2017-2-22 FBCS-TerminalStatus-UB04-R1V18-20170221.txt
     * FBCS-TerminalStatus-UB04-R1V18-C-20170221.txt R1V18_U201702211022 2017-2-22
     * FBCS-TerminalStatus-UB04-R1V18T-20170221.txt FBCS-TerminalStatus-UB04-R1V18T-C-20170221.txt
     * R1V18T_U201702211022
     */

    private Path constructFileListPath(Config config, FileSetInfo fileSetInfo) {
        String location = config.get("file.lists.location");
        String filename = fileSetInfo.sourceSystem() + fileSetInfo.processingCode() + "_mergedFiles.lst";

        return Paths.get(location, fileSetInfo.sourceSystem(), filename);
    }

    private void appendToFiledList(BufferedWriter mergedListWriter, BatchTracker batch) {
        StringBuilder line = new StringBuilder();

        line.append(DateTimeFormatter.ISO_LOCAL_DATE.format(batch.startTimestamp()));
        line.append(" ").append(batch.fileInfo().file().getName());
        line.append(" ").append(batch.fileInfo().companionFile().getName());
        line.append(" ").append(batch.batchId());

        try {
            mergedListWriter.write(line.toString());
            mergedListWriter.newLine();
        } catch (IOException e) {
            throw new UnrecoverableException(e);
        }

    }

    private void copyCompanionFiles(List<File> files, File targetDir) {
        FilesUtils.copyFiles(files, targetDir);
    }

    private BatchTracker processFile(FilePatternConfig processingConfig, File inputFile,
                                     File targetDir, Eci eci, BufferedWriter concatWriter,
                                     LocalDateTime startTimestamp) {
        File outputFile = new File(targetDir, inputFile.getName());
        logger.info("Processing file '{}' ... ", inputFile.getName());
        FbcsFileProcessor fileProcessor = new FbcsFileProcessor(config);
        return fileProcessor.processFile(processingConfig, inputFile, outputFile, eci, concatWriter,
                startTimestamp);
    }

    private List<File> findInputFiles(File inputDir, String includeFilePattern,
                                      String excludeFilePattern) {
        return FilesUtils.findFiles(inputDir, includeFilePattern, excludeFilePattern);
    }

}