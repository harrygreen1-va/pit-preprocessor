package pit.etl.postprocessor;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.setcurrent.UpdateIsCurrentOrchestrator;
import pit.util.EmailSender;
import pit.util.FileListUtils;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PostprocessorMain {

    private static final Logger logger = LoggerFactory.getLogger(PostprocessorMain.class);


    public static void main(String[] args) {
        logger.info("** Starting ETL postprocessor with the following command line parameters:\n {}",
                Arrays.asList(args));
        Config config = null;
        try {
            // create the parser
            CommandLineParser parser = new DefaultParser();
            CommandLine line;
            // parse the command line arguments
            line = parser.parse(createOptions(), args);


            String env = line.getOptionValue(ENV_OPT);
            config = Config.load(env);
            StopWatch sw = StopWatch.createStarted();
            var postprocessor = new PostprocessorMain();
            if (line.hasOption(IS_CURRENT_MODE)) {
                logger.error("Is_current bulk update is disabled.");
                //postprocessor.runIsCurrent(config, line);
            }
            else {
                File fileListFile = new File(line.getOptionValue(FILE_LIST_FILE_OPT));
                if (!fileListFile.exists()) {
                    throw new UnrecoverableException("File %s does not exist", fileListFile.getAbsolutePath());
                }

                postprocessor.process(config, fileListFile);
            }
            logger.info("** ETL postprocessor completed in {}", sw);

        } catch (Exception e) {
            logger.error("Postprocessor failed", e);

            if (config != null) {
                EmailSender emailSender = new EmailSender(config);
                emailSender.sendErrorNotification("Postprocessor failed", e);
            } else {
                logger.warn("Config was not initialized properly, unable to send an email");
            }

            throw new UnrecoverableException(e);
        }

    }


    private void process(Config config, File fileListFile) {

        List<BatchTracker> batches;

        if (StringUtils.containsIgnoreCase(fileListFile.getName(), "merge")) {
            batches = obtainBatchesFromFBCSMergeFile(fileListFile);
        } else {
            batches = obtainBatchesForNonFBCSStreams(fileListFile);
        }

        if (batches.isEmpty()) {
            logger.warn("Postprocessor did not find any batches to process");
            return;
        }

        logger.info("Postprocessor will process the following batches:\n{}", batches);

        JobRunner runner = new JobRunner(config);
        runner.runJobs(batches);

    }

    private void runIsCurrent(Config config, CommandLine commandLine) {
        var fromDate = getDateFromCommandLine(commandLine, IS_CURRENT_FROM_DATE);
        var toDate = getDateFromCommandLine(commandLine, IS_CURRENT_TO_DATE);
        var isCurrentOrchestrator = new UpdateIsCurrentOrchestrator(config);
        var sourceSystemPrefix = commandLine.getOptionValue(IS_CURRENT_SOURCE_SYSTEM);
        boolean isResubmissionOnly = commandLine.hasOption(IS_RESUBMISSION_ONLY);
        isCurrentOrchestrator.runUpdateIsCurrent(fromDate, toDate, sourceSystemPrefix, isResubmissionOnly);

    }

    private LocalDate getDateFromCommandLine(CommandLine commandLine, String optionName) {
        var dateStr = commandLine.getOptionValue(optionName);
        if (StringUtils.isBlank(dateStr)) {
            throw new UnrecoverableException("Date option %s is required for running is_current job", optionName);
        }
        return LocalDate.parse(dateStr);
    }

    private static final String ENV_OPT = "e";
    private static final String FILE_LIST_FILE_OPT = "l";
    public static final String IS_CURRENT_MODE = "iscurmode";
    public static final String IS_RESUBMISSION_ONLY = "resub_only";
    public static final String IS_CURRENT_FROM_DATE = "iscur_from";
    public static final String IS_CURRENT_TO_DATE = "iscur_to";
    public static final String IS_CURRENT_SOURCE_SYSTEM = "iscur_system";

    private static Options createOptions() {
        Options options = new Options();
        Option fileList = Option.builder(FILE_LIST_FILE_OPT)
                .hasArg()
                .desc("File containing the the list of processed files in the format data file_name")
                .longOpt("fileListFile")
                .build();

        options.addOption(fileList);
        options.addOption(ENV_OPT, "env", true, "Environment to use.");

        options.addOption(IS_CURRENT_MODE, false, "Run is_current only, do not run postprocessor jobs");
        options.addOption(IS_CURRENT_FROM_DATE, true, "From date for is_current");
        options.addOption(IS_CURRENT_TO_DATE, true, "To date for is_current");
        options.addOption(IS_CURRENT_SOURCE_SYSTEM, true, "Source system for is_current");
        options.addOption(IS_RESUBMISSION_ONLY, false, "Run only resubmission update, do not run general is_current logic");


        return options;
    }

    private List<BatchTracker> obtainBatchesFromFBCSMergeFile(File fileListFile) {
        List<String> lines = FileListUtils.readLines(fileListFile);
        List<BatchTracker> batches = new ArrayList<>();
        for (String line : lines) {
            line=StringUtils.strip(line);
            if (!FileListUtils.isComment(line)) {
                String[] colVals = StringUtils.split(line);
                File fileName = new File(colVals[1]);
                String batchId = colVals[colVals.length - 1];
                BatchTracker batch = new BatchTracker(DelimitedFileInfo.fromFile(fileName), batchId);
    
                batches.add(batch);
            }
        }

        return batches;
    }


    private List<BatchTracker> obtainBatchesForNonFBCSStreams(File fileListFile) {
        // TODO: column to config
        Set<String> fileNamesToProcess = FileListUtils.extractColumnFromFile(fileListFile, 1);
        logger.info("Will process the following files:\n{}", fileNamesToProcess);

        if (fileNamesToProcess.isEmpty()) {
            logger.warn("Postprocessor did not find any file names in the list file {}", fileListFile);
        }

        BatchLogDao batchLogDao = new BatchLogDao();
        List<BatchTracker> batches = batchLogDao.selectBatchInfoFromFileNames(fileNamesToProcess);
        if (!fileNamesToProcess.isEmpty() && batches.isEmpty()) {
            throw new UnrecoverableException("Postprocessor did not find any batches to process for files %s", fileNamesToProcess);
        }

        return batches;

    }

}