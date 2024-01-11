package pit.etl.filecopytransform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.config.Config;
import pit.etl.config.FilePatternConfig;
import pit.etl.filecopytransform.fbcs.FbcsFileSetProcessor;
import pit.util.EmailSender;

import java.io.File;
import java.util.Arrays;

public class FileProcessorMain {
    private static final Logger logger = LoggerFactory.getLogger(FileProcessorMain.class);

    // TODO: save the original files
    public static void main(String[] args) {
        String usage = "source_dir dest_dir file_mask";

        logger.info("** Starting the ETL file processor with the following command line parameters: {}",
                Arrays.asList(args));

        int expectedNumberOfParam = 3;
        if (args.length < expectedNumberOfParam) {
            throw new IllegalArgumentException("You must provide at least " + expectedNumberOfParam
                    + " parameters. Usage:\n" + usage);
        }

        int i = 0;
        File sourceDir = new File(args[i++]);
        File destDir = new File(args[i++]);
        validateExists(sourceDir);
        validateExists(destDir);
        String fileMask = args[i];
        Config config = Config.load();

        try {
            long startTime = System.nanoTime();
            processFiles(config, sourceDir, destDir, fileMask);
            long elapsedTime = System.nanoTime() - startTime;
            logger.info("** ETL file processor completed in {} ms", elapsedTime / 1000 / 1000);
        } catch (Exception e) {

            logger.error("Preprocessor/postprocessor failed", e);
            EmailSender emailSender = new EmailSender(config);
            emailSender.sendErrorNotification("Preprocessor failed. Params:" + Arrays.asList(args), e);
            throw new UnrecoverableException(e);
        }
    }

    private static void validateExists(File dir) {
        if (!dir.exists())
            throw new IllegalArgumentException(
                    "Directory " + dir.getAbsolutePath() + " does not exist");

    }

    private static void processFiles(Config config, File sourceDir, File destDir, String filePattern) {

        // FBCS files must have the configuration defined
        FilePatternConfig filePatternConfig = FilePatternConfig.findMatchingConfig(config, filePattern);
        if (filePatternConfig != null) {

            FbcsFileSetProcessor processor = new FbcsFileSetProcessor(config);
            processor.process(filePatternConfig, sourceDir, filePattern, destDir);

        } else { // no modifications necessary, copy as is
            logger.info("Files matching the pattern '{}' will be copied without changes", filePattern);
            FilesUtils.copyFiles(sourceDir, filePattern, destDir);
        }
    }
}
