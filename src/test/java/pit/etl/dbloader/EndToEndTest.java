package pit.etl.dbloader;

import lombok.SneakyThrows;
import org.apache.commons.io.FilenameUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.FileEtlTest;
import pit.etl.config.Config;
import pit.etl.filecopytransform.fbcs.DelimitedFileInfo;
import pit.etl.filecopytransform.fbcs.LineColumnCounter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EndToEndTest implements FileEtlTest {


    @SuppressWarnings("FieldCanBeLocal")
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    
    @BeforeClass
    public static void setupGlobal() {
        System.setProperty("etl_log_home", "./logs");
        System.setProperty(Config.DISABLE_LOADING_SYSPROP, "true");
    }


    @Test
    public void ccrsTerminalMissingFields() {
        File ccrsDirPITDEV2445 = new File(ccrsDir, "PITDEV-2445"); // HCFA
        ccrsTerminalAllFiles(ccrsDirPITDEV2445);
        File ccrsDirPITDEV2446 = new File(ccrsDir, "PITDEV-2446"); // UB
        ccrsTerminalAllFiles(ccrsDirPITDEV2446);
        File ccrsDirPITDEV2447 = new File(ccrsDir, "PITDEV-2447"); // DENT
        ccrsTerminalAllFiles(ccrsDirPITDEV2447);
    }

    private void ccrsTerminalAllFiles(File ccrsDirPITDEV2445) {
        File[] listOfFiles = ccrsDirPITDEV2445.listFiles();

        if (listOfFiles != null) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    System.out.println(file.getName()); // This prints the file name
                    ccrsTerminalHelper(file.getName(), ccrsDirPITDEV2445);
                }
            }
        } else {
            System.out.println("The specified directory does not exist or is not a directory.");
        }
    }

    @SneakyThrows
    private void ccrsTerminalHelper(String fileMask, File ccrsDirPITDEV2445) {
        // fileMask example
        // "CCRS-TerminalStatus-DENT-CCRS-20231202-Box1A-ICN-missing.txt";
        runPreprocessor(ccrsDirPITDEV2445, fileMask);

        String sourceFileName = FilenameUtils.removeExtension(fileMask);
        String rejectFileName = sourceFileName + "-Rejects.txt";
        File rejectsDir = new File(ccrsDirPITDEV2445, DelimitedFileInfo.REJECTS_DIR_NAME);
        File rejectsFile = new File(rejectsDir, rejectFileName);
        if(false && !rejectsFile.exists()) {
            logger.error(">>>>>> file does not exist: skipping " + rejectFileName + " rejectDir " + rejectsDir.toString());
            return;
        }
        assertTrue(rejectsFile.exists());

        var rejectsCount = LineColumnCounter.count(rejectsFile);

        if(false && rejectsCount.lineCount > 2) {
            logger.error(">>>>>>> " + rejectFileName + " line count > 3");
            return;
        }
        assertEquals(2, rejectsCount.lineCount);

        List<String> lines = Files.readAllLines(Paths.get(rejectsDir.toString(), rejectFileName));
        String fileContent = String.join("\n", lines);
        String missingFieldName = missingFieldNameHelper(fileMask);

        // might help one day - logger.error("missingFieldName: " + missingFieldName + " -- fileContent: " + fileContent);

        // if (missingFieldName) contains "-" then split into two strings and compare both
        if (missingFieldName.contains("-")) {
            assertTrue(fileContent.contains(missingFieldName.split("-")[0])
                    || fileContent.contains(missingFieldName.split("-")[1]));
        } else {
            assertTrue(fileContent.contains(missingFieldName));
        }

    }

    private String missingFieldNameHelper(String fileName) {
        final String regex = ".*2023.*-(.*?)-missing.txt";
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(fileName);
        // System.out.println(fileName + " " + regex);
        // return matcher.group();
        String match = "";
        while (matcher.find()) {
            // System.out.println("Full match: " + matcher.group(0));
            for (int i = 1; i <= matcher.groupCount(); i++) {
                // System.out.println("Group " + i + ": " + matcher.group(i));
                match = matcher.group(i);

            }
        }
        if (match.isEmpty()) {
            throw new UnrecoverableException("unable to find missing field name");
        } else {
            return match;
        }
    }

    @Ignore ("Place holder should you need to test an individual file")
    @Test
    public void ccrsTerminalHcfa() {
        File ccrsDirPITDEV2445 = new File(ccrsDir, "PITDEV-2446");
        String fileMask = "CCRS-TerminalStatus-UB04-CCRS-20231210-Box60A-ICN-missing.txt";
        runPreprocessor(ccrsDirPITDEV2445, fileMask);
    }

    @Test
    public void vacsTerminalHcfa() {
        String fileMask = "VACS-TerminalStatus-HCFA-VACDB-20231117.txt";
        runPreprocessor(vacsDir, fileMask);
    }

    @Test
    public void ecamsDent() {
        String fileMask = "CCNN-TerminalStatus-DENT-CCNNC-20230306-1.txt";
        runPreprocessor(ccnnDir, fileMask);
    }
}