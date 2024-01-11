package pit.etl.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jdom2.Document;
import org.jdom2.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pit.util.FilePathUtils;
import pit.util.JDomUtils;
import pit.util.XMLParsingUtils;

public class FilesConfig {
    private final Logger logger = LoggerFactory.getLogger(getClass());


    public static FilesConfig loadConfig(String fileName) {
        File configFile = new File(FilePathUtils.getAbsoluteFilePath(fileName));

        return new FilesConfig(configFile);
    }

    private Document configDoc;

    public FilesConfig(File configFile) {
        configDoc = XMLParsingUtils.parseFile(configFile);
        logger.info("Loaded file processing configuration file from " + configFile.getPath());
    }

    public final static String FILE_CONFIG_NAME = "file-config";
    public final static String PATTERN_NAME = "pattern";


    public FilePatternConfig findFilePatternConfigForPattern(String pattern) {
        List<Element> filePatternElts = configDoc.getRootElement().getChildren(FILE_CONFIG_NAME);
        List<Element> matchedElts = new ArrayList<>();

        for (Element objectConfigElt : filePatternElts) {
            String fileConfigPattern = JDomUtils.getRequiredChildElementValue(objectConfigElt, PATTERN_NAME);

            if (matches(pattern, fileConfigPattern)) {
                matchedElts.add(objectConfigElt);
                logger.info("Found configuration for the file mask '{}', pattern in files-config: {}", pattern, fileConfigPattern);
                break;
            }
        }

        if (matchedElts.isEmpty())
            return null;

        FilePatternConfig filePatternConfig = new FilePatternConfig(matchedElts);

        return filePatternConfig;
    }



    private boolean matches(String stringToMatch, String pattern) {

        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(stringToMatch);

        return matcher.matches();
    }




}
