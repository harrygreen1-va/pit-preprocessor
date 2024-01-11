package pit.etl.config;

import org.apache.commons.lang3.StringUtils;
import org.jdom2.Element;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class FilePatternConfig {


    private final List<Element> configElts;

    FilePatternConfig(List<Element> configElts) {
        this.configElts = configElts;
    }

    public static FilePatternConfig findMatchingConfig(Config config, String toMatch) {
        String patternConfigFile = config.get("pattern.config.file");
        FilesConfig filesConfig = FilesConfig.loadConfig(patternConfigFile);

        return filesConfig.findFilePatternConfigForPattern(toMatch);
    }

    private final static String NUMBER_OF_COLUMNS_ELT_NAME = "columns-number";
    private final static String NUMBER_OF_COLUMNS_LOADER_ELT_NAME = "loader-columns-number";
    private final static String APPEND_STRING_ELT_NAME = "append-string";
    private final static String APPEND_STRING_HEADER_ELT_NAME = "append-string-header";
    private final static String POSTPROCESSING_ELT_NAME = "postprocessing";

    public int getNumberOfColumns() {
        return getNumberOrMinusOne(NUMBER_OF_COLUMNS_ELT_NAME);
    }

    public int getNumberOfColumnsForLoader() {
        return getNumberOrMinusOne(NUMBER_OF_COLUMNS_LOADER_ELT_NAME);
    }

    private int getNumberOrMinusOne(String eltName) {
        String numberOfColsS = getElementValue(eltName);
        return (StringUtils.isNotBlank(numberOfColsS) ? Integer.parseInt(numberOfColsS) : -1);
    }

    public String getPattern() {
        return getElementValue("pattern");
    }

    public String getStringToAppend() {
        return getElementValue(APPEND_STRING_ELT_NAME);
    }

    public String getStringToAppendToHeader() {
        return getElementValue(APPEND_STRING_HEADER_ELT_NAME);
    }

    public List<String> getStringList(String name) {
        String strList = getElementValue(name);

        if (strList == null)
            return new ArrayList<>();
        String[] strs = StringUtils.splitByWholeSeparator(strList, ",");
        List<String> list = new ArrayList<>();
        for (String str : strs) {
            list.add(str.trim());
        }
        return list;
    }

    private String getElementValue(String eltName) {
        String val = null;

        for (Element configElt : configElts) {
            Element elt = configElt.getChild(eltName);
            if (elt != null) {
                val = elt.getTextTrim();
                if (val != null)
                    break;
            }
        }

        return val;
    }

    public Set<String> getPostprocessingJobs() {
        return new LinkedHashSet<>(getStringList(POSTPROCESSING_ELT_NAME));
    }

    public String getConformantValidationConfig() {
        return getElementValue("conformant-validation");
    }


    @Override
    public String toString() {
        return "String to append: '" + getStringToAppend() + "' String to append to header: '"
                + getStringToAppendToHeader() + "' Number of columns: " + getNumberOfColumns();
    }

}