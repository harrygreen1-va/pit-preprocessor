package pit.etl.filecopytransform.fbcs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.dbloader.validation.ProcessingStats;
import pit.etl.filecopytransform.LineProcessor;

import static pit.etl.dbloader.validation.SourceDataValidator.validationLogger;

public class ColumnNormalizer implements LineProcessor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String delim = DelimitedFileInfo.DELIM;

    private int numberOfColumnsInHeader;
    private int targetNumberOfColumns;

    private String stringToAppend;
    private String stringToAppendToHeader;

    private ProcessingStats stats;

    public ColumnNormalizer(int targetNumberOfColumns) {
        super();
        this.targetNumberOfColumns = targetNumberOfColumns;
    }

    public ColumnNormalizer(int targetNumberOfColumns, String stringToAppend,
                            String stringToAppendToHeader, ProcessingStats stats) {
        this(targetNumberOfColumns);
        this.stringToAppend = stringToAppend;
        this.stringToAppendToHeader = stringToAppendToHeader;
        if (stats!=null) {
            this.stats = stats;
        }
    }


    @Override
    public String processLine(String inputString, int lineNumber) {
        if (targetNumberOfColumns < 0) {
            return inputString;
        }

        inputString = StringUtils.trim(inputString);
        int numberOfColumns = StringUtils.countMatches(inputString, delim) + 1;
        if (lineNumber == 0) {
            numberOfColumnsInHeader = numberOfColumns;
            logger.info("Number of columns in the header before any changes: {}, target number of columns: {}"
                    , numberOfColumnsInHeader, targetNumberOfColumns);
        }

        if (numberOfColumns != numberOfColumnsInHeader && stats!=null) {
            validationLogger.warn(String.format("%s:%d: The number of columns (%d) does not match the number of columns in the header (%d)",
                    stats.fileName(), lineNumber, numberOfColumns, numberOfColumnsInHeader));
            stats.incr("line-header-column-mismatch");
        }

        return adjustColumns(inputString, lineNumber, numberOfColumns, targetNumberOfColumns);
    }


    private String adjustColumns(String inputStr, int lineNumber, int numOfCols, int targetNumOfCols) {
        String outputString = inputStr;

        if (targetNumOfCols > numOfCols) {

            if (stringToAppend != null)
                outputString = performAppend(inputStr, lineNumber);
            else
                outputString = addColumns(inputStr, targetNumOfCols - numOfCols);
        }
        else if (targetNumOfCols < numOfCols) {
            outputString = removeColumns(inputStr, numOfCols - targetNumOfCols);
        }

        return outputString;
    }

    private String performAppend(String inputString, int lineNumber) {
        String outputString;

        // if this is a header
        if (lineNumber == 0 && stringToAppendToHeader != null) {
            outputString = inputString + stringToAppendToHeader;
        }
        else {
            outputString = inputString + stringToAppend;
        }

        return outputString;
    }

    private String addColumns(String sourceString, int numberOfColumnsToAdd) {
        StringBuilder resultingString = new StringBuilder(sourceString);
        String sToAppend = StringUtils.repeat(delim, numberOfColumnsToAdd);
        resultingString.append(sToAppend);

        return resultingString.toString();
    }

    private String removeColumns(String sourceString, int numberOfColumnsToRemove) {
        // TODO: handle removal of a column w/o delims
        // ^^^
        int startingRemovalInd = StringUtils.lastOrdinalIndexOf(sourceString, delim,
                numberOfColumnsToRemove);
        if (startingRemovalInd < 0) {
            throw new UnrecoverableException(
                    "Was not able to cut %d columns from the string '%s', this string does not have that many columns",
                    numberOfColumnsToRemove, sourceString);
        }
        return StringUtils.substring(sourceString, 0, startingRemovalInd);
    }

}