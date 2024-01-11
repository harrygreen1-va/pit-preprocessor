package pit.etl.filecopytransform.fbcs;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LineColumnCounter {

    private final static Logger logger = LoggerFactory.getLogger(LineColumnCounter.class);
    private static final String delim = "^";

    public static int countLines(File... inputFiles) {
        int lineCount = 0;
        for (File file : inputFiles) {
            lineCount += count(file).lineCount;
        }

        return lineCount;

    }

    public static Counts count(File inputFile) {
        Counts counts = new Counts();
        
        int lineCounter = 0;
        Map<Integer, Object> delimCounts = new HashMap<>();

        try (FileReader fileReader = new FileReader(inputFile); 
                BufferedReader bufFileReader = new BufferedReader(fileReader);) {

            String inputLine;
            while ((inputLine = bufFileReader.readLine()) != null) {
                int numberOfColumns = StringUtils.countMatches(inputLine, delim) + 1;
                if (!delimCounts.containsKey(numberOfColumns)) {
                    // System.out.println( inputLine );
                    delimCounts.put(numberOfColumns, inputLine);
                }

                ++lineCounter;
            }

        } catch (IOException ioex) {
            throw new UnrecoverableException(ioex);

        }

        logger.info("Finished counting lines in the file {}. Number of lines: {}", inputFile.getName(),
                lineCounter);
        if (lineCounter > 0 && delimCounts.keySet().size() > 1) {
            logger.error("Different number of columns in the file:\n" + delimCounts.keySet());
            counts.differentNumberOfCols=true;
        }
        
        int resultingNumberOfCols = -1;
        if (lineCounter > 0)
            resultingNumberOfCols = delimCounts.keySet().iterator().next();

        logger.info("Finished processing the file {}. Number of columns: {} Number of lines: {}",
                inputFile.getName(), resultingNumberOfCols, lineCounter);
        
        counts.lineCount = lineCounter;
        counts.columnCount = resultingNumberOfCols;

        return counts;
    }

    public static class Counts {
        public int lineCount;
        public int columnCount;
        public boolean differentNumberOfCols;
        @Override
        public String toString() {
            return "Counts [lineCount=" + lineCount + ", columnCount=" + columnCount + "]";
        }
        
        
    }
}