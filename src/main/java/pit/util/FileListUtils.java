package pit.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import pit.UnrecoverableException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileListUtils {

    public static Set<String> extractColumnFromFile(File file, int colInd) {
        Set<String> vals = new HashSet<>();
        List<String> lines = readLines(file);

        for (String line : lines) {
            String[] cols = StringUtils.split(line);
            vals.add(cols[colInd]);
        }

        return vals;
    }

    public static boolean isComment(String line) {
        line=StringUtils.strip(line);
        return StringUtils.startsWithIgnoreCase(line, "#");
    }
    
    @SuppressWarnings("unused")
    public static List<List<String>> extractColumnsFromFile(File file, int... colInd) {
        List<List<String>> rows = new ArrayList<>();
        List<String> lines = readLines(file);

        List<String> row = new ArrayList<>();
        for (String line : lines) {
            String[] colVals = StringUtils.split(line);
            for (int ind : colInd) {
                row.add(colVals[ind]);
            }
            rows.add(row);
        }

        return rows;
    }


    public static List<String> readLines(File file) {
        List<String> lines;
        try {
            lines = FileUtils.readLines(file, Charset.defaultCharset());
        } catch (IOException e) {
            throw new UnrecoverableException(e);
        }
        return lines;
    }
}
