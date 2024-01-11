package pit.etl.filecopytransform;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilesUtils {

    private static final Logger logger = LoggerFactory.getLogger(FilesUtils.class);

    public static void copyFiles(File sourceDir, String includePattern, File targetDir) {
        List<File> files = findFiles(sourceDir, includePattern, null);
        copyFiles(files, targetDir);
    }

    public static void copyFiles(List<File> files, File targetDir) {
        files.forEach(file -> copyFileAsIs(file, new File(targetDir, file.getName())));
        logger.info("Copied {} files to {}", files.size(), targetDir.getAbsolutePath());
    }


    public static void saveFile(File file, String s) {
        saveFile(file, List.of(s));
    }

    @SneakyThrows(IOException.class)
    public static void saveFile(File file, List<String> strings) {
        FileUtils.forceMkdirParent(file);
        try (var destWriter = createBufferedWriter(file)) {
            for (var s : strings) {
                destWriter.write(s);
                destWriter.newLine();
            }
        }
    }

    @SneakyThrows(IOException.class)
    public static BufferedWriter createBufferedWriter(File file) {
        var filePath = file.toPath();
        Files.deleteIfExists(filePath);
        return Files.newBufferedWriter(filePath, StandardOpenOption.CREATE_NEW);
    }

    public static void copyFileAsIs(File inputFile, File outputFile) {
        try {
            // TODO: Java8 Files.copy, preserve timestamp
            FileUtils.copyFile(inputFile, outputFile);
            logger.info("Copied file {} to file {}", inputFile.getAbsolutePath(),
                    outputFile.getAbsolutePath());

        } catch (IOException ioex) {
            throw new UnrecoverableException(ioex);
        }
    }

    public static List<File> findFiles(File inputDir, String includeFilePattern,
                                       String excludeFilePattern) {
        List<IOFileFilter> filters = new ArrayList<>();
        if (includeFilePattern != null) {
            filters.add(createFilter(includeFilePattern));
        }
        if (excludeFilePattern != null)
            filters.add(new NotFileFilter(createFilter(excludeFilePattern)));

        AndFileFilter mainAndFilter = new AndFileFilter();
        mainAndFilter.setFileFilters(filters);

        File[] files = inputDir.listFiles((FileFilter) mainAndFilter);
        if (files != null) {
            String msg = String.format("Found %d files matching the pattern '%s'", files.length, includeFilePattern);
            if (excludeFilePattern != null) {
                msg += " with exclude pattern " + excludeFilePattern;
            }
            logger.info(msg);
        }
        else {
            files = new File[0];
        }
        return Arrays.asList(files);
    }

    private static WildcardFileFilter createFilter(String patterns) {
        return WildcardFileFilter.builder()
                .setWildcards(patterns)
                .setIoCase(IOCase.INSENSITIVE)
                .get();
    }

}