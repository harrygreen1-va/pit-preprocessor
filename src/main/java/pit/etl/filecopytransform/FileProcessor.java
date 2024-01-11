package pit.etl.filecopytransform;

import java.io.File;

public interface FileProcessor {
    int processFile(File inputFile, File outputFile );
}
