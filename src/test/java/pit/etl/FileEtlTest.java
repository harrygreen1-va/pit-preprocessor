package pit.etl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import pit.UnrecoverableException;
import pit.etl.config.Config;
import pit.etl.filecopytransform.FileProcessorMain;
import pit.etl.filecopytransform.fbcs.FileProcessorTestUtils;
import pit.etl.postprocessor.PostprocessorMain;

import java.io.File;
import java.io.IOException;

public interface FileEtlTest {
    File testFilesDir = new File("./test_files");
    File inDir = new File(testFilesDir, "in");

    File fbcsInDir = new File(inDir, "FBCS");
    File ccnnDir = new File(inDir, "CCNN");
    File vacsDir = new File(inDir, "VACS");
    File ccrsDir = new File(inDir, "CCRS");
    File outDir = new File(testFilesDir, "out");
    File testDir = new File(inDir, "testing");

    default void runPreprocessor(File inDir, String fileMask) {
        var env = System.getProperty(Config.ENV_SYSTEM_PROP);
        if (StringUtils.isBlank(env)) {
            env = FileProcessorTestUtils.getEnv();
            System.setProperty(Config.ENV_SYSTEM_PROP, env);
        }

        if (outDir.exists()) {
            try {
                FileUtils.forceDelete(outDir);
            } catch (IOException e) {
                throw new UnrecoverableException(e);
            }
        }
        //noinspection ResultOfMethodCallIgnored
        outDir.mkdir();
        String[] args = new String[]{inDir.getAbsolutePath(), outDir.getAbsolutePath(), fileMask};
        FileProcessorMain.main(args);
    }

    default void runPostprocessor(String... args) {
        PostprocessorMain.main(args);
    }
}