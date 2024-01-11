package pit.etl.postprocessor;

import org.junit.Test;
import pit.etl.FileEtlTest;

import java.io.File;

public class PostprocessorTest implements FileEtlTest {

    @Test
    public void testPostprocessor(){
        File listFile = new File(ccrsDir, "mergedFiles.lst");

        runPostprocessor("-l", listFile.getAbsolutePath());
    }

    @Test
    public void testPostprocessorVacs(){
        File listFile = new File(vacsDir, "mergedFiles.lst");

        runPostprocessor("-l", listFile.getAbsolutePath());
    }

}