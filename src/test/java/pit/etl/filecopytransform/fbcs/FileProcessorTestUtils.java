package pit.etl.filecopytransform.fbcs;

import org.apache.commons.lang3.StringUtils;
import pit.util.FilePathUtils;
import pit.util.Utils;

import java.io.File;

public class FileProcessorTestUtils {

    public static String getEnv() {
        // DJH - Currently no functionally for simulation tests, so support for 
        // another enviroment is not needed.
        String  env = "local";
        return env;
    }

}
