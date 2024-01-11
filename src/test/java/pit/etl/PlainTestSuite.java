package pit.etl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import pit.etl.dbloader.EndToEndTest;
import pit.etl.filecopytransform.fbcs.DupeRejectTest;
import pit.etl.filecopytransform.fbcs.FileCopyTransformTest;
import pit.etl.filecopytransform.fbcs.FileInfoTest;
import pit.etl.filecopytransform.fbcs.LineValidationTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        FileInfoTest.class,
        FileCopyTransformTest.class,
        DupeRejectTest.class,
        LineValidationTest.class,
        EndToEndTest.class
})

public class PlainTestSuite {
}