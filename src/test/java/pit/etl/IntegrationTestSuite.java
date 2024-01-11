package pit.etl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import pit.etl.batchlog.BatchLogDaoTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        BatchLogDaoTest.class,

//        FileLoaderTest.class,
//        EndToEndTest.class,
//        PostprocessorTest.class
})

public class IntegrationTestSuite {
}