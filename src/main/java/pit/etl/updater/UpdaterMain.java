package pit.etl.updater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.etl.config.Config;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @since 4/1/2019
 */
public class UpdaterMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdaterMain.class);
    static final Config CONFIG = Config.load("updater-config");

    /**
     * Updates date loaded in given batches with stream name.
     *
     * @param args - environment, streamName, batchId...
     */
    public static void main(String[] args) throws InterruptedException {

        int nThreads = CONFIG.getInt("nThreads");
        int batchSize = CONFIG.getInt("batch");
        String sourceSystem = CONFIG.get("sourceSystem");

        List<String> passed = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));
        List<String> filtered = passed.stream().filter(b -> b.startsWith("CCN")).collect(Collectors.toList());
        String stream = args[0];

        LOGGER.info("parameters {} {}", stream, passed);
        if (filtered.size() > 0) {
            LOGGER.info("processing etl_batch_id {}", filtered);
        }
        else {
            LOGGER.error("No etl_batch_id has been provided in {}", passed);
            System.exit(1);
        }

        List<String> tables = CONFIG.getStringList(stream);
        LOGGER.info("tables to update {}", tables);
        LOGGER.info("using {} threads and batch size {}", nThreads, batchSize);

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);

        for (String table : tables) {
            executor.submit(new UpdateJob(CONFIG, filtered, batchSize, table, sourceSystem));
        }
        LOGGER.info("submitted all");
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.MINUTES);
        LOGGER.info("all updates finished");
    }
}