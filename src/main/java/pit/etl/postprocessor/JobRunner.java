package pit.etl.postprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pit.UnrecoverableException;
import pit.etl.batchlog.BatchLogDao;
import pit.etl.batchlog.BatchTracker;
import pit.etl.config.Config;
import pit.etl.config.DataSourceFactory;
import pit.etl.config.FilePatternConfig;
import pit.etl.dbloader.JobInfo;
import pit.etl.setcurrent.UpdateIsCurrentOrchestrator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static pit.etl.batchlog.BatchLogDao.IN_PROCESS_STATUS;
import static pit.etl.batchlog.BatchLogDao.TERMINAL_STATUS;

class JobRunner {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Config config;

    private final Map<String, PostJob> namedJobs = new HashMap<>();
    private final BatchLogDao batchLogDao = new BatchLogDao();
    private final UpdateIsCurrentOrchestrator updateIsCurrentOrchestrator;

    JobRunner(Config config) {
        this.config = config;
        updateIsCurrentOrchestrator = new UpdateIsCurrentOrchestrator(config);
        registerJobs();
    }

    private void registerJobs() {
        UpdateKeysJob updateKeysJob = new UpdateKeysJob();
        namedJobs.put("updatekeys/ecams_hcfa_update_keys", updateKeysJob);
        namedJobs.put("updatekeys/ecams_ub_update_keys", updateKeysJob);
        namedJobs.put("updatekeys/fbcs_ub_update_keys", updateKeysJob);
        namedJobs.put("updatekeys/fbcs_hcfa_update_keys", updateKeysJob);
        namedJobs.put("updatekeys/ccrs_ncpdp_update_keys", updateKeysJob);
    }


    void runJobs(List<BatchTracker> batchesToProcess) {

        DataSourceFactory.init(config);

        // for each batch, determine jobs from file config
        for (BatchTracker batch : batchesToProcess) {
            String fileName = batch.fileInfo().file().getName();
            FilePatternConfig filePatternConfig = FilePatternConfig.findMatchingConfig(config, fileName);
            if (filePatternConfig != null) {
                batchLogDao.updateBatchEntryStatus(batch.batchId(), IN_PROCESS_STATUS);
                Set<String> jobNames = filePatternConfig.getPostprocessingJobs();
                if (jobNames.isEmpty()) {
                    logger.warn("Postprocessing jobs are not defined for file pattern {}", filePatternConfig.getPattern());
                }
                else {
                    runJobs(jobNames, batch);
                    logger.info("Completed all jobs for {}", batch.batchId());
                }
                batchLogDao.updateBatchEntryStatus(batch.batchId(), TERMINAL_STATUS);
            }
            else {
                logger.info("Didn't find file pattern definition matching {}", fileName);
            }
        }
        updateIsCurrentOrchestrator.runUpdateIsCurrent(batchesToProcess, false);
    }


    private void runJobs(Set<String> jobNames, BatchTracker batch) {
        for (String jobName : jobNames) {
            PostJob job = namedJobs.get(jobName);
            if (job == null) {
                throw new UnrecoverableException("Postprocessing job %s is not registered", jobName);
            }
            JobInfo jobInfo = Config.jobInfoFromConfig(jobName, batch);
            runJob(jobInfo, job, batch);
        }
    }


    private void runJob(JobInfo jobInfo, PostJob job, BatchTracker batch) {
        logger.info("* Running the job {} for the batch {}", jobInfo.name(), batch);
        long startTime = System.nanoTime();
        job.setEnvConfig(config);
        job.postprocess(jobInfo, batch);
        long elapsedTime = System.nanoTime() - startTime;
        logger.info("Job {} batch {} completed in {} ms", jobInfo.name(), batch.batchId(), elapsedTime / 1000 / 1000);

    }


}