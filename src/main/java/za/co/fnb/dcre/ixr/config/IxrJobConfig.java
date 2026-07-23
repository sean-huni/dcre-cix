package za.co.fnb.dcre.ixr.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.ixr.service.ReaderTasklet;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.HeartbeatWriter;
import za.co.fnb.dcre.platform.batch.OutcomeSeamListener;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;

@Configuration
public class IxrJobConfig {

    @Bean
    public Job ixrJob(JobRepository repo, PlatformTransactionManager tx, ReaderTasklet tasklet,
                      HeartbeatWriter heartbeatWriter,
                      @Value("${dcre.exchange-root}") String exchangeRoot) {
        // CRDB 40001 retry on the ingest step (the one that WRITES): reply-file
        // upserts run while heavy writers run concurrently, so commit-time
        // serialization aborts are expected. Retry, never skip (the handler
        // covers the chunk-commit boundary). The step tx is THIN (SCRUM-42):
        // all writes commit in per-slice REQUIRES_NEW transactions inside
        // ReaderService, so a step-level re-run no-ops over committed slices.
        Step readerStep = new StepBuilder("readerStep", repo).tasklet(tasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("IXR")).build();
        // SCRUM-58: shared seam listener (platform-batch). Same COMPLETED gate
        // and constant BUSINESS_ACCEPTED verdict as the retired inline record;
        // the dev fallback name upgrades to the self-describing local-ixr-<id>.
        // SCRUM-88 (M12): register the HeartbeatWriter listener explicitly (Batch 6 does not
        // auto-apply listener beans) so it stamps agt_ops liveness while this job runs, chained
        // after the outcome seam listener.
        return new JobBuilder("ixrJob", repo)
                .listener(new OutcomeSeamListener("ixr", exchangeRoot, execution -> "BUSINESS_ACCEPTED"))
                .listener(heartbeatWriter)
                .start(readerStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "IXR_BATCH_", 60);
    }
}
