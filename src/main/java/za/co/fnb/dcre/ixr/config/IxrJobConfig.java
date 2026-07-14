package za.co.fnb.dcre.ixr.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
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
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;
import java.nio.file.Path;

@Configuration
public class IxrJobConfig {

    @Bean
    public Job ixrJob(JobRepository repo, PlatformTransactionManager tx, ReaderTasklet tasklet,
                      @Value("${dcre.exchange-root}") String exchangeRoot) {
        // CRDB 40001 retry on the ingest step (the one that WRITES): 300k rows per
        // reply file land in one step transaction while heavy writers run
        // concurrently, so commit-time serialization aborts are expected.
        // Retry, never skip (the handler covers the chunk-commit boundary).
        Step readerStep = new StepBuilder("readerStep", repo).tasklet(tasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("IXR")).build();
        return new JobBuilder("ixrJob", repo)
                .listener(new SeamListener(exchangeRoot))
                .start(readerStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "IXR_BATCH_", 60);
    }

    record SeamListener(String exchangeRoot) implements JobExecutionListener {

        @Override
        public void afterJob(JobExecution execution) {
            if (execution.getStatus() != BatchStatus.COMPLETED) {
                return;
            }
            String jobName = System.getenv().getOrDefault("JOB_NAME", "local-" + execution.getId());
            OutcomeFileWriter.write(Path.of(exchangeRoot), jobName, "BUSINESS_ACCEPTED");
        }
    }
}
