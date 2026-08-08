package za.co.fnb.dcre.cix.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.StepLocator;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import za.co.fnb.dcre.cix.service.ReaderTasklet;
import za.co.fnb.dcre.platform.batch.HeartbeatWriter;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves the shared CRDB 40001 retry handler (platform-batch) is registered on
 * the step the REAL job config builds. The only injected failure is thrown from
 * PlatformTransactionManager.doCommit, the observed production failure mode
 * ("JDBC commit; ERROR: restart transaction"): a wiring that does not cover
 * commit-time aborts cannot turn this test green.
 */
class CixJobConfigRetryTest {

    /** Fails the first {@code failures} transaction COMMITS the way JdbcTransactionManager surfaces a CRDB 40001. */
    static final class CommitFailingTxManager extends ResourcelessTransactionManager {

        private final int failures;
        private int commits;

        CommitFailingTxManager(final int failures) {
            this.failures = failures;
        }

        @Override
        protected void doCommit(final DefaultTransactionStatus status) {
            if (++commits <= failures) {
                throw new CannotAcquireLockException(
                        "JDBC commit; ERROR: restart transaction: TransactionRetryWithProtoRefreshError:"
                                + " RETRY_ASYNC_WRITE_FAILURE");
            }
            super.doCommit(status);
        }
    }

    /** Counts step-transaction runs; bypasses file IO (the seam under test is the step transaction, not parsing). */
    static final class CountingTasklet extends ReaderTasklet {

        final AtomicInteger executions = new AtomicInteger();

        CountingTasklet() {
            super(null);
        }

        @Override
        public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) {
            executions.incrementAndGet();
            return RepeatStatus.FINISHED;
        }
    }

    @Test
    void readerStepRetriesCommitTimeCrdbAborts() throws Exception {
        var repo = new ResourcelessJobRepository();
        var tasklet = new CountingTasklet();
        // SCRUM-88: the job now also registers a HeartbeatWriter listener; this unit test drives
        // the STEP directly (never the job lifecycle), so a no-op writer (null datasource, no
        // JOB_NAME) satisfies the signature without touching the retry behaviour under test.
        Job job = new CixJobConfig().cixJob(repo, new CommitFailingTxManager(2), tasklet,
                new HeartbeatWriter(null, null, null), "unused-exchange-root");
        Step readerStep = ((StepLocator) job).getStep("readerStep");

        JobInstance instance = repo.createJobInstance("retryJob", new JobParameters());
        JobExecution jobExecution = repo.createJobExecution(instance, new JobParameters(), new ExecutionContext());
        StepExecution stepExecution = repo.createStepExecution("readerStep", jobExecution);
        readerStep.execute(stepExecution);

        assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus(),
                "two commit-time 40001 aborts must be retried, not fail the ingest step");
        assertEquals(3, tasklet.executions.get(), "tasklet transaction re-runs once per aborted commit");
    }
}
