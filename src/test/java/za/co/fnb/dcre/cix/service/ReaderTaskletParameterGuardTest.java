package za.co.fnb.dcre.cix.service;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.nio.file.NoSuchFileException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Red-proofs the {@code input.file} guard on the real tasklet, with no container and no database.
 *
 * <p>Measured on a live cluster on 2026-09-11: launched as a one-off Job without the parameter,
 * this stage migrated, ran its step, and died with a NullPointerException raised inside
 * {@code sun.nio.fs.UnixFileSystem.getPath}, naming neither the parameter, nor the stage, nor the
 * job. The two failing cases below assert the MESSAGE, not merely that something was thrown: a
 * guard asserted as "throws" passes for every reason a method can fail, the original
 * NullPointerException included.
 *
 * <p>The third case is the control that keeps the other two honest. A present value must reach the
 * filesystem exactly as it did before, so the expected outcome is {@link NoSuchFileException} from
 * the read and NOT an {@link IllegalArgumentException} from the guard. Without it, a guard that
 * rejected every value, valid ones included, would pass this class.
 */
class ReaderTaskletParameterGuardTest {

    private static void execute(final JobParameters parameters) throws Exception {
        var repository = new ResourcelessJobRepository();
        JobInstance instance = repository.createJobInstance("cixJob", parameters);
        JobExecution jobExecution =
                repository.createJobExecution(instance, parameters, new ExecutionContext());
        StepExecution stepExecution = repository.createStepExecution("readerStep", jobExecution);
        new ReaderTasklet(null).execute(new StepContribution(stepExecution),
                new ChunkContext(new StepContext(stepExecution)));
    }

    @Test
    void anAbsentInputFileNamesTheParameterAndTheStage() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("original.name", "whatever.txt").toJobParameters();

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> execute(parameters));

        assertEquals("CIX requires the job parameter 'input.file': it was not supplied."
                        + " Launch the job with input.file=<path to the file to ingest>.",
                thrown.getMessage(),
                "the message must name the parameter and the stage; the NullPointerException it"
                        + " replaces named neither");
    }

    @Test
    void aBlankInputFileIsMissingToo() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("input.file", "   ")
                .addString("original.name", "whatever.txt").toJobParameters();

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> execute(parameters));

        assertEquals("CIX requires the job parameter 'input.file': it was blank ('   ')."
                        + " Launch the job with input.file=<path to the file to ingest>.",
                thrown.getMessage(),
                "blank must be rejected by the guard: it reaches Path.of without a"
                        + " NullPointerException and then fails somewhere else entirely");
    }

    @Test
    void aSuppliedInputFileStillReachesTheFilesystemUnchanged() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("input.file", "/dcre/no/such/file/cix-guard-control.txt")
                .addString("original.name", "whatever.txt").toJobParameters();

        assertThrows(NoSuchFileException.class, () -> execute(parameters),
                "a present value must pass straight through to the read; an IllegalArgumentException"
                        + " here would mean the guard had started rejecting valid launches");
    }
}
