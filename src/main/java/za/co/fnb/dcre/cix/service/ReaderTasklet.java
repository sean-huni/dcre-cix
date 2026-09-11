package za.co.fnb.dcre.cix.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;

/** Thin entry adapter (3-tier, configuration.md point 21): no SQL, no parsing. */
@Component
public class ReaderTasklet implements Tasklet {

    /** The launch parameter naming the file to ingest; AGT supplies it on every launch. */
    static final String INPUT_FILE = "input.file";

    private final ReaderService service;

    public ReaderTasklet(ReaderService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        String fileText = Files.readString(
                Path.of(requiredInputFile((String) params.get(INPUT_FILE))));
        int rows = service.ingest(fileText, (String) params.get("original.name"));
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putInt("rows", rows);
        return RepeatStatus.FINISHED;
    }

    /**
     * Names the missing parameter, and this stage, before the value can reach {@link Path#of}.
     * Unguarded, a launch without {@code input.file} dies inside the JDK's filesystem code with a
     * NullPointerException that names neither the parameter, nor this stage, nor the job, so the
     * operator reading that log learns nothing about what to supply. Blank counts as missing: a
     * blank string builds an empty path with no NullPointerException at all and then fails
     * somewhere else entirely, which is the quieter half of the same defect.
     */
    private static String requiredInputFile(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("CIX requires the job parameter '" + INPUT_FILE
                    + "': it was " + (value == null ? "not supplied" : "blank ('" + value + "')")
                    + ". Launch the job with " + INPUT_FILE + "=<path to the file to ingest>.");
        }
        return value;
    }
}
