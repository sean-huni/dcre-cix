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

    private final ReaderService service;

    public ReaderTasklet(ReaderService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        String fileText = Files.readString(Path.of((String) params.get("input.file")));
        int rows = service.ingest(fileText, (String) params.get("original.name"));
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putInt("rows", rows);
        return RepeatStatus.FINISHED;
    }
}
