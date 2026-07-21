package za.co.fnb.dcre.ixr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
class IxrJobTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @TempDir
    static Path dir;

    @Autowired
    Job ixrJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void crwSourceTables() {
        // SCRUM-55: ingest resolves OrgnlMsgId against the CRW-owned registry tables.
        CrwSourceTables.bootstrap(jdbc);
    }

    // [SYNTHETIC-CONTRACT R-35] reply shape
    static final String REPLY = """
            <Document>
              <OrgnlMsgId>MSG-0001</OrgnlMsgId>
              <Tx><OrgnlEndToEndId>E2E-1</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
              <Tx><OrgnlEndToEndId>E2E-2</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
              <Tx><OrgnlEndToEndId>E2E-3</OrgnlEndToEndId><TxSts>RJCT</TxSts><Rsn>AC04</Rsn></Tx>
              <Tx><OrgnlEndToEndId>E2E-4</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
            </Document>
            """;

    @Test
    void ingestsReplyFileAndReplayIsNoOp() throws Exception {
        String original = "20260712_FNB_ISR_reply.xml";
        Path input = dir.resolve(original);
        Files.writeString(input, REPLY);

        JobExecution run = jobOperator.start(ixrJob, new JobParametersBuilder()
                .addString("arrival.id", UUID.randomUUID().toString(), true)
                .addString("input.file", input.toString(), false)
                .addString("original.name", original, false)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        assertEquals(4, jdbc.queryForObject(
                "SELECT count(*) FROM isr_resp WHERE response_file=?", Integer.class, original));
        assertEquals("RJCT", jdbc.queryForObject(
                "SELECT status FROM isr_resp WHERE response_file=? AND e2e='E2E-3'", String.class, original));
        assertEquals("AC04", jdbc.queryForObject(
                "SELECT reason FROM isr_resp WHERE response_file=? AND e2e='E2E-3'", String.class, original));
        assertEquals("ACSC", jdbc.queryForObject(
                "SELECT status FROM isr_resp WHERE response_file=? AND e2e='E2E-1'", String.class, original));
        assertNull(jdbc.queryForObject(
                "SELECT reason FROM isr_resp WHERE response_file=? AND e2e='E2E-1'", String.class, original));
        assertEquals("MSG-0001", jdbc.queryForObject(
                "SELECT orgnl_msg_id FROM isr_resp WHERE response_file=? AND e2e='E2E-1'", String.class, original));

        JobExecution replay = jobOperator.start(ixrJob, new JobParametersBuilder()
                .addString("arrival.id", UUID.randomUUID().toString(), true)
                .addString("input.file", input.toString(), false)
                .addString("original.name", original, false)
                .toJobParameters());
        assertEquals(BatchStatus.COMPLETED, replay.getStatus());
        assertEquals(4, jdbc.queryForObject(
                "SELECT count(*) FROM isr_resp WHERE response_file=?", Integer.class, original),
                "replay is a no-op via ON CONFLICT (response_file, e2e)");
    }

    @Test
    void seamFallbackNameIsLocalIxrExecutionIdWithoutJobNameEnv() throws Exception {
        assertNull(System.getenv("JOB_NAME"), "test contract: no JOB_NAME in the test environment");
        String original = "20260716_FNB_ISR_seam.xml";
        Path input = dir.resolve(original);
        Files.writeString(input, REPLY);

        JobExecution run = jobOperator.start(ixrJob, new JobParametersBuilder()
                .addString("arrival.id", UUID.randomUUID().toString(), true)
                .addString("input.file", input.toString(), false)
                .addString("original.name", original, false)
                .toJobParameters());

        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        Path seam = Path.of("build/test-exchange/outcomes/local-ixr-" + run.getId());
        assertTrue(Files.exists(seam),
                "SCRUM-58: the dev seam fallback must be self-describing (local-ixr-<executionId>): " + seam);
        assertEquals(List.of("BUSINESS_ACCEPTED"), Files.readAllLines(seam),
                "verdict semantics preserved byte-exact by the OutcomeSeamListener swap");
    }
}
