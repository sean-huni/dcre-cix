package za.co.fnb.dcre.cix.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.cix.CrwSourceTables;
import za.co.fnb.dcre.cix.data.repo.IsrRespRepo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-42 sliced-ingest proofs against a real CRDB. The 300k-row ISR sweep
 * killed CIX (exit 5): one giant serializable tx is unrefreshable
 * (RETRY_SERIALIZABLE "can't refresh txn spans") and the step-level retry
 * just re-runs the same doomed transaction. Upserts must commit in bounded
 * slices: (1) a slice that exhausts its retry budget fails the run WITHOUT
 * rolling back slices already committed; (2) a transient 40001 abort on a
 * slice is retried in a fresh tx and succeeds; (3) a re-run over committed
 * slices no-ops (row identity preserved via ON CONFLICT (response_file, e2e),
 * no dup rows). ingest() runs inside an outer REQUIRED tx exactly like the
 * Batch step tx.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
class ReaderServiceSliceTest {

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

    static final int SLICE_SIZE = 2;

    @Autowired
    IsrRespRepo repo;

    @Autowired
    PlatformTransactionManager txManager;

    @Autowired
    JdbcTemplate jdbc;

    String responseFile;
    String reply;
    TransactionTemplate stepTx;

    @BeforeEach
    void newReply() {
        // SCRUM-55: ingest resolves OrgnlMsgId against the CRW-owned registry tables.
        CrwSourceTables.bootstrap(jdbc);
        String unique = UUID.randomUUID().toString().substring(0, 8);
        responseFile = "20260714_FNB_ISR_" + unique + "_RESP.xml";
        reply = reply(6);
        stepTx = new TransactionTemplate(txManager);
    }

    @Test
    void sliceFailureDoesNotRollBackCommittedSlices() {
        ReaderService service = service(failingRepo(tx -> tx == 5, Integer.MAX_VALUE));

        assertThrows(TransientDataAccessException.class,
                () -> stepTx.executeWithoutResult(status -> service.ingest(reply, responseFile)),
                "a slice that exhausts its retry budget must fail the run");

        assertEquals(4, rowCount(),
                "slices (1,2) and (3,4) committed before the failing slice must stand");
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM isr_resp WHERE response_file=?"
                        + " AND e2e IN ('E2E-5','E2E-6')", Integer.class, responseFile),
                "nothing from the failed slice may leak");
    }

    @Test
    void transientAbortOnSliceRetriesInFreshTxThenSucceeds() {
        ReaderService service = service(failingRepo(tx -> tx == 3, 2));

        int rows = stepTx.execute(status -> service.ingest(reply, responseFile));

        assertEquals(6, rows, "all six Tx verdicts ingested");
        assertEquals(6, rowCount(), "retried slice committed after transient 40001 aborts");
        assertEquals("RJCT", jdbc.queryForObject("SELECT status FROM isr_resp WHERE response_file=?"
                + " AND e2e='E2E-3'", String.class, responseFile), "retried slice wrote its content");
    }

    @Test
    void sliceRerunNoOpsOverCommittedSlices() {
        assertThrows(TransientDataAccessException.class, () -> stepTx.executeWithoutResult(
                status -> service(failingRepo(tx -> tx == 5, Integer.MAX_VALUE)).ingest(reply, responseFile)));
        assertEquals(4, rowCount(), "restart precondition: four rows committed");
        List<UUID> committedIds = jdbc.queryForList(
                "SELECT id FROM isr_resp WHERE response_file=? ORDER BY e2e", UUID.class, responseFile);

        int rerun = stepTx.execute(status -> service(repo).ingest(reply, responseFile));

        assertEquals(6, rerun, "restart re-reads the whole file and resumes the failed slice");
        assertEquals(6, rowCount(), "one row per Tx block, nothing else (no dup rows)");
        List<UUID> after = jdbc.queryForList(
                "SELECT id FROM isr_resp WHERE response_file=? ORDER BY e2e", UUID.class, responseFile);
        assertTrue(after.containsAll(committedIds),
                "committed rows keep their identity (ON CONFLICT updates, no delete+reinsert)");
        assertEquals("RJCT", jdbc.queryForObject("SELECT status FROM isr_resp WHERE response_file=?"
                + " AND e2e='E2E-3'", String.class, responseFile), "verdict content survives the re-run");
    }

    private ReaderService service(final IsrRespRepo respRepo) {
        return new ReaderService(respRepo, txManager, SLICE_SIZE);
    }

    /** Delegates to the real repo; throws a CRDB-shaped 40001 for matching Tx numbers, {@code failures} times. */
    private IsrRespRepo failingRepo(final IntPredicate failTx, final int failures) {
        AtomicInteger thrown = new AtomicInteger();
        // upsert args since SCRUM-55: (responseFile, orgnlMsgId, emissionId, e2e, status, reason)
        return (IsrRespRepo) Proxy.newProxyInstance(IsrRespRepo.class.getClassLoader(),
                new Class<?>[]{IsrRespRepo.class}, (proxy, method, args) -> {
                    if ("upsert".equals(method.getName())
                            && failTx.test(Integer.parseInt(((String) args[3]).substring(4)))
                            && thrown.getAndIncrement() < failures) {
                        throw new CannotAcquireLockException("ERROR: restart transaction:"
                                + " TransactionRetryWithProtoRefreshError: RETRY_SERIALIZABLE"
                                + " - failed preemptive refresh: can't refresh txn spans; not valid");
                    }
                    try {
                        return method.invoke(repo, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    // [SYNTHETIC-CONTRACT R-35] reply shape; Tx 3 is the RJCT verdict.
    private String reply(final int txCount) {
        StringBuilder sb = new StringBuilder("<Document>\n  <OrgnlMsgId>MSG-")
                .append(responseFile, 18, 26).append("</OrgnlMsgId>\n");
        for (int tx = 1; tx <= txCount; tx++) {
            sb.append("  <Tx><OrgnlEndToEndId>E2E-").append(tx).append("</OrgnlEndToEndId>");
            sb.append(tx == 3 ? "<TxSts>RJCT</TxSts><Rsn>AC04</Rsn>" : "<TxSts>ACSC</TxSts>");
            sb.append("</Tx>\n");
        }
        return sb.append("</Document>\n").toString();
    }

    private int rowCount() {
        return jdbc.queryForObject("SELECT count(*) FROM isr_resp WHERE response_file=?",
                Integer.class, responseFile);
    }
}
