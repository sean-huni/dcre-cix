package za.co.fnb.dcre.cix.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.cix.CrwSourceTables;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * SCRUM-55 batch correlation proofs against a real CRDB. A reply's OrgnlMsgId
 * resolves once per file to the CRW emission batch (unique
 * crw_emission.outbound_msg_id); verdicts whose e2e is not a member of that
 * batch are skipped fail-closed (WARN reason=FOREIGN_E2E, never ingested); an
 * unknown OrgnlMsgId ingests fail-open with emission_id NULL plus one
 * file-level WARN reason=UNKNOWN_OUTBOUND_MSG (statuses are still truth even
 * if CRW's registry is behind). CRW owns the crw_* DDL, so the suite
 * bootstraps lookalike tables via plain JDBC ({@link CrwSourceTables}).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
class BatchCorrelationIT {

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

    @Autowired
    ReaderService service;

    @Autowired
    PlatformTransactionManager txManager;

    @Autowired
    JdbcTemplate jdbc;

    TransactionTemplate stepTx;
    ListAppender<ILoggingEvent> logEvents;

    @BeforeEach
    void setUp() {
        CrwSourceTables.bootstrap(jdbc);
        stepTx = new TransactionTemplate(txManager);
        logEvents = new ListAppender<>();
        logEvents.start();
        readerLogger().addAppender(logEvents);
    }

    @AfterEach
    void tearDown() {
        readerLogger().detachAppender(logEvents);
    }

    @Test
    void resolvesBatchAndSkipsForeignE2eFailClosed() {
        String outbound = "DCRERF2026071600000021_2";
        String responseFile = "20260716_FNB_ISR_" + UUID.randomUUID().toString().substring(0, 8) + "_RESP.xml";
        UUID emissionId = seedEmission(outbound, 2, "FNBRF01_DCRERF2026071600000021_2_PAIN008.xml");
        seedMember(emissionId, 1, "E2E-A");
        seedMember(emissionId, 2, "E2E-B");
        seedMember(emissionId, 3, "E2E-C");
        // [SYNTHETIC-CONTRACT R-35] reply shape: A and B are members, Z is foreign.
        String reply = """
                <Document>
                  <OrgnlMsgId>%s</OrgnlMsgId>
                  <Tx><OrgnlEndToEndId>E2E-A</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
                  <Tx><OrgnlEndToEndId>E2E-B</OrgnlEndToEndId><TxSts>RJCT</TxSts><Rsn>AC04</Rsn></Tx>
                  <Tx><OrgnlEndToEndId>E2E-Z</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
                </Document>
                """.formatted(outbound);

        int rows = stepTx.execute(status -> service.ingest(reply, responseFile));

        assertEquals(2, rows, "the foreign verdict must not count as ingested");
        assertEquals(emissionId, emissionIdOf(responseFile, "E2E-A"), "member verdict correlates to the batch");
        assertEquals(emissionId, emissionIdOf(responseFile, "E2E-B"), "member verdict correlates to the batch");
        assertEquals("RJCT", jdbc.queryForObject("SELECT status FROM isr_resp WHERE response_file=?"
                + " AND e2e='E2E-B'", String.class, responseFile), "verdict content ingested unchanged");
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM isr_resp WHERE response_file=?"
                        + " AND e2e='E2E-Z'", Integer.class, responseFile),
                "foreign e2e must NEVER be ingested (fail-closed correlation)");
        assertEquals(1, warns("reason=FOREIGN_E2E", "e2e=E2E-Z").size(),
                "exactly one exclusion WARN for the foreign e2e");
        assertEquals(0, warns("reason=UNKNOWN_OUTBOUND_MSG").size(),
                "a resolved OrgnlMsgId must not warn as unknown");
    }

    @Test
    void unknownOutboundMsgIngestsFailOpenWithNullBatch() {
        String responseFile = "20260716_FNB_ISR_" + UUID.randomUUID().toString().substring(0, 8) + "_RESP.xml";
        String reply = """
                <Document>
                  <OrgnlMsgId>UNKNOWN-MSG-77</OrgnlMsgId>
                  <Tx><OrgnlEndToEndId>E2E-U1</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
                  <Tx><OrgnlEndToEndId>E2E-U2</OrgnlEndToEndId><TxSts>ACSC</TxSts></Tx>
                </Document>
                """;

        int rows = stepTx.execute(status -> service.ingest(reply, responseFile));

        assertEquals(2, rows, "fail-open ingest: statuses are truth even if CRW's registry is behind");
        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM isr_resp WHERE response_file=?",
                Integer.class, responseFile));
        assertNull(jdbc.queryForObject("SELECT emission_id FROM isr_resp WHERE response_file=?"
                + " AND e2e='E2E-U1'", UUID.class, responseFile), "no batch to correlate: emission_id NULL");
        assertEquals(1, warns("reason=UNKNOWN_OUTBOUND_MSG").size(),
                "exactly ONE file-level WARN, not one per verdict");
        assertEquals(0, warns("reason=FOREIGN_E2E").size(),
                "no membership check possible without a resolved batch");
    }

    private UUID emissionIdOf(final String responseFile, final String e2e) {
        return jdbc.queryForObject("SELECT emission_id FROM isr_resp WHERE response_file=? AND e2e=?",
                UUID.class, responseFile, e2e);
    }

    private List<ILoggingEvent> warns(final String... needles) {
        return logEvents.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> {
                    String message = event.getFormattedMessage();
                    for (final String needle : needles) {
                        if (!message.contains(needle)) {
                            return false;
                        }
                    }
                    return true;
                })
                .toList();
    }

    private UUID seedEmission(final String outboundMsgId, final int batchOrdinal, final String fileName) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                        INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state,
                                                  batch_ordinal, outbound_msg_id)
                        VALUES (?, ?, ?, ?, 'VISIBLE', ?, ?)""",
                id, UUID.randomUUID(), LocalDate.of(2026, 7, 16), fileName, batchOrdinal, outboundMsgId);
        return id;
    }

    private void seedMember(final UUID emissionId, final int sequence, final String e2e) {
        jdbc.update("INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount) VALUES (?, ?, ?, 10.00)",
                emissionId, sequence, e2e);
    }

    private static Logger readerLogger() {
        return (Logger) LoggerFactory.getLogger(ReaderService.class);
    }
}
