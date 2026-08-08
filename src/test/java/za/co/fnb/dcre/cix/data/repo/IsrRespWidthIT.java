package za.co.fnb.dcre.cix.data.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SCRUM-58 response_file widening (spec 1.4): a 129+ char reply name registers
 * in agt_ops (file_arrival.physical_filename is 512) then crashes the reader
 * insert on the old isr_resp VARCHAR(128). Post-widening, a 200-char name must
 * round-trip AND the replay-guard UNIQUE (response_file, e2e) must survive the
 * type change untouched.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
class IsrRespWidthIT {

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

    /** 200 chars: realistic long reply basename, over the old 128 limit. */
    static final String LONG_NAME = "20260716_FNB_ISR_" + "X".repeat(174) + "_RESP.xml";

    @Autowired
    IsrRespRepo repo;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void twoHundredCharResponseFileRoundTrips() {
        assertEquals(200, LONG_NAME.length(), "fixture must exercise exactly the 200-char case");

        repo.upsert(LONG_NAME, "MSG-W1", null, "E2E-W1", "ACSC", null);

        assertEquals(LONG_NAME, jdbc.queryForObject(
                        "SELECT response_file FROM isr_resp WHERE response_file=? AND e2e='E2E-W1'",
                        String.class, LONG_NAME),
                "the 200-char name must round-trip byte-exact through isr_resp");
    }

    @Test
    void replayGuardUniqueStillEnforcedPostWidening() {
        repo.upsert(LONG_NAME, "MSG-W2", null, "E2E-W2", "ACSC", null);
        repo.upsert(LONG_NAME, "MSG-W2", null, "E2E-W2", "RJCT", "AC04");

        assertEquals(1, jdbc.queryForObject(
                        "SELECT count(*) FROM isr_resp WHERE response_file=? AND e2e='E2E-W2'",
                        Integer.class, LONG_NAME),
                "replay converges on ONE row via ON CONFLICT (response_file, e2e)");
        assertEquals("RJCT", jdbc.queryForObject(
                        "SELECT status FROM isr_resp WHERE response_file=? AND e2e='E2E-W2'",
                        String.class, LONG_NAME),
                "conflict path updates in place");
        assertThrows(DuplicateKeyException.class, () -> jdbc.update(
                        "INSERT INTO isr_resp (id, response_file, orgnl_msg_id, e2e, status)"
                                + " VALUES (gen_random_uuid(), ?, 'MSG-W2', 'E2E-W2', 'ACSC')", LONG_NAME),
                "UNIQUE (response_file, e2e) must still reject a raw duplicate after the widening");
    }
}
