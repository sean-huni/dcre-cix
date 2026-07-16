package za.co.fnb.dcre.ixr.bdd;

import io.cucumber.java.Before;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.ixr.CrwSourceTables;

/** Same bootstrap as IxrJobTest: launcher disabled, exchange root under build/, static CRDB container. */
@CucumberContextConfiguration
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
public class CucumberSpringConfig {

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
    JdbcTemplate jdbc;

    @Before
    public void crwSourceTables() {
        // SCRUM-55: ingest resolves OrgnlMsgId against the CRW-owned registry tables.
        CrwSourceTables.bootstrap(jdbc);
    }
}
