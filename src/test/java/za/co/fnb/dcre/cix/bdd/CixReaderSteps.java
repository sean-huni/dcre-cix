package za.co.fnb.dcre.cix.bdd;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Glue for the ISR reply-reader features. State is per scenario (cucumber-spring
 * scenario scope); DB isolation between scenarios comes from unique reply file names.
 *
 * [SYNTHETIC-CONTRACT R-35] reply shape: one OrgnlMsgId element, then repeated
 * Tx blocks of OrgnlEndToEndId + TxSts with an optional Rsn.
 */
public class CixReaderSteps {

    @Autowired
    Job cixJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    private String responseFile;
    private String orgnlMsgId;
    private boolean omitOrgnlMsgId;
    private final List<String> txBlocks = new ArrayList<>();
    private JobExecution lastRun;

    @Given("an ISR reply file {string} answering original message {string}")
    public void replyFile(String fileName, String msgId) {
        this.responseFile = fileName;
        this.orgnlMsgId = msgId;
    }

    @Given("a malformed ISR reply file {string} with no original message id")
    public void malformedReplyFile(String fileName) {
        this.responseFile = fileName;
        this.omitOrgnlMsgId = true;
        txBlocks.add(txBlock("E2E-BROKEN", "ACTC", null));
    }

    @Given("the reply reports transaction {string} with status {string}")
    public void replyTransaction(String e2e, String status) {
        txBlocks.add(txBlock(e2e, status, null));
    }

    @Given("the reply reports transaction {string} with status {string} and reason {string}")
    public void replyTransactionWithReason(String e2e, String status, String reason) {
        txBlocks.add(txBlock(e2e, status, reason));
    }

    @Given("the reader job has already ingested the reply file")
    public void alreadyIngested() throws Exception {
        runJob();
        assertEquals(BatchStatus.COMPLETED, lastRun.getStatus(), "first ingest must complete");
    }

    @When("the reader job ingests the reply file")
    public void ingest() throws Exception {
        runJob();
    }

    @When("the same reply file is ingested again as a new job instance")
    public void ingestAgain() throws Exception {
        runJob();
    }

    @Then("the job completes")
    public void jobCompletes() {
        assertEquals(BatchStatus.COMPLETED, lastRun.getStatus());
    }

    @Then("the job fails")
    public void jobFails() {
        assertEquals(BatchStatus.FAILED, lastRun.getStatus());
    }

    @Then("{int} verdict rows are stored for the reply file")
    public void verdictRowCount(int expected) {
        assertEquals(expected, jdbc.queryForObject(
                "SELECT count(*) FROM isr_resp WHERE response_file=?", Integer.class, responseFile));
    }

    @Then("the verdict for {string} records status {string} and original message {string}")
    public void verdictStatusAndMsgId(String e2e, String status, String msgId) {
        assertEquals(status, column("status", e2e));
        assertEquals(msgId, column("orgnl_msg_id", e2e));
    }

    @Then("the verdict for {string} records status {string} and reason {string}")
    public void verdictStatusAndReason(String e2e, String status, String reason) {
        assertEquals(status, column("status", e2e));
        assertEquals(reason, column("reason", e2e));
    }

    @Then("the verdict for {string} records no reason")
    public void verdictWithoutReason(String e2e) {
        assertNull(column("reason", e2e));
    }

    private String column(String column, String e2e) {
        return jdbc.queryForObject(
                "SELECT " + column + " FROM isr_resp WHERE response_file=? AND e2e=?",
                String.class, responseFile, e2e);
    }

    private void runJob() throws Exception {
        Path input = Files.createTempDirectory("cix-bdd").resolve(responseFile);
        Files.writeString(input, buildReply());
        lastRun = jobOperator.start(cixJob, new JobParametersBuilder()
                .addString("arrival.id", UUID.randomUUID().toString(), true)
                .addString("input.file", input.toString(), false)
                .addString("original.name", responseFile, false)
                .toJobParameters());
    }

    private String buildReply() {
        StringBuilder xml = new StringBuilder("<Document>\n");
        if (!omitOrgnlMsgId) {
            xml.append("  <OrgnlMsgId>").append(orgnlMsgId).append("</OrgnlMsgId>\n");
        }
        txBlocks.forEach(block -> xml.append("  ").append(block).append('\n'));
        return xml.append("</Document>\n").toString();
    }

    private static String txBlock(String e2e, String status, String reason) {
        return "<Tx><OrgnlEndToEndId>" + e2e + "</OrgnlEndToEndId><TxSts>" + status + "</TxSts>"
                + (reason == null ? "" : "<Rsn>" + reason + "</Rsn>") + "</Tx>";
    }
}
