package za.co.fnb.dcre.ixr.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.ixr.data.repo.IsrRespRepo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Business tier: parses the reply file and upserts one row per Tx block
 * (fan-out at ingest per R-17). Replay of the same file is a no-op via
 * ON CONFLICT (response_file, e2e).
 *
 * [SYNTHETIC-CONTRACT R-35] Reply shape: one OrgnlMsgId element, then
 * repeated Tx blocks of OrgnlEndToEndId + TxSts with an optional Rsn.
 *
 * <p>SCRUM-42 load fix: a 300k-row reply ingested in ONE serializable
 * transaction is unrefreshable; CRDB aborts it with RETRY_SERIALIZABLE
 * "can't refresh txn spans" and the step-level retry just re-runs the same
 * doomed giant transaction (IXR died exit 5 on the 300k ISR sweep). Upserts
 * therefore commit in bounded slices. Committed slices stand when a later
 * slice fails: the upsert targets the row's business identity
 * (response_file, e2e), so a restart (step-level retry or job relaunch)
 * no-ops over them and resumes the rest.
 */
@Service
public class ReaderService {

    private static final Pattern ORGNL_MSG_ID =
            Pattern.compile("<OrgnlMsgId>([^<]+)</OrgnlMsgId>");
    private static final Pattern TX = Pattern.compile(
            "<Tx>\\s*<OrgnlEndToEndId>([^<]+)</OrgnlEndToEndId>"
                    + "\\s*<TxSts>([^<]+)</TxSts>(?:\\s*<Rsn>([^<]+)</Rsn>)?",
            Pattern.DOTALL);

    private record Verdict(String e2e, String status, String reason) {
    }

    private final IsrRespRepo repo;
    private final TransactionTemplate sliceTx;
    private final int sliceSize;

    public ReaderService(final IsrRespRepo repo, final PlatformTransactionManager txManager,
                         @Value("${dcre.ixr.ingest-slice-size:10000}") final int sliceSize) {
        this.repo = repo;
        // Each slice commits in its OWN transaction so a 300k-row reply
        // ratchets progress slice by slice; and a CRDB 40001 abort poisons the
        // surrounding transaction (25P02 on any further statement), so a retry
        // needs a fresh transaction per attempt.
        this.sliceTx = new TransactionTemplate(txManager);
        this.sliceTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.sliceSize = sliceSize;
    }

    /** @return number of Tx verdicts ingested. */
    public int ingest(final String fileText, final String responseFile) {
        Matcher msgId = ORGNL_MSG_ID.matcher(fileText);
        if (!msgId.find()) {
            throw new IllegalArgumentException("reply file has no <OrgnlMsgId>: " + responseFile);
        }
        String orgnlMsgId = msgId.group(1);
        List<Verdict> verdicts = parse(fileText);
        for (int from = 0; from < verdicts.size(); from += sliceSize) {
            writeSlice(responseFile, orgnlMsgId,
                    verdicts.subList(from, Math.min(from + sliceSize, verdicts.size())), from);
        }
        return verdicts.size();
    }

    /** Single pass over the ~40MB reply string; parsing stays out of the write transactions. */
    private List<Verdict> parse(final String fileText) {
        Matcher tx = TX.matcher(fileText);
        List<Verdict> verdicts = new ArrayList<>();
        while (tx.find()) {
            verdicts.add(new Verdict(tx.group(1), tx.group(2), tx.group(3)));
        }
        return verdicts;
    }

    /** One slice = one committed unit: fresh REQUIRES_NEW tx per bounded-retry attempt. */
    private void writeSlice(final String responseFile, final String orgnlMsgId,
                            final List<Verdict> slice, final int from) {
        CrdbRetry.run("ingest slice file=%s from=%d".formatted(responseFile, from),
                () -> sliceTx.execute(status -> {
                    for (final Verdict verdict : slice) {
                        repo.upsert(responseFile, orgnlMsgId,
                                verdict.e2e(), verdict.status(), verdict.reason());
                    }
                    return null;
                }));
    }
}
