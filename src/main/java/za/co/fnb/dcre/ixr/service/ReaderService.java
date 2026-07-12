package za.co.fnb.dcre.ixr.service;

import org.springframework.stereotype.Service;
import za.co.fnb.dcre.ixr.data.repo.IsrRespRepo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Business tier: parses the reply file and upserts one row per Tx block
 * (fan-out at ingest per R-17). Replay of the same file is a no-op via
 * ON CONFLICT (response_file, e2e).
 *
 * [SYNTHETIC-CONTRACT R-35] Reply shape: one OrgnlMsgId element, then
 * repeated Tx blocks of OrgnlEndToEndId + TxSts with an optional Rsn.
 */
@Service
public class ReaderService {

    private static final Pattern ORGNL_MSG_ID =
            Pattern.compile("<OrgnlMsgId>([^<]+)</OrgnlMsgId>");
    private static final Pattern TX = Pattern.compile(
            "<Tx>\\s*<OrgnlEndToEndId>([^<]+)</OrgnlEndToEndId>"
                    + "\\s*<TxSts>([^<]+)</TxSts>(?:\\s*<Rsn>([^<]+)</Rsn>)?",
            Pattern.DOTALL);

    private final IsrRespRepo repo;

    public ReaderService(IsrRespRepo repo) {
        this.repo = repo;
    }

    /** @return number of Tx verdicts ingested. */
    public int ingest(String fileText, String responseFile) {
        Matcher msgId = ORGNL_MSG_ID.matcher(fileText);
        if (!msgId.find()) {
            throw new IllegalArgumentException("reply file has no <OrgnlMsgId>: " + responseFile);
        }
        String orgnlMsgId = msgId.group(1);
        Matcher tx = TX.matcher(fileText);
        int rows = 0;
        while (tx.find()) {
            repo.upsert(responseFile, orgnlMsgId, tx.group(1), tx.group(2), tx.group(3));
            rows++;
        }
        return rows;
    }
}
