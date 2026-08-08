package za.co.fnb.dcre.cix.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.util.UUID;

/** One reply verdict per original transaction (ISR response leg). */
@Table("isr_resp")
public class IsrRespEntity extends BaseEntity {

    private String responseFile;
    private String orgnlMsgId;
    /** CRW emission batch that carried this e2e; NULL when the registry is behind (SCRUM-55). */
    private UUID emissionId;
    private String e2e;
    private String status;
    private String reason;

    public String getResponseFile() { return responseFile; }
    public String getOrgnlMsgId() { return orgnlMsgId; }
    public UUID getEmissionId() { return emissionId; }
    public String getE2e() { return e2e; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
}
