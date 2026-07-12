package za.co.fnb.dcre.ixr.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.ixr.data.model.IsrRespEntity;

import java.util.UUID;

public interface IsrRespRepo extends CrudRepository<IsrRespEntity, UUID> {

    // CRDB: UPSERT resolves on PK only, so replay safety needs
    // INSERT ... ON CONFLICT on the business identity (response_file, e2e).
    @Modifying
    @Query("""
            INSERT INTO isr_resp (id, response_file, orgnl_msg_id, e2e, status, reason)
            VALUES (gen_random_uuid(), :responseFile, :orgnlMsgId, :e2e, :status, :reason)
            ON CONFLICT (response_file, e2e)
            DO UPDATE SET status = excluded.status, reason = excluded.reason, updated_at = now()""")
    void upsert(@Param("responseFile") String responseFile, @Param("orgnlMsgId") String orgnlMsgId,
                @Param("e2e") String e2e, @Param("status") String status, @Param("reason") String reason);

    long countByResponseFile(String responseFile);
}
