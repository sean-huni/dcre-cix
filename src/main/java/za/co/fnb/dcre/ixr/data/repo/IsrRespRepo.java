package za.co.fnb.dcre.ixr.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.ixr.data.model.IsrRespEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IsrRespRepo extends CrudRepository<IsrRespEntity, UUID> {

    // CRDB: UPSERT resolves on PK only, so replay safety needs
    // INSERT ... ON CONFLICT on the business identity (response_file, e2e).
    // emission_id updates on conflict too: a replay after CRW's registry
    // caught up backfills the correlation (SCRUM-55).
    @Modifying
    @Query("""
            INSERT INTO isr_resp (id, response_file, orgnl_msg_id, emission_id, e2e, status, reason)
            VALUES (gen_random_uuid(), :responseFile, :orgnlMsgId, :emissionId, :e2e, :status, :reason)
            ON CONFLICT (response_file, e2e)
            DO UPDATE SET status = excluded.status, reason = excluded.reason,
                          emission_id = excluded.emission_id, updated_at = now()""")
    void upsert(@Param("responseFile") String responseFile, @Param("orgnlMsgId") String orgnlMsgId,
                @Param("emissionId") UUID emissionId, @Param("e2e") String e2e,
                @Param("status") String status, @Param("reason") String reason);

    /**
     * SCRUM-55 batch correlation: resolve a reply's OrgnlMsgId to the emission
     * batch CRW registered (unique index uq_emission_outbound_msg, crw Task 1).
     * Empty when CRW's registry is behind: caller ingests fail-open with NULL.
     */
    @Query("SELECT id FROM crw_emission WHERE outbound_msg_id = :m")
    Optional<UUID> findEmissionIdByOutboundMsgId(@Param("m") String m);

    /**
     * Membership guard: the e2e set frozen into the batch at plan time,
     * bounded by the max split size (default 5000), so an in-memory Set is safe.
     */
    @Query("SELECT e2e FROM crw_emission_member WHERE emission_id = :id")
    List<String> findMemberE2e(@Param("id") UUID id);

    long countByResponseFile(String responseFile);
}
