# dcre-cix

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories) table.

ISR response reader for DCRE Collections: ingests Fintegrate ISR reply files into `isr_resp`, one verdict row per transaction block.

## What it does

| | |
|---|---|
| Stage | `CIX` |
| Family / leg | Collections (DC), RES |
| Trigger | arrival-launched: a reply file on the `fint-resp` route whose name carries `_ISR`, for a client whose flow is collections |
| Upstream | none in the DAG (response DAGs have no edges; AGT token-picks exactly one of `CIX`, `CSX`, `CPX` per reply). The replied-to batch was emitted by `CRW` |
| Downstream | none in the DAG. `CRG` (clock-launched) reads `isr_resp` through its `ext_tx_status` view |
| Diagram sheet | `dcre-collections-res` |

DAG position per AGT `RouteDags.FINT_RESP_COL` and the `_ISR` token pick in `DagEngine` on origin/dev (checked 2026-09-28). Fintegrate (simulated by dcre-infra `scripts/fint_sim_reply.py`) drops a reply file into a per-client `fint-resp/in` exchange directory; AGT launches CIX as a short-lived Kubernetes Job. CIX parses the reply, one `<OrgnlMsgId>` plus repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>` ([SYNTHETIC-CONTRACT R-35] shape), and upserts one `isr_resp` row per Tx block (fan-out at ingest per R-17). Replaying the same file is a no-op via `INSERT ... ON CONFLICT (response_file, e2e)`.

## Architecture and principles

Spring Boot 4.1.0 / Spring Batch 6 / Java 25 on CockroachDB (PostgreSQL driver). An ephemeral batch job, not a server: `ExitCodeMain` (platform-batch) wires the Batch outcome into the JVM exit code (R-34).

- **SOLID, 3-tier, layer-first packages**: `ReaderTasklet` is a thin entry adapter (no SQL, no parsing) that reads `input.file` and calls one business-tier method; `ReaderService` parses and upserts; persistence happens only through `data/repo/IsrRespRepo` (Spring Data JDBC, `IsrRespEntity` extends the platform `BaseEntity`).
- **Batch correlation (SCRUM-55)**: the file's `OrgnlMsgId` resolves once to the CRW emission (`crw_emission.outbound_msg_id`) and its frozen member set (`crw_emission_member`). A verdict whose e2e is not a member is skipped with a `FOREIGN_E2E` WARN (fail closed); an unknown `OrgnlMsgId` ingests fail-open with `emission_id` NULL and an `UNKNOWN_OUTBOUND_MSG` WARN. Packages: `config`, `service`, `data/model`, `data/repo`.
- **12FactorApp Alignment: https://12factor.net/**: config strictly from the environment over committed working dev defaults in `application.yml` (a clean clone runs with no `.env` at all), stateless one-shot process, the shared CockroachDB as an attached backing resource.
- **Idempotent restart semantics**: `IsrRespRepo.upsert` is a native `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` on the business identity (CRDB `UPSERT` arbitrates on the PK only, so the business key needs `ON CONFLICT`). Replays and relaunches converge on the same rows.
- **Sliced ingest (SCRUM-42)**: one giant serializable transaction is unrefreshable at 300k rows (`RETRY_SERIALIZABLE`), so upserts commit in bounded slices (`dcre.cix.ingest-slice-size`, default 10000), each in its own `REQUIRES_NEW` transaction wrapped by `CrdbRetry` (5 attempts, exponential backoff with jitter). Committed slices stand when a later slice fails; a restart no-ops over them and resumes the rest.
- **40001 at the step boundary**: `readerStep` registers the shared `CrdbRetryExceptionHandler` (platform-batch) so commit-time serialization aborts retry instead of failing the job.
- **Stale-execution sweep (A-39a)**: an `@Order(-10)` runner calls `StaleExecutionSweeper.abandonStale(ds, "CIX_BATCH_", 60)` before job launch so a relaunch after a pod kill never throws `JobExecutionAlreadyRunning`.
- **Outcome seam (R-33)**: on `COMPLETED`, platform-batch's `OutcomeSeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>`. A non-COMPLETED execution writes nothing; the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33).

### Data

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp on `launch_intent` |

- Writes: `isr_resp` (Liquibase `db/changelog/2026/08/002-isr-resp.xml`, the v1 baseline): `response_file` VARCHAR(512), `orgnl_msg_id`, `emission_id` (nullable, no FK), `e2e`, `status`, `reason` (nullable), plus `BaseEntity` columns (`version`, `created_at`, `updated_at`); `UNIQUE (response_file, e2e)` and index `ix_isr_emission`. Plus the `CIX_BATCH_*` tables (`dcre.batch.table-prefix`; Boot 4.1 no longer binds `spring.batch.jdbc.*`) from a Liquibase-owned copy of the Batch 6 DDL with `EXIT_MESSAGE` widened to TEXT (`001-batch-metadata.xml`).
- Reads: `crw_emission`, `crw_emission_member` (CRW-owned).
- Liquibase history is per-service: `cix_databasechangelog` / `cix_databasechangeloglock`.

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers CockroachDB for tests, image build for deployment)
- Platform libs in Maven Local: `za.co.fnb.dcre:platform-persistence:0.1.0` and `za.co.fnb.dcre:platform-batch:0.1.0` (batch brings `platform-files` and `platform-model` transitively via its `api` chain)

## Quickstart

```bash
# 1) Publish the platform libs to Maven Local (once), in dependency order:
#    dcre-platform-model -> dcre-platform-files -> dcre-platform-batch; dcre-platform-persistence standalone.
#    In each platform repo clone:
./gradlew publishToMavenLocal

# 2) Build and test (Docker required; no .env needed, dev defaults are committed)
./gradlew test

# 3) Local one-shot run against a local CockroachDB (defaults target localhost:26257)
./gradlew bootJar
java -jar build/libs/cix-2.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/reply.xml,java.lang.String,false' \
  'original.name=20260712_FNB_ISR_reply.xml,java.lang.String,false'
```

## Configuration

Env over committed dev defaults (`application.yml`); precedence: yml default < environment.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | Shared collections DB (CockroachDB) |
| `DCRE_DB_USER` | `root` | DB username |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / (empty) | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam (AGT sets `/exchange`) |
| `DCRE_CIX_INGEST_SLICE_SIZE` | `10000` | Rows per committed ingest slice (SCRUM-42) |
| `JOB_NAME` | `local-cix-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

JobParameters: `arrival.id` (identifying, R-16), `input.file` and `original.name` (non-identifying; `original.name` becomes the `response_file` identity column).

## Testing

```bash
./gradlew test   # needs Docker
```

- `CixJobTest`: full job on Testcontainers CockroachDB `v26.2.3`; ingests a 4-Tx synthetic reply (ACSC/RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, and proves replay of the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs against real CRDB; a slice that exhausts its retry budget fails the run without rolling back committed slices, a transient 40001 abort retries in a fresh transaction, and a re-run no-ops over committed slices preserving row identity.
- `BatchCorrelationIT`: resolves the emission batch and skips foreign e2e (fail closed); an unknown `OrgnlMsgId` ingests fail-open with a NULL batch.
- `IsrRespWidthIT`: a 200-character `response_file` round-trips; the replay-guard unique constraint holds at baseline width.
- `CixJobConfigRetryTest`: proves the shared 40001 retry handler is registered on the step the real job config builds, covering commit-time aborts.
- `CucumberSuiteTest`: business-readable BDD scenarios in `src/test/resources/features/isr-reply-reader.feature` (fan-out, reject reasons, replay, malformed and empty replies).

## Local cluster deployment

```bash
VERSION=<fleet release tag>
./gradlew bootJar
docker build -t dcre-cix:$VERSION .
kind load docker-image --name dcre-dev dcre-cix:$VERSION
```

The image is `eclipse-temurin:25-jre-alpine`; the kind cluster `dcre-dev` and the database come from dcre-infra. AGT resolves the image from `AGT_CIX_IMAGE` (empty by default, which leaves the stage launch-disabled). dcre-infra `scripts/switch-version.sh` does NOT set `AGT_CIX_IMAGE`: its stage list still names the retired `IXR` (checked 2026-09-28), so set it on the AGT deployment by hand (`kubectl set env -n dcre deploy/dcre-agt AGT_CIX_IMAGE=dcre-cix:$VERSION`). Per reply AGT creates a Job in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with program args `arrival.id=<uuid>`, `input.file=<claimed path>` and `original.name=<physical filename>`, and env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` (AGT `JobLauncher` on origin/dev, checked 2026-09-28). Releases are digits-only 3-component SemVer tags, uniform across the fleet.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
