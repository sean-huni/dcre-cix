# dcre-ixr

ISR response-leg reader (SCRUM-25, M4). Ingests synthetic pain.002-family ISR reply files (token _ISR) into isr_resp: one row per Tx block, fan-out at ingest per R-17. Replay-safe via INSERT ... ON CONFLICT (response_file, e2e). 3-tier: ReaderTasklet -> ReaderService -> data/repo. [SYNTHETIC-CONTRACT R-35] reply shape.

Spring Boot 4.1.0 / Spring Batch 6 / Java 25, CockroachDB via the PostgreSQL driver. Ephemeral batch job, not a server: `ExitCodeMain` wires the Batch outcome into the JVM exit code (R-34). ISR is the initial status report leg: the file/micro-batch level ACK/NACK from Fintegrate (formal expansion owed, A-5).

## Pipeline position

Response flow (SPEC-DAG-PIPELINE): `IXR | SXR | PXR -> ext_tx_status -> PRG`. Fintegrate drops a reply file into the exchange; AGT's `fint-resp` route (R-36) selects the reader by the filename reply-type token, `_ISR` launching IXR as an ephemeral K8s Job (unknown tokens quarantine fail-closed). The ingested rows feed the `ext_tx_status` consolidation with stage-rank precedence PBSR > SBSR > ISR > AIS/CTV (R-17), which PRG reads. Boundary reader per R-30: only boundary services touch files.

## Job structure

`ixrJob` (za.co.fnb.dcre.ixr.config.IxrJobConfig), single tasklet step `readerStep`:

- `ReaderTasklet` (thin entry adapter: no SQL, no parsing) reads `input.file` and hands the text plus `original.name` to the business tier, recording the ingested row count as `rows` in the execution context.
- `ReaderService` extracts the single `<OrgnlMsgId>` (missing one is an error) and every `<Tx>` block (`<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>`) via regex against the [SYNTHETIC-CONTRACT R-35] reply shape, upserting one `isr_resp` row per Tx block.
- `IsrRespRepo.upsert`: native `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` on the business identity (CRDB `UPSERT` resolves on the PK only), so replaying the same file is a no-op.

`SeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` on COMPLETED (SYNTHETIC-CONTRACT, R-35). A non-COMPLETED execution writes nothing: the exit code and the K8s Failed condition are the witnesses; AGT treats absence as never-success (R-33 arbiter clause).

JobParameters: `arrival.id` (identifying, R-16), `input.file` and `original.name` (non-identifying; `original.name` becomes the `response_file` identity column).

## Data

`isr_resp` (Liquibase `001-ixr.xml`, CREATE IF NOT EXISTS): `response_file`, `orgnl_msg_id`, `e2e`, `status`, `reason` (nullable), plus BaseEntity columns (version, created_at, updated_at); `UNIQUE (response_file, e2e)`.

## Local module dependencies

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `BaseEntity` (version/created_at/updated_at on `IsrRespEntity`), `JdbcConfig` (Spring Data JDBC base config, imported by `IxrApplication`) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34 exit-code wiring), `OutcomeFileWriter` (outcome seam), `StaleExecutionSweeper` (A-39a self-abandonment) |

Both resolve from Maven Local only (no remote repository): run `./gradlew publishToMavenLocal` in each dependency repo first, publish chain `dcre-platform-model` -> `dcre-platform-files` -> `dcre-platform-batch` (batch brings files and model transitively via its `api` chain); `dcre-platform-persistence` is standalone. Details in each module repo's README under "Publishing".

## Configuration (env over committed dev defaults, 12FactorApp Alignment: https://12factor.net/)

| Env | Default | Meaning |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_collections?sslmode=disable` | Shared collections DB (CockroachDB) |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | DB credentials |
| `DCRE_EXCHANGE_ROOT` | `../../infra/dcre-infra/exchange` | Exchange root for the outcome seam |
| `JOB_NAME` | `local-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

Clean clone runs with no `.env` at all; the working dev defaults are committed in `application.yml`.

## Batch metadata

Prefixed `IXR_BATCH_` tables (`spring.batch.jdbc.table-prefix`, `initialize-schema: never`) via a Liquibase-owned copy of the Batch 6 postgres DDL with EXIT_MESSAGE widened to TEXT (`002-batch-metadata.xml`, per R-34/A-39b). Per-service Liquibase history on the shared DB: `ixr_databasechangelog` / `ixr_databasechangeloglock`. `StaleExecutionSweeper.abandonStale(ds, "IXR_BATCH_", 60)` runs before the job launches (A-39a) so a relaunch after a pod kill never throws JobExecutionAlreadyRunning.

## Build & test

`./gradlew test` (needs Docker): `IxrJobTest` on Testcontainers CockroachDB v26.2.3 ingests a 4-Tx synthetic reply (ACSC/RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, and proves replay of the same file stays at 4 rows. Platform libs resolve from mavenLocal (see Local module dependencies).

## Run

Cluster: AGT launches IXR as an ephemeral K8s Job per `_ISR` reply arrival (image: `./gradlew bootJar && docker build -t dcre-ixr:0.1.0 .`, eclipse-temurin 25 jre-alpine), passing the JobParameters as program args and `JOB_NAME` in the env. Local one-shot:

```bash
./gradlew bootJar
java -jar build/libs/dcre-ixr-0.1.0.jar \
  'arrival.id=<uuid>,java.lang.String,true' \
  'input.file=/path/to/reply.xml,java.lang.String,false' \
  'original.name=20260712_FNB_ISR_reply.xml,java.lang.String,false'
```

## Observability

No metrics endpoints yet. The observable surface is: the JVM exit code (R-34), the outcome seam file (R-35), the `rows` counter in the execution context, and the `IXR_BATCH_` metadata as the step-grain diagnostics annex (R-33: advisory only).
