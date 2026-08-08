# dcre-cix

ISR response reader for DCRE Collections: ingests Fintegrate ISR reply files into `isr_resp`, one verdict row per transaction block.

## What it does

CIX is the initial-status-report leg of the response flow (`CIX | SXR | PXR -> ext_tx_status -> PRG`). Fintegrate (simulated by dcre-infra `fint_sim_reply.py`) drops a reply file into a per-client `fint-resp/in` exchange directory; AGT selects the reader by the `_ISR` filename token and launches CIX as a short-lived Kubernetes Job. CIX parses the reply, one `<OrgnlMsgId>` plus repeated `<Tx>` blocks of `<OrgnlEndToEndId>` + `<TxSts>` + optional `<Rsn>` ([SYNTHETIC-CONTRACT R-35] shape), and upserts one `isr_resp` row per Tx block (fan-out at ingest per R-17). Replaying the same file is a no-op via `INSERT ... ON CONFLICT (response_file, e2e)`.

## Architecture and principles

Spring Boot 4.1.0 / Spring Batch 6 / Java 25 on CockroachDB (PostgreSQL driver). An ephemeral batch job, not a server: `ExitCodeMain` (platform-batch) wires the Batch outcome into the JVM exit code (R-34).

- **SOLID, 3-tier, layer-first packages**: `ReaderTasklet` is a thin entry adapter (no SQL, no parsing) that reads `input.file` and calls one business-tier method; `ReaderService` parses and upserts; persistence happens only through `data/repo/IsrRespRepo` (Spring Data JDBC, `IsrRespEntity` extends the platform `BaseEntity`). Packages: `config`, `service`, `data/model`, `data/repo`.
- **12FactorApp Alignment: https://12factor.net/**: config strictly from the environment over committed working dev defaults in `application.yml` (a clean clone runs with no `.env` at all), stateless one-shot process, the shared CockroachDB as an attached backing resource.
- **Idempotent restart semantics**: `IsrRespRepo.upsert` is a native `INSERT ... ON CONFLICT (response_file, e2e) DO UPDATE` on the business identity (CRDB `UPSERT` arbitrates on the PK only, so the business key needs `ON CONFLICT`). Replays and relaunches converge on the same rows.
- **Sliced ingest (SCRUM-42)**: one giant serializable transaction is unrefreshable at 300k rows (`RETRY_SERIALIZABLE`), so upserts commit in bounded slices (`dcre.cix.ingest-slice-size`, default 10000), each in its own `REQUIRES_NEW` transaction wrapped by `CrdbRetry` (5 attempts, exponential backoff with jitter). Committed slices stand when a later slice fails; a restart no-ops over them and resumes the rest.
- **40001 at the step boundary**: `readerStep` registers the shared `CrdbRetryExceptionHandler` (platform-batch) so commit-time serialization aborts retry instead of failing the job.
- **Stale-execution sweep (A-39a)**: `StaleExecutionSweeper.abandonStale(ds, "CIX_BATCH_", 60)` runs before job launch so a relaunch after a pod kill never throws `JobExecutionAlreadyRunning`.
- **Outcome seam (R-35)**: on `COMPLETED`, `SeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>`. A non-COMPLETED execution writes nothing; the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33).

### Data

`isr_resp` (Liquibase `db/changelog/2026/07/001-cix.xml`): `response_file`, `orgnl_msg_id`, `e2e`, `status`, `reason` (nullable), plus `BaseEntity` columns (`version`, `created_at`, `updated_at`); `UNIQUE (response_file, e2e)`. Batch metadata lives in `CIX_BATCH_`-prefixed tables (`spring.batch.jdbc.table-prefix`, `initialize-schema: never`) via a Liquibase-owned copy of the Batch 6 DDL with `EXIT_MESSAGE` widened to TEXT (`002-batch-metadata.xml`). Liquibase history on the shared DB is per-service: `cix_databasechangelog` / `cix_databasechangeloglock`.

## Prerequisites

- Java 25 (Gradle toolchain; wrapper 9.5.1 included)
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
| `DCRE_EXCHANGE_ROOT` | `../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam |
| `DCRE_CIX_INGEST_SLICE_SIZE` | `10000` | Rows per committed ingest slice (SCRUM-42) |
| `JOB_NAME` | `local-<executionId>` | Set by AGT on the K8s Job; names the outcome seam file |

JobParameters: `arrival.id` (identifying, R-16), `input.file` and `original.name` (non-identifying; `original.name` becomes the `response_file` identity column).

## Testing

```bash
./gradlew test   # needs Docker
```

- `CixJobTest`: full job on Testcontainers CockroachDB `v26.2.3`; ingests a 4-Tx synthetic reply (ACSC/RJCT with reason AC04), asserts per-row status/reason/orgnl_msg_id, and proves replay of the same file stays at 4 rows.
- `ReaderServiceSliceTest`: sliced-ingest proofs against real CRDB; a slice that exhausts its retry budget fails the run without rolling back committed slices, a transient 40001 abort retries in a fresh transaction, and a re-run no-ops over committed slices preserving row identity.
- `CixJobConfigRetryTest`: proves the shared 40001 retry handler is registered on the step the real job config builds, covering commit-time aborts.
- `CucumberSuiteTest`: business-readable BDD scenarios in `src/test/resources/features/isr-reply-reader.feature` (fan-out, reject reasons, replay, malformed and empty replies).

## Local cluster deployment

```bash
./gradlew bootJar
docker build -t dcre-cix:2.1.1 .
kind load docker-image --name dcre-dev dcre-cix:2.1.1
```

The image is `eclipse-temurin:25-jre-alpine`. AGT launches CIX as an ephemeral K8s Job in the `dcre` namespace whenever a `_ISR` reply lands in a per-client `fint-resp/in` directory, resolving the image from its `AGT_CIX_IMAGE` env (managed fleet-wide by dcre-infra `scripts/switch-version.sh`). JobParameters arrive as program args; `JOB_NAME` is set in the Job env. Releases are digits-only 3-component SemVer tags, uniform across the fleet (current: 2.1.1).

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Stage services: [dcre-crr](https://github.com/sean-huni/dcre-crr), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-crw](https://github.com/sean-huni/dcre-crw), [dcre-sxr](https://github.com/sean-huni/dcre-sxr), [dcre-pxr](https://github.com/sean-huni/dcre-pxr), [dcre-prg](https://github.com/sean-huni/dcre-prg), [dcre-ais](https://github.com/sean-huni/dcre-ais), [dcre-hcs](https://github.com/sean-huni/dcre-hcs)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Infra and tooling: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register), [dcre-rpt](https://github.com/sean-huni/dcre-rpt)
