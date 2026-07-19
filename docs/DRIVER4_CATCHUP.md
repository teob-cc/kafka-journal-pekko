# Cassandra java-driver 3 → 4 migration: what was done

**Date:** 2026-07-19. **Status:** done — the plan (`DRIVER4_PLAN.md`, removed after execution; see git history) was executed as written with the deviations noted below. The fork now rides `org.apache.cassandra:java-driver-core:4.19.0` exclusively; the EOL DataStax 3.11.5 driver and the driver-3 vendored scassandra (43 files) are gone. Why we migrated: 3.x is the EOL DataStax line, maintenance-only since 2023 — no fixes, no protocol v5, a dead end for driver-level features.

## Source of the driver-4 wrapper

`libs/src/main/scala/com/evolution/scassandra4/` is vendored from **evolution-gaming/scassandra branch `wip/driver-4-support-attempt-2` @ `8b5e9fd12d3aad2a5c214dae9b303cede6422d4a`** (unreleased; see NOTICE.md). Upstream package `com.evolution.scassandra4` kept as-is; the module cross-built Scala 3.3 upstream, so it landed on our 3.8.4 with only trivial syntax fixes. Re-sync consideration: if upstream ever releases this (or force-reworks the branch — it is attempt 2), diff against this SHA; our fork-only additions are marked with `fork addition` comments.

## Fork-only additions to scassandra4 (gaps upstream left)

- **`NextHostRetryPolicy`** — driver 3's per-statement retry policy re-expressed on driver 4's config-class SPI (reflective `(DriverContext, profileName)` ctor). Wired via a fork-only optional **`retries` key on `CassandraConfig`** which `CreateDriverConfigLoader` translates to `advanced.retry-policy.class` + custom `advanced.retry-policy.retries`. Semantics preserved: next-node on first unavailable and on request errors/aborts, same-node on read/write timeouts, all bounded by `retries` (= `retries + 1` executions). The driver 3 `LoggingRetryPolicy` wrapper became slf4j logging inside the policy.
- **`Metadata[F]` / `KeyspaceMetadata[F]` / `TableMetadata`** — mirrors of the driver-3 scassandra wrappers over driver 4's metadata. Driver 4 metadata is an **immutable snapshot**, so the wrappers are `Applicative`-pure (no `Sync` needed — this kept `Monad`-level context bounds throughout kafka-journal's schema-setup call chain). `clusterName` is `Option` now; `userTypes` not mirrored (unused).

## Port decisions in kafka-journal

| Area | Decision |
|---|---|
| retries | moved from per-statement decoration (`CassandraSession(session, retries)`) to session-level config: `CassandraCluster.make` copies `retries` into `CassandraConfig` before connecting. `CassandraSession.apply(session, trace)` keeps only idempotence + tracing decoration. |
| metadata | driver 4 has no cluster-level metadata ⇒ `CassandraCluster[F].metadata` removed; **`CassandraSession[F].metadata`** yields a fresh `CassandraMetadata[F]` snapshot per call (used by `CreateKeyspace`/`CreateTables`; also made the stubs in specs trivial). |
| paging | our driver-3 `ResultSet[F]` paging re-implementation replaced by delegation to scassandra4 `StreamingCassandraSession.executeStream` (prefetches next page while folding, same as before). The generic fold helper + its spec remain. |
| statements | `Statement` → self-typed `Statement[?]`; `new SimpleStatement(q)` → `SimpleStatement.newInstance(q)`; batch writes rebuilt on `BatchStatement.builder(DefaultBatchType.LOGGED)`. **Gotcha:** Scala 3 cannot re-capture a wildcard `Statement[?]` through an implicit-class conversion — call `CassandraSession[F].execute(statement)` directly where the static type is already widened (one site, `JournalStatements.InsertRecords`). |
| types | driver `LocalDate` → `java.time.LocalDate` (native in driver 4); `Duration` → `CqlDuration` (same `newInstance/getDays/getNanoseconds` shape); `ConsistencyLevel` enum → `DefaultConsistencyLevel` (aliased at import sites, config readers unchanged); `GettableByNameData`/`SettableData` → `GettableByName`/`SettableByName` (immutable: setters return new instances — encode chains were already written value-passing style, no changes needed); `getUUID`/`setUUID` → `getUuid`/`setUuid`. |
| cassandra-sync | pure import swap, as planned. LWT `[applied]`/`one()` semantics identical on `AsyncResultSet`. |
| tests | `RowStub` shrank from 88 lines of driver-3 interface to 12 abstract members — driver 4's `Row` is default methods over a few primitives; typed getters (`getBoolean(name)`) still overridable directly. Driver-3 `LocalDate` conversion test deleted (meaningless now). |

## Validation

- Full unit suite green (`sbt testUnit`, all modules).
- **`IntegrationSuite` 149/149 (20 ignored) against Redpanda v25.1.1 + Scylla 2025.1** (external-services mode, fresh volumes) — run twice: once with both drivers on the classpath, once after the driver-3 drop. Covers `SetupSchema` migrations, the `metajournal_created_date_idx` secondary index, cassandra-sync LWT locks, fetchSize-bounded paging, and the in-suite replicator — every behavioral risk the plan flagged, now on driver 4 **against the clone platform** (the plan scoped Scylla validation out; the clone-platform decision made it the default via compose).

## Not yet done (follow-ups)

1. **Node-down retry parity drill**: single-node compose can't exercise next-host semantics meaningfully; verify `NextHostRetryPolicy` behavior during the cit soak or a multi-node testcontainers setup.
2. **Pooling/heartbeat defaults** differ in driver 4 — watch long-lived replicator sessions during the cit soak.
3. **Driver metrics** are not translated (`metrics`/`jmx-reporting` config keys ignored, as upstream documented) — revisit if we want driver-level metrics in the §10 dashboards.
