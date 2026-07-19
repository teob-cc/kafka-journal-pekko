# Plan: Cassandra Java driver 3 → 4 migration

**Date:** 2026-07-19. **Status:** DONE — executed same day; see `DRIVER4_CATCHUP.md` for what shipped and the deviations. Prerequisite: v9.1.1 catch-up merged (see `V9_CATCHUP.md`).

## Why

The fork rides `com.datastax.cassandra:cassandra-driver-core:3.11.5` (via vendored scassandra) — the EOL DataStax 3.x line, maintenance-only since 2023. It works against Cassandra 4.x/5.0 (protocol v4), so this is not urgent, but it is a dead end: no fixes, no protocol v5, and a hard blocker if we ever need driver-level features (reactive paging, modern metrics, Astra, etc.). Target: `org.apache.cassandra:java-driver-core:4.19.0` (the Apache-donated driver 4 line).

## The upstream head start

Evolution is building driver-4 support in scassandra on branch **`wip/driver-4-support-attempt-2`** (last push 2026-07-10, unmerged, unreleased). Assessment of the branch (2026-07-19):

- A new **additive `scassandra4` module** (34 main files, package `com.evolution.scassandra4`) mirroring the scassandra API **name-for-name** with the **same HOCON config schema**, translated programmatically to driver-4 options (`CreateDriverConfigLoader`).
- Its own `MIGRATION_PLAN.md` marks all four phases ✅ done (skeleton, config translation, data layer, polish), with integration tests including a `CoexistenceSpec` (driver 3 + 4 on one classpath against one testcontainer) and a client-facing `MIGRATION_GUIDE.md` with type-mapping tables.
- Coexistence is by construction: different Maven coordinates, different Java packages, Guava shaded in driver 4; only shared concern is Netty 4.1.x.
- **Documented gaps:** `NextHostRetryPolicy` not ported (driver 4's retry SPI is config-class based) and scassandra's `Metadata[F]` wrapper dropped (raw driver `Metadata` exposed). Both matter to us (below).

**Decision: vendor from this branch now rather than wait for an upstream release.** The fork already evolves independently; the branch is functionally complete with tests; we pin the vendored commit SHA in `libs/` docs and re-sync if upstream tags a release. (Re-assess only if the branch is force-reworked upstream — it is the *second* attempt, after all.)

## Where driver 3 touches our code (verified inventory)

| Consumer | Coupling | Port shape |
|---|---|---|
| `libs/` vendored scassandra (45 files) | is the driver-3 wrapper | replaced by vendored `scassandra4` (34 files) |
| `libs/` vendored cassandra-sync (2 files) | only scassandra API + syntax | pure import swap |
| kafka-journal modules — **60 files** (`cassandra`, `eventual-cassandra`, `snapshot-cassandra`, `replicator`, `persistence`, `tests`) | mostly `import com.evolutiongaming.scassandra.*` (syntax, codecs, `TableName`, `CassandraClusterOf`, configs, `FromGFuture`) | import swap: `com.evolution.scassandra4.*`, `FromGFuture` → `FromCompletionStage` |
| `eventual-cassandra/.../CassandraSession.scala` (our wrapper) | **direct driver types**: `LoggingRetryPolicy(NextHostRetryPolicy(retries))`, `Statement`, `PreparedStatement`, `SimpleStatement`, custom `ResultSet[F]` paging stream | the one real rewrite — see gap G1/G3 |
| `cassandra/.../CreateKeyspace.scala`, `CreateTables.scala` | `CassandraCluster[F].metadata` → `KeyspaceMetadata[F]` wrappers | gap G2 |
| config readers (`EventualCassandraConfig`, `ReplicatorConfig`, specs) | `ConsistencyLevel` (driver-3 enum), `LocalDate`, `Duration` | scassandra4 config tree covers `ConsistencyLevel` (`DefaultConsistencyLevel`); `LocalDate` → `java.time.LocalDate` (native in driver 4), `Duration` → `CqlDuration` |

## Gaps we fill in the vendored copy (upstream didn't)

- **G1 — retry policy.** kafka-journal's `CassandraSession(session, retries)` wraps statements with `LoggingRetryPolicy(NextHostRetryPolicy(retries))`. Driver 4 configures retry policies per-profile via config (class-name SPI), not per-statement instances. Port: implement a driver-4 `RetryPolicy` with next-host semantics (retry on next node up to N attempts), register it through `CreateDriverConfigLoader` as an execution profile, and have our `CassandraSession` select the profile per statement (`setExecutionProfileName`). Behavior parity test: node-down retry in the integration suite.
- **G2 — `Metadata[F]` wrappers.** Add `Metadata[F]`/`KeyspaceMetadata[F]`/`TableMetadata[F]` mirrors to vendored scassandra4 over driver 4's (synchronous, immutable-snapshot) `Metadata` API — trivial, keeps `CreateKeyspace`/`CreateTables` diffs to import swaps.
- **G3 — statement/paging surface in our wrapper.** Driver 4: statements are immutable builders (`SimpleStatement.newInstance`, self-typed `Statement[?]`), `executeAsync` returns `AsyncResultSet` with `fetchNextPage()`. Our `CassandraSession.execute: Stream[F, Row]` re-implements paging; either delegate to scassandra4's `StreamingCassandraSession.executeStream` (has next-page prefetch) or port our `ResultSet[F]` helper to `AsyncResultSet`. Prefer delegation.

## Execution plan

1. **Vendor** scassandra4 sources from the pinned branch commit into `libs/src/main/scala/com/evolution/scassandra4/` (34 files; keep upstream package). Compile-only milestone; driver-3 scassandra stays put — both coexist in `libs` exactly as upstream designed.
2. **Fill gaps** G1 + G2 in the vendored copy (~2 small files + config-loader hook).
3. **Port cassandra-sync** (import swap) and **kafka-journal's `CassandraSession` wrapper** (G3).
4. **Sweep the 60 files**: import rewrites per the upstream `MIGRATION_GUIDE.md` mapping table; fix type fallout (`Statement[?]`, `ConsistencyLevel` reader, `LocalDate`, `CqlDuration`, `GettableByName`/`SettableByName` renames).
5. **Green the suite**: full unit + testcontainers integration run (`ConsistencySpec`, `ReplicatorIntSpec`, schema creation, `SettingsIntSpec` (LWT via cassandra-sync), `JournalCirceIntSpec` paging) — all on driver 4.
6. **Drop driver 3**: delete vendored scassandra (45 files) + `CassandraDriver` dep + jffi pin if unused; full suite again. Single release contains the cutover; no long dual-driver period needed since we own all call sites.
7. **Doc + canary**: `DRIVER4_CATCHUP.md` (what/why, pinned upstream SHA), note in README. The Kafka wire-format canary (`ActionHeaderJsonSpec`) is unaffected; Cassandra CQL statements are unchanged (same schema, same queries) — verify via the schema-creation specs rather than a new golden file.

## Behavioral risks to test explicitly

| Risk | Where it bites | Mitigation |
|---|---|---|
| Retry semantics differ (per-statement → profile-based) | recovery-path reads under node failure | G1 parity test; keep retry count semantics (`maxExecutions = retries + 1` translation, as upstream notes) |
| Protocol: driver 4 requires v3+ (drops V1/V2 config parse) | none for us (Cassandra 4.x/5.0 = v4/v5) | config fixtures already verbatim-tested upstream |
| Paging/prefetch behavior change in streams | large `read(key, from)` folds, replicator batches | `fetchSize`-bounded integration test exists upstream (5 rows @ fetchSize 2); our suite's 10000-event recovery covers volume |
| Timestamp/date codecs (`LocalDate`, `Instant` now native) | `ExpireOn`/settings tables | `LocalDateTest` + `SettingsIntSpec` |
| Netty overlap (both drivers during step 1–5, then skafka/kafka-clients alongside driver 4) | classpath | `evicted` check; driver's shaded variant as fallback |
| Idle/heartbeat + pooling defaults differ in driver 4 | long-lived replicator sessions | soak in cit before prod cutover (same staging as journal adoption path) |

## Effort

Steps 1–2: **S** (a day). Steps 3–4: **M** (2–4 days; the wrapper + 60-file sweep is mechanical but wide). Steps 5–7: **S–M** (test-driven fix loop). Total ≈ **1–1.5 weeks**, one person, independent of any teob work.

## Non-goals

- No driver-agnostic abstraction layer (upstream's "mirror, don't abstract" reasoning applies to us doubly — we own both sides).
- No ScyllaDB validation in this pass (driver 4 changes the compatibility question; separate spike if ever needed).
- No contribution-back workflow: if upstream merges their branch, our vendored copy re-syncs at the next quarterly review like every other lib.
