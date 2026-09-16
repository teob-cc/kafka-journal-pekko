# CLAUDE.md

Agent takeover context for this repository. Read this first; it links everything else.

## What this repository is

The **teob-cc fork of [evolution-gaming/kafka-journal](https://github.com/evolution-gaming/kafka-journal)** — a Kafka-first event journal with Cassandra as the eventual long-term store, exposing an Apache Pekko persistence plugin. We insourced it, migrated it to **Scala 3**, and publish it under **`cc.teob`** coordinates to the platform Nexus (`nexus-api.<base_domain>`). It is the intended scalable journal backend for the TEOB event-sourcing framework (`~/work/teob`).

Key facts:

- **Vendored dependencies:** all 17 Evolution libraries (skafka, scassandra, cats-helper, scache, cassandra-sync, smetrics, …) live as *source* in `libs/` (~21.8k lines). A quirk in any client library is a patchable event here, not an upstream ticket. See `NOTICE.md` for provenance.
- **Clone platform:** we run this against **Redpanda** (Kafka API) and **ScyllaDB** (CQL) — *not* JVM Kafka/Cassandra. Apache Kafka and Cassandra exist in our world only as client protocols. Gating spike 2026-07-19: full `IntegrationSuite` **149/149 green** against Redpanda v25.1.1 + Scylla 2025.1, zero client patches. Local stack: `docker compose up -d --wait` (see `docker-compose.yml`), then `KAFKA_JOURNAL_EXTERNAL_SERVICES=true sbt tests/test`. **Full suite runs (anything including the Pekko TCK) need fresh volumes — `docker compose down -v` first**: the TCK uses fixed persistence ids and assumes a virgin journal; stale data shows up as JSON-decode fallback failures in the replay specs, not as an honest "dirty state" error.
- **History discipline:** our commits are a short readable stack replayed on top of upstream master (insourcing → scala-3 migration → v9.1.1 catch-up → stabilize → identity → publish pipeline → clone spike). We **rebase onto upstream**, never merge. After a rebase, push is `git push --force-with-lease origin master`.

## Remotes — careful

| remote | URL | role |
|---|---|---|
| `origin` | teob-cc/kafka-journal-pekko | ours — push here, `--force-with-lease` after rebases |
| `upstream` | evolution-gaming/kafka-journal | **fetch-only** — push URL set to `DISABLED` |

Versioning is **CalVer** (`build.sbt`): base `<yyyyMM>.<minor>` bumped by hand (`versionBase`), CI stamps `<base>.<run number>` releases (run number of the *publish* workflow), local builds are `<base>-SNAPSHOT`.

Release flow — **a master push is a release, but only via a green e2e**: the push runs `e2e.yml`; publishing (`publish-to-nexus.yml`) triggers on `workflow_run` of that lane and runs only when the conclusion is `success` and the triggering event was a push, checking out the exact `head_sha` e2e validated. So a release now lags a push by the e2e run (~13 min), and a red e2e means nothing is published. `workflow_dispatch` bypasses the gate entirely (escape hatch), with an optional `version` input overriding CalVer.

## Documents in this repo

- `docs/SCALA3_MIGRATION.md` — how the Scala 3 migration was done, patterns used.
- `docs/V9_CATCHUP.md` — the v6.2.0→v9.1.1 catch-up: what was ported, vendored-library refresh table (skafka 20.2.1 = kafka-clients 4.x).
- `docs/DRIVER4_CATCHUP.md` — the driver-4 migration record (done 2026-07-19): everything now rides java-driver-core 4.19.0 via vendored `scassandra4`; the DataStax 3.11.5 driver and driver-3 scassandra are gone. Includes the re-sync SHA and follow-ups.
- `docs/Notes for Next Developer.md` — **upstream's** architecture notes (actions model, reading flow); starts with a warning that it may be outdated, but the actions/marker model description is still the best intro to how the journal actually works.

## The roadmap lives in teob

Two plans in `~/work/teob/.claude/plans/` drive all work here:

- **`kafka-journal-integration.md`** — TEOB↔kafka-journal integration study: Pekko plugin config, snapshot strategy (pekko-persistence-cassandra until upstream [#532](https://github.com/evolution-gaming/kafka-journal/issues/532) lands), the read-side port (`KafkaReadJournal`, `KafkaCategoryEventSource` — shared journal, per-service Postgres views), `PekkoKafkaJournalService` template. Effort L–XL.
- **`kafka-cassandra-provisioning.md`** — infra: clones everywhere (compose→CI→cit→prod), single-node-first topology, capacity arithmetic, GitOps file lists, CI lanes (§9: per-PR clone-e2e with the same pinned images; weekly upstream canary), staged rollout with the spike result recorded.
- Related: `kafka-journal-insourcing.md` — how the fork came to be.

## Near-term work queue (from the plans)

1. **`ResetPointersApp`** admin tool: after a Redpanda topic recreation, `pointer2`/`metajournal` in Scylla reference stale offsets; upstream documents nothing. Needed for the single-node recovery runbook.
2. **Watch upstream #532** (Cassandra snapshot store) — when it ships, evaluate replacing pekko-persistence-cassandra for TEOB snapshots.
3. **Rebase cadence:** track upstream master; small frequent rebases beat big ones (this last one was cheap because upstream had only moved 3 build commits past v9.1.1).

Done 2026-07-20: the CI lanes from provisioning plan §9 are live — `e2e.yml` (clone-e2e: PR + master push + dispatch, compose stack in external-services mode, Harbor Docker-Hub proxy images) and `upstream-canary.yml` (weekly Monday 03:00 UTC + dispatch, testcontainers path on upstream pins via `KJ_KAFKA_IMAGE`/`KJ_CASSANDRA_IMAGE`). Both validated green: 149/149, ~10–13 min per run.

## Working conventions

- Commit style: lowercase, terse subject, no conventional-commit prefixes (see `git log`).
- `sbt check` is the green bar upstream used; unit tests via `sbt test` need no containers; `tests/` (integration) needs the compose stack or testcontainers.
- Formatting: scalafmt (`sbt scalafmtAll`), config in `.scalafmt.conf`.
- Two operational gotchas, learned the hard way: `Replicator.make` starts replication fibers **on resource allocation** (the yielded `F[Unit]` is only a completion handle — anything allocating it has a live replicator); sbt-forked test JVMs have `java @argsfile` cmdlines, so kill them **by classpath** (`lsof -p <pid> | grep kafka-journal`), never by process-name match.
- Drill knobs on `AppendReplicateApp`: `KJ_MODE=both|append|replicate`, `KJ_ID_PREFIX=<p>` — used for replicator stop/catch-up drills.
