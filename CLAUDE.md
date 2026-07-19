# CLAUDE.md

Agent takeover context for this repository. Read this first; it links everything else.

## What this repository is

The **lambda-house fork of [evolution-gaming/kafka-journal](https://github.com/evolution-gaming/kafka-journal)** — a Kafka-first event journal with Cassandra as the eventual long-term store, exposing an Apache Pekko persistence plugin. We insourced it, migrated it to **Scala 3**, and publish it under **`cc.lambdahouse`** coordinates to our Nexus. It is the intended scalable journal backend for the TEOB event-sourcing framework (`~/work/teob`).

Key facts:

- **Vendored dependencies:** all 17 Evolution libraries (skafka, scassandra, cats-helper, scache, cassandra-sync, smetrics, …) live as *source* in `libs/` (~21.8k lines). A quirk in any client library is a patchable event here, not an upstream ticket. See `NOTICE.md` for provenance.
- **Clone platform:** we run this against **Redpanda** (Kafka API) and **ScyllaDB** (CQL) — *not* JVM Kafka/Cassandra. Apache Kafka and Cassandra exist in our world only as client protocols. Gating spike 2026-07-19: full `IntegrationSuite` **149/149 green** against Redpanda v25.1.1 + Scylla 2025.1, zero client patches. Local stack: `docker compose up -d --wait` (see `docker-compose.yml`), then `KAFKA_JOURNAL_EXTERNAL_SERVICES=true sbt tests/test`. **Full suite runs (anything including the Pekko TCK) need fresh volumes — `docker compose down -v` first**: the TCK uses fixed persistence ids and assumes a virgin journal; stale data shows up as JSON-decode fallback failures in the replay specs, not as an honest "dirty state" error.
- **History discipline:** our commits are a short readable stack replayed on top of upstream master (insourcing → scala-3 migration → v9.1.1 catch-up → stabilize → identity → publish pipeline → clone spike). We **rebase onto upstream**, never merge. After a rebase, push is `git push --force-with-lease origin master`.

## Remotes — careful

| remote | URL | role |
|---|---|---|
| `origin` | lambda-house/kafka-journal-pekko | ours — push here, `--force-with-lease` after rebases |
| `upstream` | evolution-gaming/kafka-journal | **fetch-only** — push URL set to `DISABLED` |

CI publishes to Nexus **on tags only** (`.github/workflows/publish-to-nexus.yml`), so pushing `master` is safe; tagging releases.

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

1. **Push the rebase** (`--force-with-lease origin master`) once the post-rebase `IntegrationSuite` validation run is green — may already be done; check `git status` vs `origin/master`.
2. **CI clone-e2e lane** (provisioning plan §9): per-PR job running `IntegrationSuite` with `KAFKA_JOURNAL_EXTERNAL_SERVICES=true` against the compose images; weekly upstream-canary lane keeping the testcontainers path (`cassandra:4.1.8` + `apache/kafka-native:3.8.1`). Runs on the lambda-house self-hosted DinD runners (~4–5 Gi peak, fits the 6 Gi limit).
3. **`ResetPointersApp`** admin tool: after a Redpanda topic recreation, `pointer2`/`metajournal` in Scylla reference stale offsets; upstream documents nothing. Needed for the single-node recovery runbook.
4. **Watch upstream #532** (Cassandra snapshot store) — when it ships, evaluate replacing pekko-persistence-cassandra for TEOB snapshots.
5. **Rebase cadence:** track upstream master; small frequent rebases beat big ones (this last one was cheap because upstream had only moved 3 build commits past v9.1.1).

## Working conventions

- Commit style: lowercase, terse subject, no conventional-commit prefixes (see `git log`).
- `sbt check` is the green bar upstream used; unit tests via `sbt test` need no containers; `tests/` (integration) needs the compose stack or testcontainers.
- Formatting: scalafmt (`sbt scalafmtAll`), config in `.scalafmt.conf`.
- Two operational gotchas, learned the hard way: `Replicator.make` starts replication fibers **on resource allocation** (the yielded `F[Unit]` is only a completion handle — anything allocating it has a live replicator); sbt-forked test JVMs have `java @argsfile` cmdlines, so kill them **by classpath** (`lsof -p <pid> | grep kafka-journal`), never by process-name match.
- Drill knobs on `AppendReplicateApp`: `KJ_MODE=both|append|replicate`, `KJ_ID_PREFIX=<p>` — used for replicator stop/catch-up drills.
