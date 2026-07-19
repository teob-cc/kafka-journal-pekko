# Catch-up to upstream v9.1.1

Date: 2026-07-19. Base: the insourced Scala 3 fork (see `SCALA3_MIGRATION.md`), which was cut from upstream ~v6.2.0. This change brings the fork to functional parity with [evolution-gaming/kafka-journal v9.1.1](https://github.com/evolution-gaming/kafka-journal/releases/tag/v9.1.1).

## Upstream features ported

The real code delta v6.2.0→v9.1.1 was ~25 files (+336/−112); the remaining ~50 upstream commits were version bumps of libraries this fork vendors in `libs/`.

- **`Journal.mark` operation** (upstream [#888](https://github.com/evolution-gaming/kafka-journal/pull/888)) — produce a mark record without appending events.
- **`Journals` as actor-system extension** (upstream [#889](https://github.com/evolution-gaming/kafka-journal/pull/889)) — new `KafkaJournalsRef` in `persistence/`; works only with a single kafka-journal plugin config per actor system.
- skafka 20.x call-site adaptations in `journal/`, `replicator/`, `tests/`.

Port mechanics: `git diff v6.2.0 origin/master` with module paths rewritten (`pekko/persistence` → `persistence/` etc.), applied via `git apply --3way`. Three files needed manual conflict resolution (`JournalSpec`, `JournalAdapter`, `KafkaJournal`) — all cases of our `(using ...)` syntax vs upstream renames.

## Vendored library refresh (`libs/`)

Refreshed by 3-way merge in each library's upstream repo: branch at the previously-captured tag, overlay our migrated sources, `git merge <new tag>`, resolve, sync back.

| Library | was | now | Notes |
|---|---|---|---|
| skafka | 19.0.0 | **20.2.1** | kafka-clients 3.x→4.x port. Upstream removed deprecated `sendOffsetsToTransaction(offsets, groupId)` overload, removed `RebalanceListener`/`SerialListeners`, reworked `ConsumerConverters`. One kafka-clients 4.3 deprecation (`ConsumerGroupMetadataJ` constructor) suppressed with `@nowarn`, mirroring upstream behaviour. |
| scache | 5.1.4 | **6.0.1** | Upstream restructured into `scache` + `cache-adt` modules and natively cross-builds Scala 3, so pristine upstream sources were taken (new file: `Directive.scala`). Re-applied 29 `@unchecked` pattern annotations (E092) that upstream's 3.3 build does not flag but 3.8 does. |
| scassandra | 5.4.0 | **5.6.0** | Mostly upstream reformat; `PureconfigUtils` → `PureconfigSyntax` rename. |
| play-json-jsoniter | 1.2.3 | **1.3.0** | Zero source changes; the release is the play-json `com.typesafe.play` → `org.playframework` move (external dep bump below). |
| smetrics | 2.4.3 | 2.4.5 | No changes in vendored (core) sources — 2.4.5 only touched the sttp module, not vendored. |
| pekko-extension (serialization, test-actor) | 1.3.1 | 2.0.0 | No changes in vendored modules — 2.0.0 changed only cluster-sharding extensions, not vendored. |

Unchanged upstream, untouched: cats-helper, sstream, retry, random, resource-pool, cassandra-sync, hostname, executor-tools, config-tools, nel.

Not vendored (deliberately, as before): skafka `metrics`/`metrics_prometheus_v1`/`play-json` modules, smetrics `prometheus`/`doobie`/`http4s` modules.

## External dependency bumps

| Dependency | was | now |
|---|---|---|
| Scala | 3.8.1 | **3.8.2** |
| Pekko | 1.4.0 | **1.6.0** |
| cats-effect | 3.6.3 | **3.7.0** |
| kafka-clients | 3.4.0 | **4.3.1** |
| play-json | com.typesafe.play 2.10.8 | **org.playframework 3.0.6** |
| jsoniter-scala-core | 2.36.7 | 2.39.1 |
| circe | 0.14.15 | 0.14.16 |
| scodec-bits | 1.2.4 | 1.2.5 |
| logback / slf4j | 1.5.32 / 2.0.17 | 1.5.38 / 2.0.18 |
| scalatest | 3.2.19 | 3.2.20 |

## Wire-format compatibility canary

`journal/src/test/scala/.../ActionHeaderJsonSpec.scala` (upstream code, kept) round-trips `ActionHeader` against golden JSON files in `journal/src/test/resources/` — this is the guard against accidental Kafka record-format drift. Any failure there after a sync/refresh means reader/writer version skew during rolling deploys is unsafe.
