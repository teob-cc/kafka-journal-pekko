# Scala 3 Migration Report

Migration of kafka-journal from Scala 2.13.18 / 3.3.7 cross-build to Scala 3.8.1 only.

**366 files changed, 22,123 insertions, 4,789 deletions.**

---

## Part 1: Structural Changes

### 1.1 Insourced Dependencies into `libs/` Module

All Evolution/EvolutionGaming library dependencies were insourced as source code into a new `libs/` module (196 files, ~21,800 lines). This eliminated the need for the JFrog/Evolution artifact resolver entirely.

**Libraries insourced:**
- `cats-helper` (40+ files: Log, FromFuture, ToFuture, MeasureDuration, Serial, Schedule, etc.)
- `skafka` (Consumer, Producer, configs, converters, metrics)
- `scassandra` (CassandraSession, codecs, configs, syntax)
- `sstream` (Stream, FoldWhile)
- `scache` (Cache, LoadingCache, ExpiringCache, PartitionedCache, SerialMap)
- `smetrics` (Counter, Gauge, Histogram, Summary, CollectorRegistry)
- `retry` (Retry, Strategy, Decision, Sleep)
- `random` (Random, SeedOf, RandomStateOf)
- `resource-pool` (ResourcePool)
- `cassandra-sync` (CassandraSync)
- `play-json-jsoniter` (PlayJsonJsoniter, JsonValueCodecJsValue)
- `pekko-extension-serialization` (SerializedMsg, SerializedMsgSerializer)
- `pekko-extension-test-actor` (PekkoActorSuite)
- `hostname` (HostName)
- `executor-tools` (CurrentThreadExecutionContext, ExecutionContextExecutorServiceFactory)
- `config-tools` (ConfigHelper)
- `nel` (Nel)

**New `libs/` module dependencies (external only):**
cats, cats-effect, pekko-actor, pureconfig, play-json, jsoniter-scala-core, kafka-clients, cassandra-driver-core, scodec, slf4j, scalatest.

### 1.2 Removed Akka Modules

Dropped the entire `akka/` subtree (67 files deleted): `akkaPersistence`, `akkaPersistenceCirce`, `akkaTests`. The project now only supports Pekko.

### 1.3 Flattened Pekko Modules

Renamed `pekko/persistence` → `persistence/`, `pekko/persistence-circe` → `persistence-circe/`, `pekko/tests` → `tests/` (67 files renamed). Module names changed accordingly:
- `kafka-journal-pekko-persistence` → `kafka-journal-persistence`
- `kafka-journal-pekko-persistence-circe` → `kafka-journal-persistence-circe`

### 1.4 Build Configuration

**`build.sbt`:**
- `crossScalaVersions` changed from `Seq("2.13.18", "3.3.7")` to `Seq("3.8.1")`
- Removed all `crossSettings(if2 = ..., if3 = ...)` conditionals
- `-Ykind-projector:underscores` → `-Xkind-projector:underscores` (Scala 3.8 flag change)
- Kept `-no-indent`, `-explain`, `-explain-types`
- Removed `com.evolution:sbt-scalac-opts-plugin` (Scala 2 only)
- Removed `com.evolution:sbt-artifactory-plugin` and `publishTo := Resolver.evolutionReleases`
- Removed `kind-projector` compiler plugin (built into Scala 3)
- Removed `scala-java8-compat` dependency scheme
- All modules now `dependsOn(libs)` instead of declaring individual library deps

**`project/Dependencies.scala`:**
- Removed 17 Evolution library dependencies (SKafka, CatsHelper, SCache, etc.)
- Removed Akka dependency group entirely
- Removed Scodec Scala2/Scala3 split → single `Scodec.Core`
- Removed Pureconfig Scala2/Scala3 split → single `Pureconfig.Generic` (generic-scala3)
- Added `Jsoniter` dependency (was transitive via play-json-jsoniter)

**`.scalafmt.conf`:** `runner.dialect` changed from `scala213source3` to `scala3`

**`.jvmopts`:** Added with `-Xms16G -Xmx16G` — needed for compiling 196 insourced files with heavy implicit resolution.

**`.sbtopts`:** Removed (superseded by `.jvmopts`).

**`project/build.properties`:** sbt 1.12.3 → 1.12.4

**CI:** Replaced `ci.yml` + `release.yml` (GitHub Actions with matrix build for 2.13/3.3) with `publish-to-nexus.yml` (self-hosted runner, single Scala version).

---

## Part 2: Scala 3 Compilation Errors (libs/)

The insourced libraries were originally written for Scala 2.13. These changes were needed to make them compile under Scala 3.

### 2.1 Wildcard Types: `_` → `?`

Scala 3 uses `?` instead of `_` for wildcard types in type positions.

```scala
// Before
Class[_], Foo[_ <: Bar]
// After
Class[?], Foo[? <: Bar]
```

~15 occurrences across libs.

### 2.2 `scala.compat.java8.DurationConverters` → `scala.jdk.DurationConverters`

The Java 8 compat library is unnecessary on Scala 3 (requires JDK 17+). Replaced with stdlib:

```scala
// Before
import scala.compat.java8.DurationConverters._
duration.toScala
// After
import scala.jdk.DurationConverters._
duration.toScala
```

### 2.3 Explicit `using` for Context-Bound Parameter Passing

Scala 3 requires `using` when passing implicit/context parameters explicitly. This was the most pervasive change (~69 locations in libs, ~18 in other modules).

```scala
// Before
FromFuture[F].apply { future }(executor)
MeasureDuration.fromClock(Clock[F])
// After
FromFuture[F].apply { future }(using executor)
MeasureDuration.fromClock(using Clock[F])
```

Affected patterns: `FromFuture`, `ToFuture`, `MeasureDuration`, `Memoize`, `SerialRef`, `Producer.send`, `CollectorRegistry` metric calls, `Codec.apply`, `Stream` operations.

### 2.4 `for/yield` with `-no-indent`

With the `-no-indent` flag, `for/yield` expressions that use Scala 3's braceless syntax need explicit braces around the yield body:

```scala
// This fails with -no-indent:
for a <- next(32)
yield a / floatUnit

// Fixed:
for a <- next(32)
yield { a / floatUnit }
```

Applied in `Random.scala`.

---

## Part 3: Scala 3 Compiler Warnings (~177 total)

After compilation succeeded, ~177 warnings remained. All were fixed to achieve zero-warning compilation.

### 3.1 Vararg Syntax: `x: _*` → `x*` (7 locations)

```scala
// Before
statement.bind(values: _*)
// After
statement.bind(values*)
```

Files: `CassandraSession.scala`, `Log.scala`, `MetricsConfig.scala`, `CommonConfig.scala`, `ProducerConfig.scala`, `ConsumerConfig.scala`.

### 3.2 Alphanumeric Infix Operators (~50 locations)

Scala 3 warns when alphanumeric methods not declared `infix` are used in infix position.

```scala
// Before
config hasPath path
range contains seqNr
str equalsIgnoreCase other
xs mkString ","
// After
config.hasPath(path)
range.contains(seqNr)
str.equalsIgnoreCase(other)
xs.mkString(",")
```

Methods affected: `contains`, `intersects`, `in`, `to`, `diff`, `plus`, `minus`, `isBefore`, `concat`, `fallbackTo`, `mkString`, `getOrElse`, `equalsIgnoreCase`, `noneIf`, `prefixed`, `getString`, `hasPath`.

### 3.3 Unchecked Type Tests at Runtime (~30 locations)

Pattern matches on generic types cannot be checked at runtime due to type erasure. Fixed by annotating patterns with `@unchecked`:

```scala
import scala.unchecked
// Before
case v: EntryState.Value[F, V] => ...
// After
case v: (EntryState.Value[F, V] @unchecked) => ...
```

Files: `LoadingCache.scala`, `ExpiringCache.scala`, `Cache.scala`, `CacheMetered.scala`, `RebalanceCallback.scala`, `PartitionCacheSpec.scala`.

### 3.4 Deprecation Warnings (~23 locations)

**API updates:**
- `TraversableOnce` → `IterableOnce` in `ParallelHelper.scala`
- `scala.collection.JavaConverters` → `scala.jdk.CollectionConverters` in `ConfigHelper.scala`

**Suppressed with `@nowarn` (deprecated APIs within our own insourced code):**
- `Stream.scala` — deprecated `Cmd` class and methods referencing it
- `syntax.scala` — deprecated `ToCql.Ops`
- `Producer.scala`, `ProducerLogging.scala` — deprecated `sendOffsetsToTransaction`

### 3.5 Non-Exhaustive Match on Java Enums (3 locations)

Scala 3 cannot verify exhaustiveness of pattern matches on Java enums (e.g., `java.util.concurrent.TimeUnit`).

```scala
import scala.unchecked
// Before
self match { case DAYS => ... }
// After
(self: @unchecked) match { case DAYS => ... }
```

Files: `TimeUnitHelper.scala` (2 matches), `TemporalHelper.scala` (added wildcard `case _`).

### 3.6 Feature Warnings: Implicit Conversions (2 locations)

Scala 3 requires an explicit language import when defining implicit conversions:

```scala
import scala.language.implicitConversions
```

Files: `MeasureDurationSyntax.scala`, `syntax.scala` (scassandra).

### Warning Summary

| Category | Count | Fix |
|---|---|---|
| Vararg syntax `x: _*` → `x*` | 7 | Mechanical replacement |
| Implicit `using` clause | 44 | Add `using` keyword |
| Alphanumeric infix operators | ~50 | Convert to dot-call syntax |
| Unchecked type tests | ~30 | `@unchecked` on patterns |
| Deprecations | ~23 | Update API or `@nowarn` |
| Non-exhaustive Java enum match | 3 | `@unchecked` on scrutinee or wildcard |
| Implicit conversion feature | 2 | Language import |
| **Total** | **~177** | |

---

## Result

- Scala version: **3.8.1** (single target, no cross-build)
- `sbt clean Test/compile`: **0 errors, 0 warnings**
- `sbt test`: **149 tests pass, 20 ignored** (expected — integration tests requiring Cassandra/Kafka)
