import Dependencies.*
import com.typesafe.tools.mima.core.*
import sbt.Package.ManifestAttributes

lazy val commonSettings = Seq(
  organization := "com.evolution",
  organizationName := "Evolution",
  organizationHomepage := Some(url("https://evolution.com")),
  homepage := Some(url("https://github.com/lambda-house/kafka-journal-pekko")),
  startYear := Some(2018),
  crossScalaVersions := Seq("3.8.1"),
  scalaVersion := crossScalaVersions.value.head,
  scalacOptions ++= Seq(
    "-release:17",
    "-deprecation",
    "-Xkind-projector:underscores",
    "-no-indent",
    "-explain",
    "-explain-types",
  ),
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits", "-no-link-warnings"),
  Compile / doc / scalacOptions -= "-Xfatal-warnings",
  licenses := Seq(("MIT", url("https://opensource.org/licenses/MIT"))),
  libraryDependencySchemes ++= Seq(
    "org.scala-lang.modules" %% "scala-xml" % "always",
  ),
  autoAPIMappings := true,
  versionScheme := Some("early-semver"),
  versionPolicyIntention := Compatibility.BinaryCompatible,
  packageOptions := {
    Seq(
      ManifestAttributes(
        ("Implementation-Version", (ThisProject / version).value),
      ),
    )
  },
  versionPolicyIgnored ++= Seq(
    // add libraries here that are known to be binary compatible, like:
//    "com.typesafe" %% "ssl-config-core",
  ),
)

ThisBuild / mimaBinaryIssueFilters ++= Seq(
  // add mima check exceptions here, like:
//  ProblemFilters.exclude[IncompatibleMethTypeProblem](
//    "com.evolution.kafka.journal.replicator.TopicReplicator#ConsumerOf.make",
//  ),
)

ThisBuild / libraryDependencySchemes ++= Seq(
  // add mima check overrides for RC-level libraries, like:
//  "org.tpolecat" %% "doobie-core" % VersionScheme.Always,
)

val alias: Seq[sbt.Def.Setting[?]] =
  addCommandAlias("fmt", "all scalafmtAll scalafmtSbt") ++
    addCommandAlias(
      "check",
      "all versionPolicyCheck Compile/doc scalafmtCheckAll scalafmtSbtCheck",
    ) ++
    addCommandAlias("build", "all compile test")

lazy val root = project
  .in(file("."))
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .settings(publish / skip := true)
  .settings(alias)
  .aggregate(
    libs,
    core,
    journal,
    snapshot,
    replicator,
    cassandra,
    eventualCassandra,
    snapshotCassandra,
    circe,
    persistence,
    persistenceCirce,
    tests,
    ScalaTestIO,
  )

lazy val libs = project
  .in(file("libs"))
  .settings(name := "kafka-journal-libs")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      Cats.Core,
      Cats.Effect,
      Pekko.Actor,
      Pekko.Testkit % Test,
      Pureconfig.Core,
      Pureconfig.Cats,
      Pureconfig.Generic,
      PlayJson,
      Jsoniter,
      KafkaClients,
      CassandraDriver,
      Scodec.Bits,
      Scodec.Core,
      Slf4j.Api,
      ScalaTest,
      Logback.Classic % Test,
    ),
  )

lazy val core = project
  .in(file("core"))
  .settings(name := "kafka-journal-core")
  .settings(commonSettings)
  .dependsOn(libs, ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      PlayJson,
      Pureconfig.Core,
      Cats.Core,
      Cats.Effect,
      Scodec.Bits,
      Scodec.Core,
    ),
  )

lazy val journal = project
  .in(file("journal"))
  .settings(name := "kafka-journal")
  .settings(commonSettings)
  .dependsOn(libs, core % "test->test;compile->compile", ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      KafkaClients,
      PlayJson,
      Pureconfig.Core,
      Pureconfig.Cats,
      Pureconfig.Generic,
      Cats.Core,
      Cats.Effect,
      ScalaTest % Test,
      Logback.Core % Test,
      Logback.Classic % Test,
    ),
  )

lazy val snapshot = project
  .in(file("snapshot"))
  .settings(name := "kafka-journal-snapshot")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(libraryDependencies ++= Seq(ScalaTest % Test))

lazy val persistence = project
  .in(file("persistence"))
  .settings(name := "kafka-journal-persistence")
  .settings(commonSettings)
  .dependsOn(libs, journal % "test->test;compile->compile", eventualCassandra, snapshotCassandra)
  .settings(
    libraryDependencies ++= Seq(
      Pekko.Persistence,
      Pekko.Testkit % Test,
    ),
  )

lazy val tests = project
  .in(file("tests"))
  .settings(name := "kafka-journal-tests")
  .settings(commonSettings)
  .settings(
    Seq(
      publish / skip := true,
      Test / fork := true,
      Test / parallelExecution := false,
      Test / javaOptions ++= Seq("-Xms3G", "-Xmx3G"),
    ),
  )
  .dependsOn(persistence % "test->test;compile->compile", persistenceCirce, replicator)
  .settings(
    libraryDependencies ++= Seq(
      TestContainers.Cassandra % Test,
      TestContainers.Kafka % Test,
      ScalaTest % Test,
      Pekko.PersistenceTck % Test,
      Pekko.Slf4j % Test,
      Slf4j.Log4jOverSlf4j % Test,
      Logback.Core % Test,
      Logback.Classic % Test,
    ),
  )

lazy val replicator = project
  .in(file("replicator"))
  .settings(name := "kafka-journal-replicator")
  .settings(commonSettings)
  .dependsOn(
    journal % "test->test",
    eventualCassandra,
    ScalaTestIO % Test,
  )
  .settings(libraryDependencies ++= Seq(
    Logback.Core % Test,
    Logback.Classic % Test,
    ScalaTest % Test,
  ))

lazy val cassandra = project
  .in(file("cassandra"))
  .settings(name := "kafka-journal-cassandra")
  .settings(commonSettings)
  .dependsOn(libs, core, ScalaTestIO % Test)
  .settings(
    libraryDependencies ++= Seq(
      Pureconfig.Generic,
    ),
  )

lazy val eventualCassandra = project
  .in(file("eventual-cassandra"))
  .settings(name := "kafka-journal-eventual-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra % "test->test;compile->compile", journal % "test->test;compile->compile")

lazy val snapshotCassandra = project
  .in(file("snapshot-cassandra"))
  .settings(name := "kafka-journal-snapshot-cassandra")
  .settings(commonSettings)
  .dependsOn(cassandra, snapshot % "test->test;compile->compile")

lazy val circe = project
  .in(file("circe"))
  .settings(name := "kafka-journal-circe")
  .settings(commonSettings)
  .dependsOn(journal % "test->test;compile->compile")
  .settings(libraryDependencies ++= Seq(Circe.Core, Circe.Generic, Circe.Jawn))

lazy val persistenceCirce = project
  .in(file("persistence-circe"))
  .settings(name := "kafka-journal-persistence-circe")
  .settings(commonSettings)
  .dependsOn(circe, persistence % "test->test;compile->compile")

lazy val ScalaTestIO = project
  .in(file("scalatest-io"))
  .settings(name := "kafka-journal-scalatest-io")
  .settings(commonSettings)
  .dependsOn(libs)
  .settings(publish / skip := true)
  .settings(libraryDependencies ++= Seq(ScalaTest, Cats.Core, Cats.Effect))

// not part of aggregate, tests can be run only manually
lazy val benchmark = project
  .dependsOn(journal % "test->test;compile->compile")
  .enablePlugins(JmhPlugin)
  .settings(commonSettings)
  .settings(
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
  )
