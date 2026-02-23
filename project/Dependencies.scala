import sbt.*

object Dependencies {
  val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.19"
  val CassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.11.5"
  val KafkaClients = "org.apache.kafka" % "kafka-clients" % "3.4.0"
  val PlayJson = "com.typesafe.play" %% "play-json" % "2.10.8"
  val Jsoniter = "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.36.7"

  object Cats {
    val Core = "org.typelevel" %% "cats-core" % "2.13.0"
    val Effect = "org.typelevel" %% "cats-effect" % "3.6.3"
  }

  object Logback {
    private val version = "1.5.32"
    val Core = "ch.qos.logback" % "logback-core" % version
    val Classic = "ch.qos.logback" % "logback-classic" % version
  }

  object Slf4j {
    private val version = "2.0.17"
    val Api = "org.slf4j" % "slf4j-api" % version
    val Log4jOverSlf4j = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object Pekko {
    private val version = "1.4.0"
    val Actor = "org.apache.pekko" %% "pekko-actor" % version
    val Testkit = "org.apache.pekko" %% "pekko-testkit" % version
    val Stream = "org.apache.pekko" %% "pekko-stream" % version
    val Persistence = "org.apache.pekko" %% "pekko-persistence" % version
    val PersistenceTck = "org.apache.pekko" %% "pekko-persistence-tck" % version
    val Slf4j = "org.apache.pekko" %% "pekko-slf4j" % version
  }

  object Scodec {
    val Bits = "org.scodec" %% "scodec-bits" % "1.2.4"
    val Core = "org.scodec" %% "scodec-core" % "2.3.3"
  }

  object Pureconfig {
    private val version = "0.17.10"
    val Core = "com.github.pureconfig" %% "pureconfig-core" % version
    val Cats = "com.github.pureconfig" %% "pureconfig-cats" % version
    val Generic = "com.github.pureconfig" %% "pureconfig-generic-scala3" % version
  }

  object Circe {
    private val version = "0.14.15"
    val Core = "io.circe" %% "circe-core" % version
    val Generic = "io.circe" %% "circe-generic" % version
    val Jawn = "io.circe" %% "circe-jawn" % version
  }

  object TestContainers {
    private val version = "1.21.4"
    val Kafka = "org.testcontainers" % "kafka" % version
    val Cassandra = "org.testcontainers" % "cassandra" % version
  }
}
