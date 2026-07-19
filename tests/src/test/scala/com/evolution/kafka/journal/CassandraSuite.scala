package com.evolution.kafka.journal

import cats.effect.IO
import com.evolution.scassandra4.CassandraClusterOf
import com.evolutiongaming.catshelper.CatsHelper.*

object CassandraSuite {
  import cats.effect.unsafe.implicits.global

  implicit lazy val cassandraClusterOf: CassandraClusterOf[IO] = CassandraClusterOf.of[IO].toTry.get
}
