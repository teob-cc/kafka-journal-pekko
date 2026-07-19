package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.evolution.scassandra4
import com.evolution.scassandra4.util.FromCompletionStage
import com.evolution.scassandra4.{CassandraClusterOf, CassandraConfig}

trait CassandraCluster[F[_]] {

  def session: Resource[F, CassandraSession[F]]
}

object CassandraCluster {

  def apply[F[_]](
    implicit
    F: CassandraCluster[F],
  ): CassandraCluster[F] = F

  def apply[F[_]: Async: Parallel: FromCompletionStage](
    cluster: scassandra4.CassandraCluster[F],
  ): CassandraCluster[F] = new CassandraCluster[F] {

    def session: Resource[F, CassandraSession[F]] = {
      for {
        session <- cluster.connect
        session <- CassandraSession.make[F](session)
      } yield {
        CassandraSession(session)
      }
    }
  }

  def make[F[_]: Async: Parallel: FromCompletionStage](
    config: CassandraConfig,
    cassandraClusterOf: CassandraClusterOf[F],
    retries: Int,
  ): Resource[F, CassandraCluster[F]] = {

    for {
      // driver 4 configures the retry policy at session level rather than per statement
      cluster <- cassandraClusterOf(config.copy(retries = retries.some))
    } yield {
      apply[F](cluster)
    }
  }
}
