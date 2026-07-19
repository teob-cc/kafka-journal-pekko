package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.{Async, Concurrent, Resource}
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, Row, SimpleStatement, Statement}
import com.evolution.kafka.journal.JournalError
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolution.scache.Cache
import com.evolution.scassandra4
import com.evolution.scassandra4.StreamingCassandraSession.*
import com.evolution.scassandra4.util.FromCompletionStage
import com.evolutiongaming.catshelper.{MonadThrowable, Runtime}
import com.evolutiongaming.sstream.Stream

trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement[?]): Stream[F, Row]

  /**
   * Fresh schema metadata snapshot of the current session.
   *
   * Driver 4 metadata is an immutable snapshot taken at the call, unlike the live view of driver 3,
   * hence metadata lives on the session rather than on [[CassandraCluster]].
   */
  def metadata: F[CassandraMetadata[F]]

  def unsafe: scassandra4.CassandraSession[F]

  final def execute(statement: String): Stream[F, Row] = execute(SimpleStatement.newInstance(statement))
}

object CassandraSession {

  def apply[F[_]](
    implicit
    F: CassandraSession[F],
  ): CassandraSession[F] = F

  // retries are not decorated per statement anymore: driver 4 configures the retry policy
  // at session level, see the CassandraConfig.retries -> NextHostRetryPolicy translation
  def apply[F[_]](
    session: CassandraSession[F],
    trace: Boolean = false,
  ): CassandraSession[F] = {
    session.configured(trace)
  }

  private def apply[F[_]: Async: FromCompletionStage](
    session: scassandra4.CassandraSession[F],
  ): CassandraSession[F] = {
    new CassandraSession[F] {

      def prepare(query: String): F[PreparedStatement] = session.prepare(query)

      def execute(statement: Statement[?]): Stream[F, Row] = session.executeStream(statement)

      def metadata: F[CassandraMetadata[F]] = {
        for {
          metadata <- session.metadata
        } yield {
          CassandraMetadata[F](scassandra4.Metadata[F](metadata))
        }
      }

      def unsafe: scassandra4.CassandraSession[F] = session
    }
  }

  def make[F[_]: Async: Parallel: FromCompletionStage](
    session: scassandra4.CassandraSession[F],
  ): Resource[F, CassandraSession[F]] = {
    apply[F](session)
      .enhanceError
      .cachePrepared
  }

  implicit class CassandraSessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    def configured(
      trace: Boolean,
    ): CassandraSession[F] = new CassandraSession[F] {

      def prepare(query: String): F[PreparedStatement] = {
        self.prepare(query)
      }

      def execute(statement: Statement[?]): Stream[F, Row] = {
        val configured = statement
          .setIdempotent(true)
          .setTracing(trace)
        self.execute(configured)
      }

      def metadata: F[CassandraMetadata[F]] = self.metadata

      def unsafe: scassandra4.CassandraSession[F] = self.unsafe
    }

    def cachePrepared(implicit
      F: Concurrent[F],
      parallel: Parallel[F],
      runtime: Runtime[F],
    ): Resource[F, CassandraSession[F]] = {
      for {
        cache <- Cache.loading[F, String, PreparedStatement]
      } yield {
        new CassandraSession[F] {

          def prepare(query: String): F[PreparedStatement] = {
            cache.getOrUpdate(query) { self.prepare(query) }
          }

          def execute(statement: Statement[?]): Stream[F, Row] = self.execute(statement)

          def metadata: F[CassandraMetadata[F]] = self.metadata

          def unsafe: scassandra4.CassandraSession[F] = self.unsafe
        }
      }
    }

    def enhanceError(
      implicit
      F: MonadThrowable[F],
    ): CassandraSession[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"CassandraSession.$msg failed with $cause", cause).raiseError[F, A]
      }

      new CassandraSession[F] {

        def prepare(query: String): F[PreparedStatement] = {
          self
            .prepare(query)
            .handleErrorWith { a => error(s"prepare query: $query", a) }
        }

        def execute(statement: Statement[?]): Stream[F, Row] = {
          self
            .execute(statement)
            .handleErrorWith { (a: Throwable) => error[Row](s"execute statement: $statement", a).toStream }
        }

        def metadata: F[CassandraMetadata[F]] = {
          self
            .metadata
            .handleErrorWith { a => error("metadata", a) }
        }

        def unsafe: scassandra4.CassandraSession[F] = self.unsafe
      }
    }
  }
}
