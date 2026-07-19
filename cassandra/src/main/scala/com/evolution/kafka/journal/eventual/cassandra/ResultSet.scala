package com.evolution.kafka.journal.eventual.cassandra

import cats.effect.Spawn
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolutiongaming.sstream.FoldWhile.*
import com.evolutiongaming.sstream.Stream

/**
 * Generic paging fold, kept for its spec; production paging now delegates to
 * `StreamingCassandraSession` of scassandra4.
 */
object ResultSet {

  def apply[F[_]: Spawn, A](
    fetch: F[Unit],
    fetched: F[Boolean],
    next: F[List[A]],
  ): Stream[F, A] = new Stream[F, A] {

    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]): F[Either[L, R]] = {

      l.tailRecM[F, Either[L, R]] { l =>
        def apply(rows: List[A]): F[Either[L, Either[L, R]]] = {
          for {
            result <- rows.foldWhileM(l)(f)
          } yield {
            result.asRight[L]
          }
        }

        def fetchAndApply(rows: List[A]) = {
          for {
            fetching <- fetch.start
            result <- rows.foldWhileM(l)(f)
            result <- result match {
              case l: Left[L, R] => fetching.joinWithNever as l.rightCast[Either[L, R]]
              case r: Right[L, R] => r.leftCast[L].asRight[L].pure[F]
            }
          } yield result
        }

        for {
          fetched <- fetched
          rows <- next
          result <- if (fetched) apply(rows) else fetchAndApply(rows)
        } yield result
      }
    }
  }
}
