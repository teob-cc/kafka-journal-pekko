package com.evolution.scassandra4

import cats.effect.Sync
import cats.syntax.all.*
import cats.{FlatMap, ~>}
import com.datastax.oss.driver.api.core.metadata.Metadata as MetadataJ
import com.datastax.oss.driver.api.core.metadata.schema.{
  KeyspaceMetadata as KeyspaceMetadataJ,
  TableMetadata as TableMetadataJ,
}

import scala.jdk.CollectionConverters.*
import scala.jdk.javaapi.OptionConverters

/**
 * Mirrors `com.evolutiongaming.scassandra.Metadata` over driver 4's metadata API.
 *
 * Unlike driver 3's live `Metadata`, driver 4's is an immutable snapshot — construct a fresh
 * instance (via `CassandraSession.metadata`) whenever current schema state is needed.
 * `clusterName` is optional in driver 4; `userTypes` is not mirrored (unused downstream).
 */
trait Metadata[F[_]] {

  def clusterName: F[Option[String]]

  def keyspace(name: String): F[Option[KeyspaceMetadata[F]]]

  def keyspaces: F[List[KeyspaceMetadata[F]]]
}

object Metadata {

  def apply[F[_]: Sync](metadata: MetadataJ): Metadata[F] = {
    new Metadata[F] {

      val clusterName: F[Option[String]] = Sync[F].delay {
        OptionConverters.toScala(metadata.getClusterName)
      }

      def keyspace(name: String): F[Option[KeyspaceMetadata[F]]] = Sync[F].delay {
        OptionConverters
          .toScala(metadata.getKeyspace(name))
          .map { keyspace => KeyspaceMetadata[F](keyspace) }
      }

      val keyspaces: F[List[KeyspaceMetadata[F]]] = Sync[F].delay {
        metadata
          .getKeyspaces
          .values
          .asScala
          .toList
          .map { keyspace => KeyspaceMetadata[F](keyspace) }
      }
    }
  }

  implicit class MetadataOps[F[_]](val self: Metadata[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G,
    )(implicit
      G: FlatMap[G],
    ): Metadata[G] = new Metadata[G] {

      def clusterName: G[Option[String]] = f(self.clusterName)

      def keyspace(name: String): G[Option[KeyspaceMetadata[G]]] = {
        for {
          a <- f(self.keyspace(name))
        } yield {
          a.map { _.mapK(f) }
        }
      }

      def keyspaces: G[List[KeyspaceMetadata[G]]] = {
        for {
          a <- f(self.keyspaces)
        } yield {
          a.map { _.mapK(f) }
        }
      }
    }
  }
}

trait KeyspaceMetadata[F[_]] {

  def name: String

  def schema: F[String]

  def asCql: F[String]

  def table(name: String): F[Option[TableMetadata]]

  def tables: F[List[TableMetadata]]

  def durableWrites: Boolean

  def virtual: Boolean

  def replication: F[Map[String, String]]
}

object KeyspaceMetadata {

  def apply[F[_]: Sync](keyspaceMetadata: KeyspaceMetadataJ): KeyspaceMetadata[F] = {
    new KeyspaceMetadata[F] {

      val name: String = keyspaceMetadata.getName.asInternal

      val schema: F[String] = Sync[F].delay { keyspaceMetadata.describeWithChildren(true) }

      val asCql: F[String] = Sync[F].delay { keyspaceMetadata.describe(true) }

      def table(name: String): F[Option[TableMetadata]] = Sync[F].delay {
        OptionConverters
          .toScala(keyspaceMetadata.getTable(name))
          .map { table => TableMetadata(table) }
      }

      val tables: F[List[TableMetadata]] = Sync[F].delay {
        keyspaceMetadata
          .getTables
          .values
          .asScala
          .toList
          .map { table => TableMetadata(table) }
      }

      val durableWrites: Boolean = keyspaceMetadata.isDurableWrites

      val virtual: Boolean = keyspaceMetadata.isVirtual

      val replication: F[Map[String, String]] = Sync[F].delay {
        keyspaceMetadata.getReplication.asScala.toMap
      }
    }
  }

  implicit class KeyspaceMetadataOps[F[_]](val self: KeyspaceMetadata[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): KeyspaceMetadata[G] = new KeyspaceMetadata[G] {

      def name: String = self.name

      def schema: G[String] = f(self.schema)

      def asCql: G[String] = f(self.asCql)

      def table(name: String): G[Option[TableMetadata]] = f(self.table(name))

      def tables: G[List[TableMetadata]] = f(self.tables)

      def durableWrites: Boolean = self.durableWrites

      def virtual: Boolean = self.virtual

      def replication: G[Map[String, String]] = f(self.replication)
    }
  }
}

trait TableMetadata {
  def name: String
}

object TableMetadata {

  def apply(tableMetadata: TableMetadataJ): TableMetadata = new TableMetadata {
    val name: String = tableMetadata.getName.asInternal
  }
}
