package com.evolution.kafka.journal.eventual.cassandra

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.cql.{ColumnDefinitions, Row}
import com.datastax.oss.driver.api.core.detach.AttachmentPoint
import com.datastax.oss.driver.api.core.{CqlIdentifier, ProtocolVersion}

import java.nio.ByteBuffer

/**
 * Unlike driver 3's `Row`, driver 4's is mostly default methods on top of these few abstract
 * primitives; typed getters like `getBoolean(name)` can still be overridden directly in tests.
 */
class RowStub extends Row {

  def notImplemented(): Nothing = sys.error("method is not implemented")

  override def getColumnDefinitions: ColumnDefinitions = notImplemented()
  override def size(): Int = notImplemented()
  override def getType(i: Int): DataType = notImplemented()
  override def getType(name: String): DataType = notImplemented()
  override def getType(id: CqlIdentifier): DataType = notImplemented()
  override def getBytesUnsafe(i: Int): ByteBuffer = notImplemented()
  override def firstIndexOf(name: String): Int = notImplemented()
  override def firstIndexOf(id: CqlIdentifier): Int = notImplemented()
  override def codecRegistry(): CodecRegistry = notImplemented()
  override def protocolVersion(): ProtocolVersion = notImplemented()
  override def isDetached: Boolean = notImplemented()
  override def attach(attachmentPoint: AttachmentPoint): Unit = notImplemented()
}
