package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.evolution.kafka.journal.cassandra.DeleteToExtension.*
import com.evolution.kafka.journal.cassandra.PartitionOffsetExtension.*
import com.evolution.kafka.journal.cassandra.SeqNrExtension.*
import com.evolution.kafka.journal.{DeleteTo, PartitionOffset, SeqNr}
import com.evolution.scassandra4.syntax.*
import com.evolution.scassandra4.{DecodeRow, EncodeRow}

/**
 * Represents metadata of particular journal.
 *
 * One piece of information could be found here is the metadata of last action stored to Cassandra.
 *
 * @param partitionOffset
 *   the offset where the last stored journal action is located in Kafka
 * @param segmentSize
 *   the segmentSize used to write a journal to Cassandra
 * @param seqNr
 *   the sequence number of the last event in journal stored in Cassandra
 * @param deleteTo
 *   the last sequence number deleted from journal if any
 * @param expiry
 *   the expiry time of the journal. On expiry the journal will be purged
 * @param recordId
 *   unique ID of metadata entry, created once and _never_ updated. Used to correlate metadata with
 *   events inserted while metadata is actual: if purging journal will by mistake delete not all
 *   events, then after inserting new events old ones will not be correlated with new metadata thus
 *   excluded from recovery. The field is optional, because it was added later and old records do
 *   not have it, thus while al least one record in `metajournal` table does not have `record_id`
 *   field - it should be optional. `metajournal` record can be _created_ with `record_id` field,
 *   but never updated!
 * @param cutoff
 *   the latest cutoff: the [[SeqNr]] of the latest replicated epoch record, see
 *   [[com.evolution.kafka.journal.EpochMark]]
 * @param purgedTo
 *   the floor of the current incarnation of the journal: every [[SeqNr]] at or below it was purged,
 *   or was never written because the incarnation started above it. A purge keeps the head as a
 *   tombstone with its [[SeqNr]] here, and the next incarnation continues above it
 */
private[journal] final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo] = none,
  expiry: Option[Expiry] = none,
  recordId: Option[RecordId] = none,
  cutoff: Option[SeqNr] = none,
  purgedTo: Option[SeqNr] = none,
) {

  /**
   * The journal was purged and nothing was appended since: the head is a tombstone
   */
  def purged: Boolean = purgedTo.exists { _ >= seqNr }
}

private[journal] object JournalHead {

  implicit def decodeRowJournalHead(
    implicit
    decode: DecodeRow[Option[Expiry]],
  ): DecodeRow[JournalHead] = {
    (row: GettableByName) =>
      {
        JournalHead(
          partitionOffset = row.decode[PartitionOffset],
          segmentSize = row.decode[SegmentSize],
          seqNr = row.decode[SeqNr],
          deleteTo = row.decode[Option[DeleteTo]]("delete_to"),
          expiry = row.decode[Option[Expiry]],
          recordId = row.decode[Option[RecordId]]("record_id"),
          cutoff = row.decode[Option[SeqNr]]("cutoff"),
          purgedTo = row.decode[Option[SeqNr]]("purged_to"),
        )
      }
  }

  implicit def encodeRowJournalHead(
    implicit
    encode: EncodeRow[Option[Expiry]],
  ): EncodeRow[JournalHead] = {
    new EncodeRow[JournalHead] {
      def apply[B <: SettableByName[B]](data: B, value: JournalHead): B = {
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome(value.deleteTo)
          .encode(value.expiry)
          .encodeSome("record_id", value.recordId)
          .encodeSome("cutoff", value.cutoff)
          .encodeSome("purged_to", value.purgedTo)
      }
    }
  }
}
