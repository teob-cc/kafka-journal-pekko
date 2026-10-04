package com.evolution.kafka.journal

import com.evolutiongaming.skafka.{Offset, Partition}

/**
 * The replicated head of a journal.
 *
 * @param partitionOffset
 *   where the last replicated action of the journal is located in Kafka
 * @param seqNr
 *   the [[SeqNr]] of the last replicated event
 * @param deleteTo
 *   the last replicated deleted [[SeqNr]], if any
 * @param cutoff
 *   the latest cutoff: the [[SeqNr]] of the latest replicated epoch record, if any, see
 *   [[EpochMark]]
 * @param purgedTo
 *   the floor of the current incarnation: every [[SeqNr]] at or below it was purged, or was never
 *   written because the incarnation started above it. A replay is not refused for these
 */
final case class JournalPointer(
  partitionOffset: PartitionOffset,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo] = None,
  cutoff: Option[SeqNr] = None,
  purgedTo: Option[SeqNr] = None,
) {

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}
