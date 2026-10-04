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
 *   the last [[SeqNr]] of the purged incarnations of the journal, if it was ever purged: the
 *   current incarnation starts above it
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
