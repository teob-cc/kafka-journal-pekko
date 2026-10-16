package com.evolution.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*

/**
 * A tiering delete: the truncation of a journal's replicated events below its latest cutoff, once
 * an archive holds them. It is an ordinary delete whose origin is [[TieringDelete.origin]]: the
 * writer issues it through a [[Journals]] made with that origin, and the delete action carries it
 * to the replicator.
 *
 * A tiering delete must stay below the latest cutoff: the epoch record at the cutoff ([[EpochMark]])
 * is where every replay starts, and a delete at or past it leaves a journal that cannot be replayed
 * ([[ShortReplayError]]). The replicator refuses one at or past the latest cutoff, or one of a
 * journal with no cutoff: the replicated journal is left as it was, and the refusal is logged as an
 * error and counted (`TopicReplicatorMetrics.tieringDeleteRefused`). Every replicator built from
 * this library does so.
 *
 * The replicator checks each delete on its own, before it merges the deletes of a journal that
 * arrive in one poll into the largest one: a refused delete never takes a smaller allowed one with
 * it. The cutoff a delete is checked against is the latest one when the delete is applied: the
 * replicated head's, raised by the epoch marks of the appends before it in the poll, and cleared by
 * a purge before it.
 *
 * A delete of any other origin is applied as before, past the cutoff too. A tiering delete merged
 * with a larger delete of another origin is that delete, and is applied as it is.
 */
object TieringDelete {

  /**
   * The origin of a tiering delete. A host name cannot hold a `/`, so no writer's own origin is
   * taken for it. The value is part of the wire format: deletes in Kafka carry it.
   */
  val origin: Origin = Origin("teob/tiering")

  /**
   * Whether a tiering delete up to `deleteTo` is refused: it is at or past the latest cutoff
   * `cutoff`, or there is none.
   */
  def refused(deleteTo: SeqNr, cutoff: Option[SeqNr]): Boolean = cutoff.forall { cutoff => deleteTo >= cutoff }

  /**
   * A tiering delete the replicator refused: the journal, the seqNr it would have deleted up to, and
   * the journal's latest cutoff when the delete was to be applied, `None` when it had none.
   */
  final case class Refused(key: Key, deleteTo: SeqNr, cutoff: Option[SeqNr]) {

    def message: String = {
      val cutoffStr = cutoff.fold("the journal has no cutoff") { cutoff => s"the latest cutoff is $cutoff" }
      s"$key: a tiering delete up to $deleteTo refused: $cutoffStr; the replicated journal is left as it was"
    }
  }

  /**
   * The records of one journal's actions, in the order of the log, with each tiering delete the
   * cutoff refuses taken out, and the refusals in the same order.
   *
   * @param cutoff
   *   the latest cutoff of the replicated head before the first record
   * @param actionOf
   *   the action of a record
   */
  def guard[A](key: Key, records: Nel[A], cutoff: Option[SeqNr])(actionOf: A => Action): (List[A], List[Refused]) = {
    val (_, kept, refusals) = records.foldLeft((cutoff, List.empty[A], List.empty[Refused])) {
      case ((cutoff, kept, refusals), record) =>
        actionOf(record) match {
          case append: Action.Append =>
            val cutoff1 = cutoffOf(append).fold(cutoff) { mark => cutoff.fold(mark) { _ max mark }.some }
            (cutoff1, record :: kept, refusals)

          case _: Action.Purge =>
            (none[SeqNr], record :: kept, refusals)

          case delete: Action.Delete if delete.origin.contains(origin) && refused(delete.to.value, cutoff) =>
            (cutoff, kept, Refused(key, delete.to.value, cutoff) :: refusals)

          case _ =>
            (cutoff, record :: kept, refusals)
        }
    }
    (kept.reverse, refusals.reverse)
  }

  /**
   * Whether `action` is a tiering delete, which [[guard]] checks.
   */
  def is(action: Action): Boolean = action match {
    case delete: Action.Delete => delete.origin.contains(origin)
    case _ => false
  }

  /**
   * The cutoff an append raises the head to: the seqNr its epoch mark names, if the mark names one
   * of its events. A mark that does not is ignored, as the replicator ignores it.
   */
  private def cutoffOf(append: Action.Append): Option[SeqNr] = {
    EpochMark(append.headers).toOption.flatten.filter { seqNr => append.range.contains(seqNr) }
  }
}
