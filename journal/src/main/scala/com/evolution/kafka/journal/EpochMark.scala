package com.evolution.kafka.journal

import cats.syntax.all.*

import scala.util.Try

/**
 * The mark on an append action that says it holds an epoch record.
 *
 * An epoch record is an ordinary event that carries the fold of every event before it, so its
 * [[SeqNr]] is a safe cutoff: a replay may start there instead of at the first event. The journal
 * never parses payloads; the writer marks the append with a Kafka record header instead.
 *
 * The replicator keeps the latest marked [[SeqNr]] in the replicated head of the journal, and a
 * replay starts at it while it is above `deleteTo`. A mark still unreplicated in the Kafka tail is
 * not known to the head yet: a replay then starts at the previous cutoff and passes through the
 * newer one.
 *
 * Example:
 * {{{
 * journal.append(events, metadata, headers ++ EpochMark.headers(epochRecord.seqNr))
 * }}}
 */
object EpochMark {

  /**
   * The Kafka record header that marks an append. Its value is the [[SeqNr]] of the epoch record in
   * the append, in decimal.
   */
  val header: String = "journal.epoch"

  /**
   * The headers that mark an append whose event at `seqNr` is an epoch record.
   */
  def headers(seqNr: SeqNr): Headers = Headers((header, seqNr.value.toString))

  /**
   * The [[SeqNr]] the headers mark as an epoch record, if they carry the mark.
   *
   * @return
   *   `Left` with the value of the header if it is not a [[SeqNr]]
   */
  def apply(headers: Headers): Either[String, Option[SeqNr]] = {
    headers
      .get(header)
      .traverse { value =>
        Try { value.trim.toLong }
          .toOption
          .flatMap { SeqNr.opt }
          .toRight(value)
      }
  }

  /**
   * The [[SeqNr]] of an epoch record among the appended events, if the headers mark one.
   *
   * @return
   *   [[InvalidEpochMarkError]] if the mark is not a [[SeqNr]] of one of the events
   */
  def of[A](key: Key, events: List[Event[A]], headers: Headers): Either[InvalidEpochMarkError, Option[SeqNr]] = {
    def error(value: String) = {
      InvalidEpochMarkError(key, value, events.map { _.seqNr }).asLeft[Option[SeqNr]]
    }
    apply(headers) match {
      case Left(value) => error(value)
      case Right(None) => none[SeqNr].asRight[InvalidEpochMarkError]
      case Right(Some(seqNr)) =>
        if (events.exists { _.seqNr == seqNr }) seqNr.some.asRight[InvalidEpochMarkError]
        else error(seqNr.value.toString)
    }
  }
}

/**
 * An append was marked as holding an epoch record, but the mark names no event of the append.
 *
 * @param key
 *   the journal
 * @param mark
 *   the value of the [[EpochMark.header]]
 * @param seqNrs
 *   the [[SeqNr]]s of the appended events
 */
final case class InvalidEpochMarkError(key: Key, mark: String, seqNrs: List[SeqNr])
extends RuntimeException(
  s"$key: the epoch mark $mark names no event of the append, seqNrs: ${ seqNrs.mkString(",") }",
)
with scala.util.control.NoStackTrace
