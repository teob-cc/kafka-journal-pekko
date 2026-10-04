package com.evolution.kafka.journal

import scala.util.control.NoStackTrace

/**
 * A replay was asked to start at or below `deleteTo`, and no epoch record is retained above
 * `deleteTo` to start from instead.
 *
 * The events the replay asks for are gone, so serving the rest of the journal would recover a wrong
 * state. The replay fails with this error instead.
 *
 * @param key
 *   the journal
 * @param from
 *   the [[SeqNr]] the replay was asked to start at
 * @param deleteTo
 *   the last deleted [[SeqNr]] of the current incarnation of the journal
 */
final case class ShortReplayError(key: Key, from: SeqNr, deleteTo: DeleteTo)
extends RuntimeException(
  s"$key: a replay from $from would start at or below deleteTo $deleteTo, and no epoch record is retained above it",
)
with NoStackTrace
