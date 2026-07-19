package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.skafka.Offset

import java.time.Instant

final case class OffsetAndTimestamp(offset: Offset, timestamp: Instant)
