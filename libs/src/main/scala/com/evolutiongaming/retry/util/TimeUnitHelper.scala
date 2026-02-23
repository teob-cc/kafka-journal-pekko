package com.evolutiongaming.retry.util

import cats.implicits._

import scala.concurrent.duration._
import scala.unchecked

private[retry] object TimeUnitHelper {

  object TimeUnitObj {
    def values: List[TimeUnit] = java.util.concurrent.TimeUnit.values().toList
  }
  

  implicit class TimeUnitOpsRetryHelper(val self: TimeUnit) extends AnyVal {

    def increment: Option[TimeUnit] = (self: @unchecked) match {
      case DAYS         => none
      case HOURS        => DAYS.some
      case MINUTES      => HOURS.some
      case SECONDS      => MINUTES.some
      case MILLISECONDS => SECONDS.some
      case MICROSECONDS => MILLISECONDS.some
      case NANOSECONDS  => MICROSECONDS.some
    }

    def decrement: Option[TimeUnit] = (self: @unchecked) match {
      case DAYS         => HOURS.some
      case HOURS        => MINUTES.some
      case MINUTES      => SECONDS.some
      case SECONDS      => MILLISECONDS.some
      case MILLISECONDS => MICROSECONDS.some
      case MICROSECONDS => NANOSECONDS.some
      case NANOSECONDS  => none
    }
  }
}
