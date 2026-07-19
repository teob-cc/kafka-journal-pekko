package com.evolution.scassandra4

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.config.DriverOption
import com.datastax.oss.driver.api.core.context.DriverContext
import com.datastax.oss.driver.api.core.retry.{RetryDecision, RetryPolicy}
import com.datastax.oss.driver.api.core.servererrors.{CoordinatorException, WriteType}
import com.datastax.oss.driver.api.core.session.Request
import org.slf4j.LoggerFactory

import scala.annotation.nowarn

/**
 * Port of `com.evolutiongaming.scassandra.NextHostRetryPolicy` (driver 3) to driver 4's
 * config-class retry SPI: instantiated reflectively by the driver via the `(DriverContext, String)`
 * constructor, configured through `advanced.retry-policy.class` plus the custom
 * [[NextHostRetryPolicy.Retries]] option — see [[CreateDriverConfigLoader]], which sets both when
 * `CassandraConfig.retries` is defined.
 *
 * Semantics of the driver 3 original, expressed in driver 4 verdicts:
 *   - unavailable: try the next node on the first attempt, then retry the same node
 *   - read/write timeout: retry the same node
 *   - request aborted or coordinator error: try the next node
 *
 * all bounded by `retries` (i.e. up to `retries + 1` executions). Logging replaces the driver 3
 * `LoggingRetryPolicy` wrapper. Idempotence is enforced by the driver core, which consults the
 * policy for aborted requests and error responses only when a statement is idempotent.
 */
@nowarn("cat=deprecation") // driver 4 keeps the pre-verdict RetryPolicy methods abstract but deprecated
final class NextHostRetryPolicy(context: DriverContext, profileName: String) extends RetryPolicy {

  import NextHostRetryPolicy.*

  private val log = LoggerFactory.getLogger(getClass)

  private val retries = {
    val profile = context.getConfig.getProfile(profileName)
    if (profile.isDefined(Retries)) profile.getInt(Retries) else RetriesDefault
  }

  def onReadTimeout(
    request: Request,
    cl: ConsistencyLevel,
    blockFor: Int,
    received: Int,
    dataPresent: Boolean,
    retryCount: Int,
  ): RetryDecision = {
    retrySame(s"read timeout, blockFor: $blockFor, received: $received", retryCount)
  }

  def onWriteTimeout(
    request: Request,
    cl: ConsistencyLevel,
    writeType: WriteType,
    blockFor: Int,
    received: Int,
    retryCount: Int,
  ): RetryDecision = {
    retrySame(s"write timeout, writeType: $writeType, blockFor: $blockFor, received: $received", retryCount)
  }

  def onUnavailable(
    request: Request,
    cl: ConsistencyLevel,
    required: Int,
    alive: Int,
    retryCount: Int,
  ): RetryDecision = {
    val reason = s"unavailable, required: $required, alive: $alive"
    if (retryCount == 0) tryNextNode(reason, retryCount)
    else retrySame(reason, retryCount)
  }

  def onRequestAborted(
    request: Request,
    error: Throwable,
    retryCount: Int,
  ): RetryDecision = {
    tryNextNode(s"request aborted: $error", retryCount)
  }

  def onErrorResponse(
    request: Request,
    error: CoordinatorException,
    retryCount: Int,
  ): RetryDecision = {
    tryNextNode(s"error response: $error", retryCount)
  }

  def close(): Unit = {}

  private def retrySame(reason: String, retryCount: Int): RetryDecision = {
    decide(reason, retryCount, RetryDecision.RETRY_SAME)
  }

  private def tryNextNode(reason: String, retryCount: Int): RetryDecision = {
    decide(reason, retryCount, RetryDecision.RETRY_NEXT)
  }

  private def decide(reason: String, retryCount: Int, decision: RetryDecision): RetryDecision = {
    if (retryCount < retries) {
      if (log.isInfoEnabled) log.info(s"$decision, retry: $retryCount of $retries, $reason")
      decision
    } else {
      if (log.isInfoEnabled) log.info(s"${ RetryDecision.RETHROW }, retries exhausted: $retries, $reason")
      RetryDecision.RETHROW
    }
  }
}

object NextHostRetryPolicy {

  /**
   * Number of retries after the initial attempt, read from the execution profile.
   */
  val Retries: DriverOption = new DriverOption {
    def getPath: String = "advanced.retry-policy.retries"
  }

  val RetriesDefault: Int = 1
}
