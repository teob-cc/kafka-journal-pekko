package com.evolution.kafka.journal

import TestJsonCodec.instance
import cats.data.NonEmptyList as Nel
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead
import com.evolution.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.retry.{Retry, Strategy}
import org.scalatest.wordspec.AsyncWordSpec
import play.api.libs.json.Json

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

/**
 * A journal whose appends mark epoch records: the replicated head carries the latest cutoff, and a
 * replay starts there.
 */
class JournalCutoffIntSpec extends AsyncWordSpec with JournalSuite {
  import JournalCutoffIntSpec.*
  import cats.effect.unsafe.implicits.global

  private implicit val kafkaRead: KafkaRead[IO, Payload] = KafkaRead.payloadKafkaRead[IO]
  private implicit val kafkaWrite: KafkaWrite[IO, Payload] = KafkaWrite.payloadKafkaWrite[IO]
  private implicit val eventualRead: EventualRead[IO, Payload] = EventualRead.payloadEventualRead[IO]

  private val journalsOf = { (headCache: Boolean) =>
    implicit val logOf: LogOf[IO] = LogOf.empty[IO]

    def headCacheOf(config: pekko.persistence.KafkaJournalConfig) = {
      if (headCache) {
        val headCacheOf = HeadCacheOf[IO](HeadCacheMetrics.empty[IO].some)
        headCacheOf(config.journal.kafka.consumer, eventualJournal)
      } else {
        Resource.pure[IO, HeadCache[IO]](HeadCache.empty[IO])
      }
    }

    for {
      config <- config.liftTo[IO].toResource
      consumer = Journals.Consumer.make[IO](config.journal.kafka.consumer, config.journal.pollTimeout)
      headCache <- headCacheOf(config)
    } yield {
      Journals[IO](
        producer = producer,
        origin = Origin("JournalCutoffIntSpec").some,
        consumer = consumer,
        eventualJournal = eventualJournal,
        headCache = headCache,
        log = Log.empty[IO],
        conversionMetrics = none,
      )
    }
  }

  private val retry = Retry[IO, Throwable](Strategy.const(100.millis).limit(1.minute))

  private def event(seqNr: Int) = Event(SeqNr.unsafe(seqNr), payload = Payload.json(Json.obj("seqNr" -> seqNr)).some)

  private def events(seqNrs: Int*) = Nel.fromListUnsafe(seqNrs.toList.map(event))

  private def epoch(seqNr: Int): Headers = EpochMark.headers(SeqNr.unsafe(seqNr))

  private def replay(journal: Journal[IO], from: SeqNr = SeqNr.min) = {
    journal.read[Payload](from).map(_.seqNr.value).toList
  }

  /**
   * The replicated journal reads, from seq 1, exactly `expected`.
   */
  private def replicated(key: Key)(expected: Long*) = {
    retry {
      eventualJournal
        .read(key, SeqNr.min)
        .map(_.seqNr.value)
        .toList
        .flatMap { seqNrs =>
          if (seqNrs == expected.toList) IO.unit else NotYet.raiseError[IO, Unit]
        }
    }
  }

  "Journal" should {

    for {
      headCache <- List(true, false)
    } {

      lazy val (journals, release) = journalsOf(headCache).allocated.unsafeRunSync()

      val name = s"headCache: $headCache"

      s"replay from seq 1 as before when no append is marked, $name" in {
        val result = for {
          key <- Key.random[IO]("journal")
          journal = journals(key)
          _ <- journal.append(events(1, 2, 3))
          _ <- journal.append(events(4, 5, 6))
          _ <- replicated(key)(1, 2, 3, 4, 5, 6)
          pointer <- eventualJournal.pointer(key)
          _ = pointer.flatMap { _.cutoff } shouldEqual none
          seqNrs <- replay(journal)
          _ = seqNrs shouldEqual List(1, 2, 3, 4, 5, 6)
          seqNrs <- replay(journal, SeqNr.unsafe(5))
          _ = seqNrs shouldEqual List(5, 6)
        } yield {}
        result.run(2.minutes)
      }

      s"start a replay at the one cutoff, $name" in {
        val result = for {
          key <- Key.random[IO]("journal")
          journal = journals(key)
          _ <- journal.append(events(1, 2, 3))
          _ <- journal.append(events(4, 5), headers = epoch(4))
          _ <- journal.append(events(6))
          _ <- replicated(key)(1, 2, 3, 4, 5, 6)
          pointer <- eventualJournal.pointer(key)
          _ = pointer.flatMap { _.cutoff } shouldEqual SeqNr.unsafe(4).some
          seqNrs <- replay(journal)
          _ = seqNrs shouldEqual List(4, 5, 6)
          // a replay asked to start above the cutoff starts where it asked
          seqNrs <- replay(journal, SeqNr.unsafe(5))
          _ = seqNrs shouldEqual List(5, 6)
          // the events below the cutoff may go: the replay is not refused
          _ <- journal.delete(DeleteTo(SeqNr.unsafe(3)))
          _ <- replicated(key)(4, 5, 6)
          seqNrs <- replay(journal)
          _ = seqNrs shouldEqual List(4, 5, 6)
          pointer <- journal.pointer
          _ = pointer shouldEqual SeqNr.unsafe(6).some
        } yield {}
        result.run(2.minutes)
      }

      s"start a replay at the latest of several cutoffs, $name" in {
        val result = for {
          key <- Key.random[IO]("journal")
          journal = journals(key)
          _ <- journal.append(events(1, 2))
          _ <- journal.append(events(3), headers = epoch(3))
          _ <- journal.append(events(4, 5))
          _ <- journal.append(events(6, 7), headers = epoch(6))
          _ <- journal.append(events(8), headers = epoch(8))
          _ <- journal.append(events(9))
          _ <- replicated(key)(1, 2, 3, 4, 5, 6, 7, 8, 9)
          pointer <- eventualJournal.pointer(key)
          _ = pointer.flatMap { _.cutoff } shouldEqual SeqNr.unsafe(8).some
          seqNrs <- replay(journal)
          _ = seqNrs shouldEqual List(8, 9)
          _ <- journal.delete(DeleteTo(SeqNr.unsafe(5)))
          _ <- replicated(key)(6, 7, 8, 9)
          seqNrs <- replay(journal)
          _ = seqNrs shouldEqual List(8, 9)
          seqNrs <- replay(journal, SeqNr.unsafe(9))
          _ = seqNrs shouldEqual List(9)
        } yield {}
        result.run(2.minutes)
      }

      s"refuse an append whose epoch mark names no event of it, $name" in {
        val result = for {
          key <- Key.random[IO]("journal")
          journal = journals(key)
          result <- journal.append(events(1, 2), headers = epoch(3)).attempt
          _ = result.left.map(_.getClass) shouldEqual Left(classOf[InvalidEpochMarkError])
          pointer <- journal.pointer
          _ = pointer shouldEqual none
        } yield {}
        result.run(2.minutes)
      }

      s"release $name" in {
        release.run(1.minute)
      }
    }
  }
}

object JournalCutoffIntSpec {

  private case object NotYet extends RuntimeException with NoStackTrace
}
