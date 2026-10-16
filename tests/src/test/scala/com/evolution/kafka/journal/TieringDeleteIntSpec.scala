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
 * The replicator refuses a tiering delete at or past the latest cutoff of a journal, or of a
 * journal with no cutoff, and checks each tiering delete before it merges the deletes of a poll
 * ([[TieringDelete]]). The deletes are written through a [[Journals]] made with
 * [[TieringDelete.origin]]; each is followed by an append, so that once the append is replicated
 * the delete was applied or refused.
 */
class TieringDeleteIntSpec extends AsyncWordSpec with JournalSuite {
  import TieringDeleteIntSpec.*
  import cats.effect.unsafe.implicits.global

  private implicit val kafkaRead: KafkaRead[IO, Payload] = KafkaRead.payloadKafkaRead[IO]
  private implicit val kafkaWrite: KafkaWrite[IO, Payload] = KafkaWrite.payloadKafkaWrite[IO]
  private implicit val eventualRead: EventualRead[IO, Payload] = EventualRead.payloadEventualRead[IO]

  private def journalsOf(origin: Origin): Resource[IO, Journals[IO]] = {
    implicit val logOf: LogOf[IO] = LogOf.empty[IO]
    for {
      config <- config.liftTo[IO].toResource
      consumer = Journals.Consumer.make[IO](config.journal.kafka.consumer, config.journal.pollTimeout)
    } yield {
      Journals[IO](
        producer = producer,
        origin = origin.some,
        consumer = consumer,
        eventualJournal = eventualJournal,
        headCache = HeadCache.empty[IO],
        log = Log.empty[IO],
        conversionMetrics = none,
      )
    }
  }

  private lazy val ((writer, sweep), releaseJournals) = {
    (journalsOf(Origin("TieringDeleteIntSpec")), journalsOf(TieringDelete.origin)).tupled.allocated.unsafeRunSync()
  }

  private val retry = Retry[IO, Throwable](Strategy.const(100.millis).limit(1.minute))

  private def event(seqNr: Int) = Event(SeqNr.unsafe(seqNr), payload = Payload.json(Json.obj("seqNr" -> seqNr)).some)

  private def events(seqNrs: Int*) = Nel.fromListUnsafe(seqNrs.toList.map(event))

  private def epoch(seqNr: Int): Headers = EpochMark.headers(SeqNr.unsafe(seqNr))

  private def replay(key: Key) = writer(key).read[Payload](SeqNr.min).map(_.seqNr.value).toList

  private def tieringDelete(key: Key, to: Int) = sweep(key).delete(DeleteTo(SeqNr.unsafe(to))).void

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

  private def deleteTo(key: Key) = eventualJournal.pointer(key).map { _.flatMap { _.deleteTo }.map { _.value.value } }

  /**
   * Events 1 to 3, then 4 and 5 with the epoch record at 4, then 6, replicated: the latest cutoff is 4.
   */
  private def cut(key: Key) = {
    for {
      _ <- writer(key).append(events(1, 2, 3))
      _ <- writer(key).append(events(4, 5), headers = epoch(4))
      _ <- writer(key).append(events(6))
      _ <- replicated(key)(1, 2, 3, 4, 5, 6)
      pointer <- eventualJournal.pointer(key)
      _ = pointer.flatMap { _.cutoff } shouldEqual SeqNr.unsafe(4).some
    } yield {}
  }

  "TieringDelete" should {

    "be refused by the replicator at or past the latest cutoff: the replicated journal is left as it was" in {
      val result = for {
        key <- Key.random[IO]("journal")
        _ <- cut(key)
        _ <- tieringDelete(key, 4) // the cutoff itself
        _ <- tieringDelete(key, 6)
        _ <- writer(key).append(events(7))
        _ <- replicated(key)(1, 2, 3, 4, 5, 6, 7)
        deleted <- deleteTo(key)
        _ = deleted shouldEqual none
        seqNrs <- replay(key)
        _ = seqNrs shouldEqual List(4, 5, 6, 7)
      } yield {}
      result.run(2.minutes)
    }

    "be applied below the latest cutoff" in {
      val result = for {
        key <- Key.random[IO]("journal")
        _ <- cut(key)
        _ <- tieringDelete(key, 3)
        _ <- replicated(key)(4, 5, 6)
        deleted <- deleteTo(key)
        _ = deleted shouldEqual 3L.some
        seqNrs <- replay(key)
        _ = seqNrs shouldEqual List(4, 5, 6)
      } yield {}
      result.run(2.minutes)
    }

    "be checked one by one: a refused larger delete does not take an allowed smaller one with it" in {
      val result = for {
        key <- Key.random[IO]("journal")
        _ <- cut(key)
        _ <- tieringDelete(key, 3)
        _ <- tieringDelete(key, 5)
        _ <- writer(key).append(events(7))
        _ <- replicated(key)(4, 5, 6, 7)
        deleted <- deleteTo(key)
        _ = deleted shouldEqual 3L.some
        seqNrs <- replay(key)
        _ = seqNrs shouldEqual List(4, 5, 6, 7)
      } yield {}
      result.run(2.minutes)
    }

    "be refused for a journal with no cutoff, while a delete of any other origin is applied as before" in {
      val result = for {
        key <- Key.random[IO]("journal")
        _ <- writer(key).append(events(1, 2, 3))
        _ <- tieringDelete(key, 2)
        _ <- writer(key).append(events(4))
        _ <- replicated(key)(1, 2, 3, 4)
        deleted <- deleteTo(key)
        _ = deleted shouldEqual none
        _ <- writer(key).delete(DeleteTo(SeqNr.unsafe(2)))
        _ <- replicated(key)(3, 4)
        deleted <- deleteTo(key)
        _ = deleted shouldEqual 2L.some
      } yield {}
      result.run(2.minutes)
    }

    "release" in {
      releaseJournals.run(1.minute)
    }
  }
}

object TieringDeleteIntSpec {

  private case object NotYet extends RuntimeException with NoStackTrace
}
