package com.evolution.kafka.journal

import cats.Monad
import cats.data.NonEmptyList as Nel
import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO, Sync}
import cats.syntax.all.*
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.*
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log, MeasureDuration, RandomIdOf}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, Succeeded}

import java.time.Instant
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class JournalSpec extends AnyWordSpec with Matchers {
  import JournalSpec.*

  // TODO add test when Kafka missing it's tail comparing to eventual
  def testF[F[_]: Monad](withJournal: (SeqNrJournal[F] => F[Assertion]) => Assertion): Unit = {
    for {
      size <- 0 to 5
      seqNrs = (1 to size).toList.map { a => SeqNr.unsafe(a) }
      combination <- Combinations(seqNrs)
    } {

      val seqNrLast = seqNrs.lastOption

      def createAndAppend(f: (SeqNrJournal[F], Option[Offset]) => F[Assertion]) = {
        withJournal { journal =>
          def append(seqNrs: Nel[SeqNr]) = {
            journal
              .append(seqNrs.head, seqNrs.tail*)
              .map { _.some }
          }
          for {
            offset <- combination.foldLeftM(none[Offset]) { (_, seqNrs) => append(seqNrs) }
            offsetNext = offset.map { _.inc[Try].get }
            result <- f(journal, offsetNext)
          } yield result
        }
      }

      val name = combination
        .map { _.toList.mkString("[", ",", "]") }
        .mkString(",")

      s"append, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.read(SeqRange.all)
            } yield {
              a shouldEqual seqNrs
            }
        }
      }

      s"read, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual seqNrs
              last = seqNrLast getOrElse SeqNr.min
              a <- journal.read(SeqNr.min.to(last))
              _ = a shouldEqual seqNrs
              a <- journal.read(SeqNr.min.to(last.next[Option].getOrElse(last)))
            } yield {
              a shouldEqual seqNrs
            }
        }
      }

      s"delete all, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- seqNrLast.fold(().pure[F]) { seqNr => journal.delete(seqNr.toDeleteTo).void }
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual Nil
              a <- journal.pointer
            } yield {
              a shouldEqual seqNrLast
            }
        }
      }

      s"delete SeqNr.Max, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- journal.delete(DeleteTo.max)
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual Nil
              a <- journal.pointer
            } yield {
              a shouldEqual seqNrLast
            }
        }
      }

      s"delete SeqNr.Min, $name" in {
        createAndAppend {
          case (journal, offset) =>
            for {
              a <- journal.delete(DeleteTo.min)
              _ = a shouldEqual offset.map { _.inc[Try].get }
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual seqNrs.dropWhile(_ <= SeqNr.min)
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      s"purge, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- journal.purge
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual List.empty
              // the purged id keeps its head seqNr: it is never reborn at seq 1
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      s"mark, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              _ <- journal.mark("test-mark-id")
              a <- journal.read(SeqRange.all)
              _ = a shouldEqual seqNrs
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      s"lastSeqNr, $name" in {
        createAndAppend {
          case (journal, _) =>
            for {
              a <- journal.pointer
              _ = a shouldEqual seqNrLast
            } yield Succeeded
        }
      }

      for {
        _ <- seqNrLast
        seqNr <- seqNrs.tail.lastOption
      } {

        s"delete except last, $name" in {
          createAndAppend {
            case (journal, _) =>
              for {
                _ <- journal.delete(seqNr.toDeleteTo)
                seqNrs <- journal.read(SeqRange.all)
                _ = seqNrs shouldEqual seqNrs.dropWhile(_ <= seqNr)
                seqNr <- journal.pointer
              } yield {
                seqNr shouldEqual seqNrLast
              }
          }
        }

        s"read tail, $name" in {
          createAndAppend {
            case (journal, _) =>
              for {
                seqNrs <- journal.read(seqNr.to(SeqNr.max))
              } yield {
                seqNrs shouldEqual seqNrs.dropWhile(_ < seqNr)
              }
          }
        }
      }
    }

    s"read SeqNr.Max" in {
      withJournal { journal =>
        for {
          seqNrs <- journal.read(SeqRange(SeqNr.max))
          _ = seqNrs shouldEqual Nil
          _ <- journal.append(SeqNr.unsafe(1))
          seqNrs <- journal.read(SeqRange(SeqNr.max))
        } yield {
          seqNrs shouldEqual Nil
        }
      }
    }

    s"append, delete, append, delete, append, read, lastSeqNr, purge" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1))
          _ <- journal.delete(SeqNr.unsafe(3).toDeleteTo)
          _ <- journal.append(SeqNr.unsafe(2), SeqNr.unsafe(3))
          _ <- journal.delete(SeqNr.unsafe(2).toDeleteTo)
          _ <- journal.append(SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(1, 2))
          _ = seqNrs shouldEqual Nil
          seqNrs <- journal.read(SeqRange.unsafe(2, 3))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(3))
          seqNrs <- journal.read(SeqRange.unsafe(3, 4))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(3), SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(4, 5))
          _ = seqNrs shouldEqual List(SeqNr.unsafe(4))
          seqNrs <- journal.read(SeqRange.unsafe(5, 6))
          _ = seqNrs shouldEqual Nil
          seqNr <- journal.pointer
          _ = seqNr shouldEqual SeqNr.unsafe(4).some
          _ <- journal.purge
          seqNrs <- journal.read(SeqRange.all)
          _ = seqNrs shouldEqual Nil
          pointer <- journal.pointer
          _ = pointer shouldEqual SeqNr.unsafe(4).some
        } yield Succeeded
      }
    }

    s"read record completely" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1), SeqNr.unsafe(2))
          a <- journal.read(SeqRange.unsafe(1, 2))
        } yield {
          a shouldEqual List(SeqNr.unsafe(1), SeqNr.unsafe(2))
        }
      }
    }

    s"read record partially" in {
      withJournal { journal =>
        for {
          _ <- journal.append(SeqNr.unsafe(1), SeqNr.unsafe(2))
          a <- journal.read(SeqRange.unsafe(2))
        } yield {
          a shouldEqual List(SeqNr.unsafe(2))
        }
      }
    }
  }

  "Journal.mark" should {
    "produce an Action.Mark with the supplied id" in {
      implicit val clock: Clock[StateT] = Clock.const[StateT](nanos = 0, millis = timestamp.toEpochMilli)
      implicit val randomIdOf: RandomIdOf[StateT] = RandomIdOf.uuid[StateT]
      implicit val measureDuration: MeasureDuration[StateT] = MeasureDuration.fromClock(clock)
      val log = Log.empty[StateT]

      val journals = Journals.of[StateT](
        eventual = StateT.eventualJournal,
        consumeActionRecords = StateT.consumeActionRecords,
        produce = Produce(StateT.produceAction, none),
        headCache = StateT.headCache,
        log = log,
        conversionMetrics = none,
        refuseShortReplay = true,
      )

      val (state, partitionOffset) = journals(JournalSpec.key)
        .mark("custom-purge-id")
        .run(State.empty)
        .unsafeRunSync()

      val ids = state
        .records
        .toList
        .collect { case ActionRecord(a: Action.Mark, _) => a.id }

      ids shouldEqual List("custom-purge-id")
      partitionOffset.partition shouldEqual partition
    }
  }

  "Journal with epoch marks" should {

    implicit val clock: Clock[StateT] = Clock.const[StateT](nanos = 0, millis = timestamp.toEpochMilli)
    implicit val randomIdOf: RandomIdOf[StateT] = RandomIdOf.uuid[StateT]
    implicit val measureDuration: MeasureDuration[StateT] = MeasureDuration.fromClock(clock)
    implicit val fromTry: FromTry[StateT] = FromTry.lift[StateT]
    implicit val fail: Fail[StateT] = Fail.lift[StateT]
    implicit val fromAttempt: FromAttempt[StateT] = FromAttempt.lift[StateT]
    implicit val fromJsResult: FromJsResult[StateT] = FromJsResult.lift[StateT]

    // replicates every action but the last `behind`
    def produceActionOf(behind: Int): ProduceAction[StateT] = { (action: Action) =>
      StateT { state =>
        val offset = Offset.unsafe(state.records.size)
        val partitionOffset = PartitionOffset(partition = partition, offset = offset)
        val record = ActionRecord(action, partitionOffset)
        val records = state.records.enqueue(record)
        val replicatedState = records
          .dropRight(behind)
          .foldLeft(EventualJournalOf.State.empty) { (state, record) => state(record) }
        (state.copy(records = records, replicatedState = replicatedState), partitionOffset)
      }
    }

    // the head of the actions not replicated yet, as the real head cache sees it
    val headCache: HeadCache[StateT] = { (_: Key, _: Partition, _: Offset) =>
      StateT { state =>
        val replicated = state.replicatedState.offset
        val headInfo = state
          .records
          .filter { record => replicated.forall { record.offset > _ } }
          .foldLeft(HeadInfo.empty) { (info, record) => info(record.action.header, record.offset) }
        (state, headInfo.some)
      }
    }

    def journalOf(behind: Int): Journal[StateT] = {
      val journals = Journals.of[StateT](
        eventual = StateT.eventualJournal,
        consumeActionRecords = StateT.consumeActionRecords,
        produce = Produce(produceActionOf(behind), none),
        headCache = headCache,
        log = Log.empty[StateT],
        conversionMetrics = none,
        refuseShortReplay = true,
      )
      journals(JournalSpec.key)
    }

    def events(seqNrs: Int*) = Nel.fromListUnsafe(seqNrs.toList.map { seqNr => Event[Payload](SeqNr.unsafe(seqNr)) })

    def epoch(seqNr: Int) = EpochMark.headers(SeqNr.unsafe(seqNr))

    def run[A](stateT: StateT[A]) = stateT.run(State.empty).unsafeRunSync()._2

    def appendAll(journal: Journal[StateT], marks: Boolean) = {
      def mark(seqNr: Int) = if (marks) epoch(seqNr) else Headers.empty
      for {
        _ <- journal.append(events(1, 2))
        _ <- journal.append(events(3), headers = mark(3))
        _ <- journal.append(events(4))
        _ <- journal.append(events(5, 6), headers = mark(5))
        _ <- journal.append(events(7))
      } yield {}
    }

    def seqNrsOf(journal: Journal[StateT], from: SeqNr) = journal.read[Payload](from).map { _.seqNr.value }.toList

    for {
      // a read appends a mark action of its own, which counts among the actions behind
      (behind, expected) <- List(
        (0, List(5, 6, 7)),
        (2, List(5, 6, 7)),
        // the newer cutoff is still in the Kafka tail: the replay starts at the previous one
        (3, List(3, 4, 5, 6, 7)),
        (4, List(3, 4, 5, 6, 7)),
        // no cutoff is replicated yet
        (5, List(1, 2, 3, 4, 5, 6, 7)),
        (6, List(1, 2, 3, 4, 5, 6, 7)),
      )
    } {
      s"start a replay at the latest replicated cutoff, $behind actions behind" in {
        val journal = journalOf(behind)
        val result = for {
          _ <- appendAll(journal, marks = true)
          from1 <- seqNrsOf(journal, SeqNr.min)
          from6 <- seqNrsOf(journal, SeqNr.unsafe(6))
          pointer <- journal.pointer
        } yield (from1, from6, pointer)
        run(result) shouldEqual ((expected, List(6, 7), SeqNr.unsafe(7).some))
      }

      s"replay from seq 1 without epoch marks, $behind actions behind" in {
        val journal = journalOf(behind)
        val result = for {
          _ <- appendAll(journal, marks = false)
          from1 <- seqNrsOf(journal, SeqNr.min)
        } yield from1
        run(result) shouldEqual List(1, 2, 3, 4, 5, 6, 7)
      }
    }

    for {
      behind <- List(0, 1, 2, 3)
    } {

      s"refuse a replay that starts at or below deleteTo with no epoch record above it, $behind actions behind" in {
        val journal = journalOf(behind)
        val result = for {
          _ <- appendAll(journal, marks = false)
          _ <- journal.delete(DeleteTo(SeqNr.unsafe(4)))
          from1 <- seqNrsOf(journal, SeqNr.min).attempt
          from4 <- seqNrsOf(journal, SeqNr.unsafe(4)).attempt
          from5 <- seqNrsOf(journal, SeqNr.unsafe(5))
        } yield (from1.left.map { _.getClass }, from4.left.map { _.getClass }, from5)
        run(result) shouldEqual (
          (Left(classOf[ShortReplayError]), Left(classOf[ShortReplayError]), List(5, 6, 7))
        )
      }

      s"serve a replay below deleteTo from an epoch record above it, $behind actions behind" in {
        val journal = journalOf(behind)
        val result = for {
          _ <- appendAll(journal, marks = true)
          _ <- journal.delete(DeleteTo(SeqNr.unsafe(4)))
          from1 <- seqNrsOf(journal, SeqNr.min)
        } yield from1
        run(result) shouldEqual List(5, 6, 7)
      }

      s"serve a replay of a purged journal, and of its next incarnation, $behind actions behind" in {
        val journal = journalOf(behind)
        val result = for {
          _ <- appendAll(journal, marks = false)
          _ <- journal.purge
          purged <- seqNrsOf(journal, SeqNr.min)
          pointer <- journal.pointer
          _ <- journal.append(events(8, 9))
          reborn <- seqNrsOf(journal, SeqNr.min)
        } yield (purged, pointer, reborn)
        run(result) shouldEqual ((List.empty[Long], SeqNr.unsafe(7).some, List(8, 9)))
      }
    }

    "refuse an append whose epoch mark names no event of it" in {
      val journal = journalOf(0)
      val result = for {
        a <- journal.append(events(1, 2), headers = epoch(3)).attempt
        b <- journal.append(events(1, 2), headers = Headers((EpochMark.header, "x"))).attempt
        pointer <- journal.pointer
      } yield (a.left.map(_.getClass), b.left.map(_.getClass), pointer)
      run(result) shouldEqual (
        (Left(classOf[InvalidEpochMarkError]), Left(classOf[InvalidEpochMarkError]), none[SeqNr])
      )
    }
  }

  "Journal" when {

    for {
      (headCacheStr, headCache) <-
        List(("invalid", HeadCache.const(none[HeadInfo].pure[StateT])), ("valid", StateT.headCache))
      (duplicatesStr, consumeActionRecordsOf, eventualJournalOf) <- List(
        ("off", (a: ConsumeActionRecords[StateT]) => a, (a: EventualJournal[StateT]) => a),
        ("on", (a: ConsumeActionRecords[StateT]) => a.withDuplicates, (a: EventualJournal[StateT]) => a),
      )
    } {

      def test(
        eventual: EventualJournal[StateT],
        consumeActionRecords: ConsumeActionRecords[StateT],
        produceAction: ProduceAction[StateT],
        headCache: HeadCache[StateT],
      ): Unit = {
        val journal =
          SeqNrJournal(
            eventualJournalOf(eventual),
            consumeActionRecordsOf(consumeActionRecords),
            produceAction,
            headCache,
          )

        testF[StateT] { f =>
          f(journal)
            .run(State.empty)
            .map { case (_, result) => result }
            .unsafeRunSync()
        }
      }

      val name = s"headCache: $headCacheStr, duplicates: $duplicatesStr"

      s"eventual journal is empty, $name" should {
        test(EventualJournal.empty[StateT], StateT.consumeActionRecords, StateT.produceAction, headCache)
      }

      s"kafka journal is empty, $name" should {

        val consumeActionRecords: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
          {
            StateT.stream { state =>
              val records = state
                .records
                .dropWhile { _.offset < from }
                .collect { case action @ ActionRecord(_: Action.Mark, _) => action }
              val state1 = state.copy(recordsToRead = records)
              (state1, StateT.actionRecords).pure[IO]
            }
          }
        }

        test(StateT.eventualJournal, consumeActionRecords, StateT.produceAction, headCache)
      }

      s"kafka and eventual journals are consistent, $name" should {
        test(StateT.eventualJournal, StateT.consumeActionRecords, StateT.produceAction, headCache)
      }

      for {
        n <- 1 to 3
      } {
        s"kafka and eventual journals are consistent, however eventual offset is $n behind, $name" should {
          val produceAction = new ProduceAction[StateT] {

            def apply(action: Action): StateT[PartitionOffset] = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState =
                  state.replicatedState(record, Offset.of[Try](offset.value - n) getOrElse Offset.min)
                val state1 = state.copy(records = records, replicatedState = replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }

      for {
        n <- 1 to 4
      } {
        s"eventual journal is $n actions behind the kafka journal, $name" should {

          val produceAction = new ProduceAction[StateT] {

            def apply(action: Action): StateT[PartitionOffset] = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = for {
                  actions <- records.dropLast(n)
                  action <- actions.lastOption
                } yield state.replicatedState(action)
                val state1 =
                  state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }

      for {
        n <- 1 to 3
        nn = n + 1
      } {
        s"eventual journal is $n actions behind and pointer is $nn behind the kafka journal, $name" should {

          val produceAction = new ProduceAction[StateT] {

            def apply(action: Action): StateT[PartitionOffset] = {
              StateT { state =>
                val offset = Offset.unsafe(state.records.size)
                val partitionOffset = PartitionOffset(partition = partition, offset = offset)
                val record = ActionRecord(action, partitionOffset)
                val records = state.records.enqueue(record)

                val replicatedState = for {
                  actions <- records.dropLast(n)
                  action <- actions.lastOption
                } yield {
                  state.replicatedState(action, Offset.of[Try](offset.value - n) getOrElse Offset.min)
                }
                val state1 =
                  state.copy(records = records, replicatedState = replicatedState getOrElse state.replicatedState)
                (state1, partitionOffset)
              }
            }
          }

          test(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, headCache)
        }
      }
    }
  }
}

object JournalSpec {
  val key: Key = Key(topic = "topic", id = "id")
  val timestamp: Instant = Instant.now()
  val partition: Partition = Partition.min

  implicit val ec: ExecutionContext = CurrentThreadExecutionContext

  trait SeqNrJournal[F[_]] {

    def append(seqNr: SeqNr, seqNrs: SeqNr*): F[Offset]

    def read(range: SeqRange): F[List[SeqNr]]

    def pointer: F[Option[SeqNr]]

    def delete(to: DeleteTo): F[Option[Offset]]

    def purge: F[Option[Offset]]

    def mark(id: String): F[Offset]
  }

  object SeqNrJournal {

    def apply[F[_]: Monad, A](
      journals: Journals[F],
    )(implicit
      kafkaRead: KafkaRead[F, A],
      eventualRead: EventualRead[F, A],
      kafkaWrite: KafkaWrite[F, A],
    ): SeqNrJournal[F] = {
      apply(journals(key))
    }

    def apply[F[_]: Monad, A](
      journal: Journal[F],
    )(implicit
      kafkaRead: KafkaRead[F, A],
      eventualRead: EventualRead[F, A],
      kafkaWrite: KafkaWrite[F, A],
    ): SeqNrJournal[F] = {

      new SeqNrJournal[F] {

        def append(seqNr: SeqNr, seqNrs: SeqNr*): F[Offset] = {
          val events = for {
            seqNr <- Nel.of(seqNr, seqNrs*)
          } yield {
            Event[A](seqNr)
          }
          for {
            partitionOffset <- journal.append(events)
          } yield {
            partitionOffset.offset
          }
        }

        def read(range: SeqRange): F[List[SeqNr]] = {
          journal
            .read(range.from)
            .dropWhile { _.seqNr < range.from }
            .takeWhile { _.seqNr <= range.to }
            .map { _.seqNr }
            .toList
        }

        def pointer: F[Option[SeqNr]] = journal.pointer

        def delete(to: DeleteTo): F[Option[Offset]] = {
          for {
            partitionOffset <- journal.delete(to)
          } yield
            for {
              partitionOffset <- partitionOffset
            } yield {
              partitionOffset.offset
            }
        }

        def purge: F[Option[Offset]] = {
          for {
            partitionOffset <- journal.purge
          } yield
            for {
              partitionOffset <- partitionOffset
            } yield {
              partitionOffset.offset
            }
        }

        def mark(id: String): F[Offset] = {
          for {
            partitionOffset <- journal.mark(id)
          } yield {
            partitionOffset.offset
          }
        }
      }
    }

    def apply[F[_]: Sync](
      eventual: EventualJournal[F],
      consumeActionRecords: ConsumeActionRecords[F],
      produceAction: ProduceAction[F],
      headCache: HeadCache[F],
    ): SeqNrJournal[F] = {
      implicit val clock: Clock[F] = Clock.const[F](nanos = 0, millis = timestamp.toEpochMilli)
      implicit val randomIdOf: RandomIdOf[F] = RandomIdOf.uuid[F]
      implicit val measureDuration: MeasureDuration[F] = MeasureDuration.fromClock(clock)
      implicit val fromTry: FromTry[F] = FromTry.lift[F]
      implicit val fail: Fail[F] = Fail.lift[F]
      implicit val fromAttempt: FromAttempt[F] = FromAttempt.lift[F]
      implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
      val log = Log.empty[F]

      // the combinations below read after deletes from seq 1: the replay starts at deleteTo + 1
      val journal = Journals.of[F](
        eventual = eventual,
        consumeActionRecords = consumeActionRecords,
        produce = Produce(produceAction, none),
        headCache = headCache,
        log = log,
        conversionMetrics = none,
        refuseShortReplay = false,
      )
        .withLog(log)
        .withMetrics(JournalMetrics.empty[F])
      SeqNrJournal(journal)
    }
  }

  final case class State(
    records: Queue[ActionRecord[Action]] = Queue.empty,
    replicatedState: EventualJournalOf.State = EventualJournalOf.State.empty,
    recordsToRead: Queue[ActionRecord[Action]] = Queue.empty,
  )

  object State {
    val empty: State = State()
  }

  type StateT[A] = cats.data.StateT[IO, State, A]

  object StateT {

    val eventualJournal: EventualJournal[StateT] = new EventualJournal[StateT] {

      def offset(topic: Topic, partition: Partition): StateT[Option[Offset]] = {
        StateT { state =>
          (state, state.replicatedState.offset)
        }
      }

      def read(key: Key, from: SeqNr): Stream[StateT, EventRecord[EventualPayloadAndType]] = {
        val events = StateT { state =>
          val events = state.replicatedState.events.toList.filter(_.seqNr >= from)
          (state, events)
        }

        for {
          events <- events.toStream
          event <- Stream[StateT].apply(events)
        } yield {
          event
        }
      }

      def pointer(key: Key): StateT[Option[JournalPointer]] = {
        StateT { state =>
          val seqNr = state.replicatedState.events.lastOption.map(_.event.seqNr)
          val pointer = for {
            seqNr <- seqNr max state.replicatedState.deleteTo.map { _.value }
            offset <- state.replicatedState.offset
          } yield {
            val partitionOffset = PartitionOffset(partition, offset)
            val replicated = state.replicatedState
            JournalPointer(partitionOffset, seqNr, replicated.deleteTo, replicated.cutoff, replicated.purgedTo)
          }

          (state, pointer)
        }
      }

      def ids(topic: Topic): Stream[StateT, Tag] = {
        Stream[StateT].single(key.id)
      }
    }

    val actionRecords: Stream[StateT, ActionRecord[Action]] = {
      val result = StateT { state =>
        state.recordsToRead.dequeueOption match {
          case Some((record, records)) => (state.copy(recordsToRead = records), Stream[StateT].single(record))
          case None => (state, Stream[StateT].empty[ActionRecord[Action]])
        }
      }
      Stream
        .repeat(result)
        .flatten
    }

    val consumeActionRecords: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
      {
        StateT.stream { state =>
          val records = state
            .records
            .dropWhile { _.offset < from }
          val state1 = state.copy(recordsToRead = records)
          (state1, actionRecords).pure[IO]
        }
      }
    }

    val produceAction: ProduceAction[StateT] = { (action: Action) =>
      {
        StateT { state =>
          val offset = Offset.unsafe(state.records.size)
          val partitionOffset = PartitionOffset(partition = partition, offset = offset)
          val record = ActionRecord(action, partitionOffset)
          val records = state.records.enqueue(record)

          val replicatedState = state.replicatedState(record)
          val state1 = state.copy(records = records, replicatedState = replicatedState)
          (state1, partitionOffset)
        }
      }
    }

    val headCache: HeadCache[StateT] = { (_: Key, _: Partition, _: Offset) =>
      {

        StateT { state =>
          val headInfo = state
            .records
            .foldLeft(HeadInfo.empty) { (info, record) => info(record.action.header, record.offset) }
          (state, headInfo.some)
        }
      }
    }

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[IO, State, A](s => IO.delay(f(s)))

    def of[A](f: State => IO[(State, A)]): StateT[A] = cats.data.StateT[IO, State, A](s => f(s))

    def stream[A](f: State => IO[(State, Stream[StateT, A])]): Stream[StateT, A] = of(f).toStream.flatten
  }

  // TODO implement via mocking EventualCassandra
  object EventualJournalOf {

    final case class State(
      events: Queue[EventRecord[EventualPayloadAndType]] = Queue.empty,
      deleteTo: Option[DeleteTo] = None,
      offset: Option[Offset] = None,
      cutoff: Option[SeqNr] = None,
      purgedTo: Option[SeqNr] = None,
    ) {

      def apply(record: ActionRecord[Action]): State = {
        apply(record, record.offset)
      }

      def apply(record: ActionRecord[Action], offset: Offset): State = {

        implicit val fromAttempt: FromAttempt[Try] = FromAttempt.lift[Try]
        implicit val fromJsResult: FromJsResult[Try] = FromJsResult.lift[Try]

        val kafkaRead = KafkaRead.summon[Try, Payload]
        val eventualWrite = EventualWrite.summon[Try, Payload]

        def updateOffset = copy(offset = offset.some)

        def onAppend(action: Action.Append) = {
          val payloadAndType = action.toPayloadAndType
          val events1 = kafkaRead(payloadAndType).get
          val batch = for {
            event <- events1.events
          } yield {
            val partitionOffset = PartitionOffset(partition, record.offset)
            EventRecord(action, event.map(eventualWrite(_).get), partitionOffset, events1.metadata)
          }
          val cutoff1 = EpochMark(action.headers)
            .toOption
            .flatten
            .filter { seqNr => events1.events.exists { _.seqNr == seqNr } }
          copy(events = events.enqueueAll(batch.toList), offset = offset.some, cutoff = cutoff max cutoff1)
        }

        def onDelete(action: Action.Delete) = {
          events.lastOption.fold(updateOffset) { last =>
            val lastSeqNr = last.event.seqNr
            if (lastSeqNr <= action.to.value) {
              copy(events = Queue.empty, deleteTo = lastSeqNr.toDeleteTo.some, offset = offset.some)
            } else {
              val left = events.dropWhile { _.event.seqNr <= action.to.value }
              copy(events = left, deleteTo = action.to.some, offset = offset.some)
            }
          }
        }

        // the head stays as a tombstone
        def onPurge = {
          val seqNr = events.lastOption.map { _.event.seqNr } max deleteTo.map { _.value }
          seqNr.fold(updateOffset) { seqNr =>
            copy(
              events = Queue.empty,
              deleteTo = seqNr.toDeleteTo.some,
              offset = offset.some,
              cutoff = None,
              purgedTo = seqNr.some,
            )
          }
        }

        record.action match {
          case a: Action.Append => onAppend(a)
          case _: Action.Mark => updateOffset
          case a: Action.Delete => onDelete(a)
          case _: Action.Purge => onPurge
        }
      }
    }

    object State {
      val empty: State = State()
    }
  }

  implicit class TestFutureOps[T](val self: Future[T]) extends AnyVal {

    def get(): T = self.value.get.get
  }

  implicit class QueueOps[T](val self: Queue[T]) extends AnyVal {

    def dropLast(n: Int): Option[Queue[T]] = {
      if (self.size <= n) none
      else self.dropRight(n).some
    }
  }

  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {
    def withDuplicates(
      implicit
      F: Monad[F],
    ): ConsumeActionRecords[F] = new ConsumeActionRecords[F] {
      def apply(key: Key, partition: Partition, from: Offset): Stream[F, ActionRecord[Action]] = {
        self
          .apply(key, partition, from)
          .withDuplicates
      }
    }
  }

  implicit class StreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {
    def withDuplicates(
      implicit
      F: Monad[F],
    ): Stream[F, A] = {
      self.flatMap { a =>
        List
          .fill(2)(a)
          .toStream1[F]
      }
    }
  }
}
