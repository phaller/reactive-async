package com.phaller.rasync.test

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicInteger

import com.phaller.rasync.cell.Outcome
import com.phaller.rasync.lattice.{ DefaultKey, Lattice, NaturalNumberKey, Updater }
import com.phaller.rasync.pool.HandlerPool
import com.phaller.rasync.test.lattice.IntUpdater
import org.scalatest.FunSuite

/** Verify that callbacks of SequentialCells do not run concurrently. */
class SequentialSuite extends FunSuite with SequentialCompleterFactory {
  implicit val intUpdater: Updater[Int] = new IntUpdater

  test("when: calling sequentially") {
    val n = 1000

    val runningCallbacks = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool: HandlerPool[Int] = new HandlerPool[Int](NaturalNumberKey)
    val completer1 = mkCompleter[Int]

    val cell1 = completer1.cell
    for (i <- 1 to n) { // create n predecessors
      val tmpCompleter = mkCompleter[Int]

      // let cell1 depend on the predecessor tmpCompleter
      cell1.when(tmpCompleter.cell)(it => {
        val x = it.head._2
        assert(runningCallbacks.incrementAndGet() == 1)
        Thread.`yield`()
        try {
          Thread.sleep(random.nextInt(3))
        } catch {
          case _: InterruptedException => /* ignore */
        }
        assert(runningCallbacks.decrementAndGet() == 0)
        Outcome(x.get.value * n, x.get.value == n)
      })

      cell1.onComplete(_ => {
        latch.countDown()
      })

      pool.execute(() => tmpCompleter.putFinal(i))
    }

    assert(latch.await(10, TimeUnit.SECONDS))

    assert(cell1.getResult() == n * n)

    pool.onQuiescenceShutdown()
  }

  test("when: state") {
    // cell1 has deps to 1000 cells. All callbacks
    // share a counter (i.e. state) that must not be
    // incremented concurrently
    val n = 1000
    var count = Set[Int]()

    class PowerSetLattice[T] extends Lattice[Set[T]] {

      def join(left: Set[T], right: Set[T]): Set[T] =
        left ++ right

      val bottom: Set[T] =
        Set[T]()

    }

    implicit val theUpdater: Updater[Set[Int]] = Updater.latticeToUpdater(new PowerSetLattice[Int])

    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    val theKey = new DefaultKey[Set[Int]]
    implicit val pool: HandlerPool[Set[Int]] = new HandlerPool[Set[Int]](theKey)
    val completer1 = mkCompleter[Set[Int]]
    val cell1 = completer1.cell

    cell1.onComplete(_ => {
      latch.countDown()
    })

    for (i <- 1 to n) {
      val completer2 = mkCompleter[Set[Int]]
      cell1.when(completer2.cell)(_ => {
        count = count ++ Set(count.size)
        Thread.`yield`()
        try {
          Thread.sleep(random.nextInt(3))
        } catch {
          case _: InterruptedException => /* ignore */
        }
        Outcome(count, count.size == n)
      })
      pool.execute(() => completer2.putNext(Set(i)))
    }

    latch.await()

    assert(cell1.getResult().size == n)

    pool.onQuiescenceShutdown()
  }

}
