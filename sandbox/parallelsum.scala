import lattice.{DefaultKey, Lattice, LatticeViolationException, NaturalNumberLattice, NaturalNumberKey}
import cell.{Cell, CellCompleter, HandlerPool}

import scala.util.{Try, Success, Failure}
import scala.collection.immutable.{IndexedSeq, Vector}

import scala.concurrent.{Future, Promise, ExecutionContext}

import java.util.concurrent.{CountDownLatch, ForkJoinPool}
import java.util.concurrent.atomic.AtomicLong


object ParallelSum {

  type K = DefaultKey[Int]

  implicit val intLattice: Lattice[Int] = new NaturalNumberLattice

  def parallelSumFut(l: IndexedSeq[Int])(implicit ctx: ExecutionContext): Future[Int] = {
    val chunk = l.size / 2
    // chunk = half of sequential size
    if (chunk == 16384) {  // sequential
      val sum = l.reduce(_ + _)
      val p = Promise[Int]()
      p.success(sum).future
    } else {
      val (left, right) = l.splitAt(chunk)
      // introduce parallelism
      val leftPromise = Promise[Int]()
      ctx.execute(new Runnable {
        def run(): Unit = {
          val res = parallelSumFut(left)
          leftPromise.completeWith(res)
        }
      })
      val rightFut = parallelSumFut(right)
      val zipped = leftPromise.future.zip(rightFut)
      zipped.map(tup => tup._1 + tup._2)
    }
  }

  // final result is eventually put into returned cell
  def parallelSum(l: IndexedSeq[Int])(implicit pool: HandlerPool): Cell[K, Int] = {
    val chunk = l.size / 2
    // chunk = half of sequential size
    if (chunk == 16384) {  // sequential
      val sum = l.reduce(_ + _)
      val completer = CellCompleter[K, Int](pool, new DefaultKey[Int])
      completer.putFinal(sum)
      completer.cell
    } else {
      val (left, right) = l.splitAt(chunk)
      // introduce parallelism
      val leftCompleter = CellCompleter[K, Int](pool, new DefaultKey[Int])
      pool.execute { () =>
        val res = parallelSum(left)
        // TODO: replace with whenCompleteEx
        // res.whenComplete
        res.onComplete {
          case Success(x) => leftCompleter.putFinal(x)
          case f @ Failure(_) => leftCompleter.tryComplete(f)
        }
      }
      val rightCell = parallelSum(right)
      val zipped = leftCompleter.cell.zipFinal(rightCell)
      val completer = CellCompleter[K, Int](pool, new DefaultKey[Int])
      // TODO: replace with `mapFinal`
      zipped.onComplete {
        case Success((x, y)) => completer.putFinal(x + y)
        case f @ Failure(_)  => completer.tryComplete(f.asInstanceOf[Try[Int]])
      }
      completer.cell
    }
  }

  // returns duration in ns
  def oneIteration(list: IndexedSeq[Int])(implicit pool: HandlerPool): Long = {
    val latch = new CountDownLatch(1)
    val time  = new AtomicLong

    val startCell = System.nanoTime()
    val res = parallelSum(list)
    res.onComplete {
      case Success(x) =>
        val endCell = System.nanoTime()
        time.lazySet(endCell - startCell)
        latch.countDown()
      case Failure(t) =>
        assert(false)
        latch.countDown()
    }

    latch.await()
    time.get()
  }

  // returns duration in ns
  def oneIterationFut(list: IndexedSeq[Int])(implicit ctx: ExecutionContext): Long = {
    val latch = new CountDownLatch(1)
    val time  = new AtomicLong

    val start = System.nanoTime()
    val res = parallelSumFut(list)
    res.onComplete {
      case Success(x) =>
        val end = System.nanoTime()
        time.lazySet(end - start)
        latch.countDown()
      case Failure(t) =>
        assert(false)
        latch.countDown()
    }

    latch.await()
    time.get()
  }

  def oneIterationSequential(seq: IndexedSeq[Int]): (Int, Long) = {
    val start = System.nanoTime()
    val sum = seq.reduce(_ + _)
    val end = System.nanoTime()
    (sum, end - start)
  }

  def main(args: Array[String]): Unit = {
    val numThreads = args(1).toInt
    println(s"Using $numThreads threads")

    val seed: Int = 5
    val rnd = new scala.util.Random(seed)
    //val list = List.fill(1048576) { rnd.nextInt(1000) }
    //val list = List.fill(2048) { rnd.nextInt(1000) }
    // out of memory:
    //val list: IndexedSeq[Int] = Vector.fill(1073741824) { rnd.nextInt(1) }
    // 2 ^ 24 = 16777216
    //val list: IndexedSeq[Int] = Vector.fill(16777216) { rnd.nextInt(1) }
    val seqSize = 2 << (args(0).toInt - 1)
    println(s"size of sequence: $seqSize")
    if (seqSize < 16384) {
      println(s"ERROR: size of sequence must be at least 16384")
      return
    }

    val list: IndexedSeq[Int] = Vector.fill(seqSize) { rnd.nextInt(1) }

    // reach steady state
    for (_ <- 1 to 100)
      oneIterationSequential(list)

    // average of 9 iterations
    val durationsSeq = (1 to 9).map(i => oneIterationSequential(list))
    val durationSumSeq = durationsSeq.map(tup => tup._2 / 1000000).reduce(_ + _)
    val resSeq = durationSumSeq / 9
    //println(s"sum of all integers: $expectedResult")
    println(s"time (sequential): $resSeq ms")

    implicit val pool = new HandlerPool(numThreads)

    // reach steady state
    for (_ <- 1 to 100)
      oneIteration(list)

    // average of 9 iterations
    val durations = (1 to 9).map(i => oneIteration(list))
    val durationSum = durations.map(d => d / 1000000).reduce(_ + _)
    val res = durationSum / 9
    println(s"time (cells): $res ms")

    implicit val ctx = ExecutionContext.fromExecutorService(new ForkJoinPool(numThreads))
    // reach steady state
    for (_ <- 1 to 100)
      oneIterationFut(list)

    // average of 9 iterations
    val durationsFut = (1 to 9).map(i => oneIterationFut(list))
    val durationSumFut = durationsFut.map(d => d / 1000000).reduce(_ + _)
    val resFut = durationSumFut / 9
    println(s"time (futures): $resFut ms")

    // clean up
    pool.shutdown()
  }
}
