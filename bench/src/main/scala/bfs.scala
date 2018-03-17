
import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import cell.{Cell, CellCompleter, HandlerPool, WhenNext}
import lattice.{Lattice, LatticeViolationException, Key}

import scala.annotation.tailrec

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._


trait DefaultKey[T] extends Key[T] {
  def resolve[K <: Key[T]](cells: Seq[Cell[K, T]]): Seq[(Cell[K, T], T)] = Seq()
  def default[K <: Key[T]](cells: Seq[Cell[K, T]]): Seq[(Cell[K, T], T)] = Seq()
}

object BooleanLattice extends Lattice[Boolean] {
  def join(current: Boolean, next: Boolean): Boolean = {
    if (current && !next) throw LatticeViolationException(current, next)
    else next
  }

  def empty: Boolean = false
}

object BooleanKey extends DefaultKey[Boolean] {
  val lattice = BooleanLattice
}

object VertexSetLattice extends Lattice[Set[Vertex]] {
  def join(current: Set[Vertex], next: Set[Vertex]): Set[Vertex] =
    current ++ next
  def empty: Set[Vertex] = Set()
}

object VertexSetKey extends DefaultKey[Set[Vertex]] {
  val lattice = VertexSetLattice
}

class Vertex(val value: Int)(implicit pool: HandlerPool) {
  val visited = new AtomicBoolean

  var neighbors: List[Vertex] =
    List()
}

object BFSBenchmark {

  val NumVertices = 100000

  def createBinTree(rnd: Random, n: Int)(implicit pool: HandlerPool): Vertex = {
    val parent = new Vertex(rnd.nextInt(100))
    if (n == 0) parent
    else {
      val left = createBinTree(rnd, n-1)
      val right = createBinTree(rnd, n-1)
      parent.neighbors = List(left, right)
      parent
    }
  }

  val count = new AtomicInteger
  val CutOff = 20

  def bfs(root: Vertex): Unit = {
    val notYetVisited = root.visited.compareAndSet(false, true)
    if (notYetVisited) {
      if (root.value < CutOff) {
        count.incrementAndGet()
      }
      for (n <- root.neighbors)
        bfs(n)
    }
  }

  // not concurrent
  def bfs2(root: Vertex, completer: CellCompleter[VertexSetKey.type, Set[Vertex]]): Unit = {
    val notYetVisited = root.visited.compareAndSet(false, true)
    if (notYetVisited) {
      if (root.value < CutOff) {
        completer.putNext(Set(root))
      }
      for (n <- root.neighbors)
        bfs2(n, completer)
    }
  }

  // concurrent
  def bfs3(root: Vertex, completer: CellCompleter[VertexSetKey.type, Set[Vertex]])(implicit pool: HandlerPool): Unit = {
    val notYetVisited = root.visited.compareAndSet(false, true)
    if (notYetVisited) {
      if (root.value < CutOff) {
        completer.putNext(Set(root))
      }
      for (n <- root.neighbors) {
        pool.execute { () =>
          bfs3(n, completer)
        }
      }
    }
  }

  def bfs4(i: Int, root: Vertex, completer: CellCompleter[VertexSetKey.type, Set[Vertex]])(implicit pool: HandlerPool): Unit = {
    val notYetVisited = root.visited.compareAndSet(false, true)
    if (notYetVisited) {
      if (root.value < CutOff) {
        //println(s"adding vertex ${root.value}")
        completer.putNext(Set(root))
      }
      for (n <- root.neighbors) {
        if (i > 14) {
          pool.execute { () =>
            val subcompleter = CellCompleter[VertexSetKey.type, Set[Vertex]](pool, VertexSetKey)
            completer.cell.whenNext(subcompleter.cell, (nset: Set[Vertex]) => WhenNext, None)
            bfs4(i-1, n, subcompleter)
          }
        } else {
          bfs4(i-1, n, completer)
        }
      }
    }
  }

  val pools = (1 to 8).map(i => new HandlerPool(i))

  // pass duration in micro secs to continuation
  def runBfs3With(nthreads: Int, rnd: Random)(cont: Long => Unit): Unit = {
    implicit val pool = pools(nthreads - 1)

    val root = createBinTree(rnd, 17)
    val completer = CellCompleter[VertexSetKey.type, Set[Vertex]](pool, VertexSetKey)

    val t1 = System.nanoTime()
    pool.execute { () =>
      bfs3(root, completer)
    }
    pool.onQuiescent { () =>
      val t2 = System.nanoTime()
      cont((t2 - t1)/1000)
    }
  }

  val BinTreeDepth = 18

  def resetVisited(root: Vertex): Unit = {
    root.visited.set(false)
    for (n <- root.neighbors) resetVisited(n)
  }

  def runBfs3Repeatedly(nthreads: Int, root: Vertex)(cont: List[Long] => Unit): Unit = {
    implicit val pool = pools(nthreads - 1)
    val completer = CellCompleter[VertexSetKey.type, Set[Vertex]](pool, VertexSetKey)
    // reset all visited members in root
    resetVisited(root)

    val todo: (Long => Unit) => Unit = { (innerCont: Long => Unit) =>
      val t1 = System.nanoTime()
      pool.execute { () =>
        bfs3(root, completer)
      }
      pool.onQuiescent { () =>
        val t2 = System.nanoTime()
        innerCont((t2 - t1)/1000)
      }
    }
    repeat(7, List[Long]())(todo)(cont)
  }

  def runBfs4Repeatedly(nthreads: Int, root: Vertex)(cont: List[Long] => Unit): Unit = {
    implicit val pool = pools(nthreads - 1)
    // reset all visited members in root
    resetVisited(root)
    val completer = CellCompleter[VertexSetKey.type, Set[Vertex]](pool, VertexSetKey)
    val todo: (Long => Unit) => Unit = { (innerCont: Long => Unit) =>
      val t1 = System.nanoTime()
      pool.execute { () =>
        bfs4(BinTreeDepth, root, completer)
      }
      pool.onQuiescent { () =>
        val t2 = System.nanoTime()
        innerCont((t2 - t1)/1000)
      }
    }

    repeat(7, List[Long]())(todo)(cont)
  }

  def repeat[T](n: Int, acc: List[T])(todo: (T => Unit) => Unit)(cont: List[T] => Unit): Unit = {
    if (n == 0) cont(acc)
    else
      todo { (singleResult: T) =>
        repeat(n-1, singleResult :: acc)(todo)(cont)
      }
  }

  def main(args: Array[String]): Unit = {
    // handler pool
    implicit val pool = new HandlerPool

    val seed: Long = 52436L
    var rnd = new Random(seed)

    // binary tree with 2^17 = 131'072 nodes
    val binTree = createBinTree(rnd, BinTreeDepth)

    // run bfs
    val t1 = System.nanoTime()
    bfs(binTree)
    val t2 = System.nanoTime()

    println(s"1st run: found ${count.get()} vertices whose value is below a cut-off of ${CutOff}% (took ${(t2 - t1)/1000} micro secs)")

    // reset count
    count.set(0)
    // re-create tree (to reset visited)
    rnd = new Random(seed)
    val binTree2 = createBinTree(rnd, BinTreeDepth)
    // run bfs
    val t3 = System.nanoTime()
    bfs(binTree2)
    val t4 = System.nanoTime()

    println(s"2nd run: found ${count.get()} vertices whose value is below a cut-off of ${CutOff}% (took ${(t4 - t3)/1000} micro secs)")

    // re-create tree (to reset visited)
    rnd = new Random(seed)
    val binTree3 = createBinTree(rnd, BinTreeDepth)
    val completer = CellCompleter[VertexSetKey.type, Set[Vertex]](pool, VertexSetKey)
    // run bfs2
    val t5 = System.nanoTime()
    bfs2(binTree3, completer)
    pool.onQuiescent { () =>
      val t6 = System.nanoTime()
      println(s"3nd run (bfs2, non-concurrent): found ${completer.cell.getResult().size} vertices whose value is below a cut-off of ${CutOff}% (took ${(t6 - t5)/1000} micro secs)")

      rnd = new Random(seed)
      val root = createBinTree(rnd, BinTreeDepth)

      runBfs3Repeatedly(8, root) { times =>
        val avg8 = times.drop(1).reduce(_ + _) / 6
        println(s"bfs3, concurrent, 8 threads: took on average (6 runs) $avg8 micro secs")

        runBfs3Repeatedly(4, root) { times =>
          val avg4 = times.drop(1).reduce(_ + _) / 6
          println(s"bfs3, concurrent, 4 threads: took on average (6 runs) $avg4 micro secs")

          runBfs3Repeatedly(2, root) { times =>
            val avg2 = times.drop(1).reduce(_ + _) / 6
            println(s"bfs3, concurrent, 2 threads: took on average (6 runs) $avg2 micro secs")

            runBfs3Repeatedly(1, root) { times =>
              val avg1 = times.drop(1).reduce(_ + _) / 6
              println(s"bfs3, concurrent, 1 threads: took on average (6 runs) $avg1 micro secs")

              runBfs4Repeatedly(8, root) { times =>
                val aavg8 = times.drop(1).reduce(_ + _) / 6
                println(s"bfs4, concurrent, 8 threads: took on average (6 runs) $aavg8 micro secs")

                runBfs4Repeatedly(4, root) { times =>
                  val aavg4 = times.drop(1).reduce(_ + _) / 6
                  println(s"bfs4, concurrent, 4 threads: took on average (6 runs) $aavg4 micro secs")

                  runBfs4Repeatedly(2, root) { times =>
                    val aavg2 = times.drop(1).reduce(_ + _) / 6
                    println(s"bfs4, concurrent, 2 threads: took on average (6 runs) $aavg2 micro secs")

                    runBfs4Repeatedly(1, root) { times =>
                      val aavg1 = times.drop(1).reduce(_ + _) / 6
                      println(s"bfs4, concurrent, 1 threads: took on average (6 runs) $aavg1 micro secs")
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    Thread.sleep(args(0).toInt * 1000)
  }

}
