package cell

import org.scalatest.FunSuite

import java.io.File
import java.util.concurrent.CountDownLatch

import scala.util.{ Success, Failure }
import scala.concurrent.Await
import scala.concurrent.duration._

import lattice._

import opal.PurityAnalysis
import org.opalj.br.analyses.Project

class InternalBaseSuite extends FunSuite {

  implicit val stringIntLattice: Lattice[Int] = new StringIntLattice

  test("cellDependencies: By adding dependencies") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "key1")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, (x: Int) => x == 0, 0)
    cell1.whenComplete(cell2, (x: Int) => x == 0, 0)

    assert(cell1.numCompleteDependencies == 2)
    assert(cell2.numCompleteDependencies == 0)
  }

  test("cellDependencies: By removing dependencies") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "key1")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, (x: Int) => x == 0, 0)
    cell1.whenComplete(cell2, (x: Int) => x == 0, 0)

    completer1.putFinal(0)

    assert(cell1.numCompleteDependencies == 0)
    assert(cell2.numCompleteDependencies == 0)
  }
}
