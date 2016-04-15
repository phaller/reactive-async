import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

import scala.util.{Success, Failure}
import scala.concurrent.Await
import scala.concurrent.duration._

import cell.{CellCompleter, HandlerPool}

import lattice._

import opal.PurityAnalysis
import org.opalj.br.analyses.Project
import java.io.File

class BaseSuite extends FunSuite {
  test("putFinal") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    val cell = completer.cell
    cell.onComplete {
      case Success(v) =>
        assert(v === 5)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putFinal(5)

    latch.await()

    pool.shutdown()
  }

  test("whenComplete") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    pool.shutdown()
  }

  test("whenComplete: dependency 1") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)

    latch.await()

    assert(cell1.dependencies.isEmpty)

    pool.shutdown()
  }

  test("whenComplete: dependency 2") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

    completer2.putFinal(9)

    cell1.waitUntilNoDeps()

    assert(cell1.dependencies.isEmpty)

    pool.shutdown()
  }

  test("handler pool") {
    val pool = new HandlerPool
    val latch = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    pool.execute { () => latch.await() }
    pool.onQuiescent { () => latch2.countDown() }
    latch.countDown()

    latch2.await()
    assert(true)

    pool.shutdown()
  }

  test("quiescent incomplete cells") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "key1")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => x == 1, 1)
    cell2.whenComplete(cell1, x => x == 1, 1)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.map(_.key).toString == "List(key2, key1)")
  }

  test("quiescent resolve cycle") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "key1")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, x => x == 0, 0)
    cell2.whenComplete(cell1, x => x == 0, 0)
    val qfut = pool.quiescentResolveCell
    Await.ready(qfut, 2.seconds)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.size == 0)
  }

  test("getResult: from complete cell") {
    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    val cell = completer.cell

    completer.putFinal(10)

    val result = cell.getResult

    assert(result == 10)
  }

  test("getResult: from incomplete cell") {
    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    val cell = completer.cell

    val result = cell.getResult

    assert(result == 0)
  }

  test("getResult: from a partially complete cell") {
    val pool = new HandlerPool
    val completer = CellCompleter[MutabilityKey.type, Mutability](pool, MutabilityKey)
    val cell = completer.cell

    completer.putNext(ConditionallyImmutable)

    val res = cell.getResult

    assert(res == ConditionallyImmutable)
  }

  test("purity analysis with Demo.java: pure methods") {
    val file = new File("lib")
    val lib = Project(file)

    val report = PurityAnalysis.doAnalyze(lib, List.empty, () => false).toConsoleString.split("\n")

    val pureMethods = List(
      "pureness.Demo{ public int pureThoughItUsesField(int,int) }",
      "pureness.Demo{ public int pureThoughItUsesField2(int,int) }",
      "pureness.Demo{ public int simplyPure(int,int) }",
      "pureness.Demo{ int foo(int) }",
      "pureness.Demo{ int bar(int) }",
      "pureness.Demo{ int fooBar(int) }",
      "pureness.Demo{ int barFoo(int) }",
      "pureness.Demo{ int m1(int) }",
      "pureness.Demo{ int m2(int) }",
      "pureness.Demo{ int m3(int) }",
      "pureness.Demo{ int cm1(int) }",
      "pureness.Demo{ int cm2(int) }",
      "pureness.Demo{ int scc0(int) }",
      "pureness.Demo{ int scc1(int) }",
      "pureness.Demo{ int scc2(int) }",
      "pureness.Demo{ int scc3(int) }"
    )

    val finalRes = pureMethods.filter(!report.contains(_))

    assert(finalRes.size == 0)
  }

  test("purity analysis with Demo.java: impure methods") {
    val file = new File("lib")
    val lib = Project(file)

    val report = PurityAnalysis.doAnalyze(lib, List.empty, () => false).toConsoleString.split("\n")

    val impureMethods = List(
      "public static int impure(int)",
      "static int npfoo(int)",
      "static int npbar(int)",
      "static int mm1(int)",
      "static int mm2(int)",
      "static int mm3(int)",
      "static int m1np(int)",
      "static int m2np(int)",
      "static int m3np(int)",
      "static int cpure(int)",
      "static int cpureCallee(int)",
      "static int cpureCalleeCallee1(int)",
      "static int cpureCalleeCallee2(int)",
      "static int cpureCalleeCalleeCallee(int)",
      "static int cpureCalleeCalleeCalleeCallee(int)"
    )

    val finalRes = impureMethods.filter(report.contains(_))

    assert(finalRes.size == 0)
  }

  test("PurityLattice: successful joins") {
    val lattice = PurenessKey.lattice

    val purity = lattice.join(UnknownPurity, Pure)
    assert(purity == Some(Pure))

    val newPurity = lattice.join(purity.get, Pure)
    assert(newPurity == None)
  }

  test("PurityLattice: failed joins") {
    val lattice = PurenessKey.lattice

    try {
      val newPurity = lattice.join(Impure, Pure)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }

    try {
      val newPurity = lattice.join(Pure, Impure)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }
  }

  test("MutabilityLattice: successful joins") {
    val lattice = MutabilityKey.lattice

    val mutability = lattice.join(UnknownMutability, ConditionallyImmutable)
    assert(mutability == Some(ConditionallyImmutable))

    val newMutability1 = lattice.join(mutability.get, Mutable)
    val newMutability2 = lattice.join(mutability.get, Immutable)
    assert(newMutability1 == Some(Mutable))
    assert(newMutability2 == Some(Immutable))
  }

  test("MutabilityLattice: failed joins") {
    val lattice = MutabilityKey.lattice

    try {
      val mutability = lattice.join(Mutable, ConditionallyImmutable)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }

    try {
      val mutability = lattice.join(Mutable, Immutable)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }

    try {
      val mutability = lattice.join(Immutable, ConditionallyImmutable)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }

    try {
      val mutability = lattice.join(Immutable, Mutable)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }
  }

  test("putNext: Successful, using MutabilityLattce") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[MutabilityKey.type, Mutability](pool, MutabilityKey)
    val completer2 = CellCompleter[MutabilityKey.type, Mutability](pool, MutabilityKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell

    completer1.putNext(ConditionallyImmutable)
    completer2.putNext(ConditionallyImmutable)

    assert(cell1.getResult == ConditionallyImmutable)

    completer1.putNext(Immutable)
    completer2.putNext(Mutable)

    assert(cell1.getResult == Immutable)
    assert(cell2.getResult == Mutable)

    pool.shutdown()
  }

  test("putNext: Failed, using MutabilityLattce") {
    val pool = new HandlerPool
    val completer = CellCompleter[MutabilityKey.type, Mutability](pool, MutabilityKey)
    val cell = completer.cell

    completer.putNext(Immutable)

    assert(cell.getResult == Immutable)

    // Should fail putNext with LatticeViolationException
    try {
      completer.putNext(ConditionallyImmutable)
      assert(false)
    } catch {
      case lve: LatticeViolationException[_] => assert(true)
      case e: Exception => assert(false)
    }

    completer.putFinal(Immutable)

    assert(cell.getResult == Immutable)

    // Should fail putNext because of IllegalStateException, cell is already complete
    try {
      completer.putNext(Immutable)
      assert(false)
    } catch {
      case ise: IllegalStateException => assert(true)
      case e: Exception => assert(false)
    }

    pool.shutdown()
  }
}
