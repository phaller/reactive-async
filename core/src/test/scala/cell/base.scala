package cell

import org.scalatest.FunSuite

import java.util.concurrent.CountDownLatch

import scala.util.{ Success, Failure }
import scala.concurrent.{ Promise, Await }
import scala.concurrent.duration._

import lattice.{ Lattice, StringIntLattice, LatticeViolationException, StringIntKey }

import org.opalj.fpcf.analyses.FieldMutabilityAnalysis
import org.opalj.fpcf.properties.FieldMutability
import org.opalj.fpcf.FPCFAnalysesManager
import org.opalj.fpcf.FPCFAnalysis
import org.opalj.fpcf.FPCFAnalysesManagerKey
import opal._
import org.opalj.br.analyses.Project
import java.io.File

class BaseSuite extends FunSuite {

  implicit val stringIntLattice: Lattice[Int] = new StringIntLattice

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

  test("putFinal: 2 putFinals with same value to the same cell") {
    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    val cell = completer.cell

    completer.putFinal(5)

    try {
      completer.putFinal(5)
      assert(true)
    } catch {
      case e: Exception => assert(false)
    }

    pool.shutdown()
  }

  test("putFinal: putFinal on complete cell without adding new information") {
    val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val cell = completer.cell

    completer.putFinal(Mutable)

    try {
      completer.putFinal(ConditionallyImmutable)
      assert(false)
    } catch {
      case ise: IllegalStateException => assert(true)
      case e: Exception => assert(false)
    }

    pool.shutdown()
  }

  test("putFinal: putFinal on complete cell adding new information") {
    val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val cell = completer.cell

    completer.putFinal(ConditionallyImmutable)

    try {
      completer.putFinal(Mutable)
      assert(false)
    } catch {
      case ise: IllegalStateException => assert(true)
      case e: Exception => assert(false)
    }

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

    assert(cell1.numCompleteDependencies == 0)

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

    assert(cell1.numCompleteDependencies == 0)

    pool.shutdown()
  }

  test("whenComplete: callback removal") {
    val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)

    completer1.cell.whenComplete(completer2.cell, (imm: Immutability) => imm == Mutable, Mutable)

    completer1.putFinal(Immutable)
    assert(completer2.cell.numCompleteCallbacks == 0)
    completer2.putFinal(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.shutdown()
  }

  test("onNext") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")

    val cell = completer.cell

    cell.onNext {
      case Success(x) =>
        assert(x === 9)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer.putNext(9)

    latch.await()

    pool.shutdown()
  }

  test("whenNext") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNext
      else FalsePred
    }, 20)

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(10)
    latch.await()

    assert(cell1.numNextDependencies == 1)

    pool.shutdown()
  }

  test("whenNext: dependency 1") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNext
      else FalsePred
    }, 20)

    cell1.onNext {
      case Success(x) =>
        assert(x === 8)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer1.putNext(8)

    latch.await()

    pool.shutdown()
  }

  test("whenNext: dependency 2") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNext
      else FalsePred
    }, 30)
    cell1.whenComplete(completer2.cell, (x: Int) => x == 10, 20)

    assert(cell1.numNextDependencies == 1)

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    cell1.onNext {
      case Success(x) =>
        assert(false)
      case Failure(e) =>
        assert(false)
    }

    completer2.putFinal(10)
    latch.await()

    assert(cell1.numNextDependencies == 0)

    pool.shutdown()
  }

  test("whenNext: Triggered by putFinal when no whenComplete exist for same cell") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNext
      else FalsePred
    }, 20)

    assert(cell1.numNextDependencies == 1)

    cell1.onNext {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putFinal(10)
    latch.await()

    pool.shutdown()
  }

  test("whenNext: Remove whenNext dependencies from cells that depend on a completing cell") {
    /* Needs the dependees from the feature/cscc-resolving branch */
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNext
      else FalsePred
    }, 20)

    completer2.putFinal(10)

    cell1.waitUntilNoNextDeps()

    assert(cell1.numNextDependencies == 0)

    pool.shutdown()
  }

  test("whenNext: callback removal") {
    val pool = new HandlerPool

    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)

    completer1.cell.whenNext(completer2.cell, (imm: Immutability) => imm match {
      case Mutable => WhenNext
      case _ => FalsePred
    }, Mutable)

    completer1.putFinal(Immutable)
    assert(completer2.cell.numNextCallbacks == 0)
    completer2.putNext(Mutable)

    assert(completer1.cell.getResult() == Immutable)
    assert(completer2.cell.getResult() == Mutable)

    pool.shutdown()
  }

  test("whenNext: Dependencies concurrency test") {
    val pool = new HandlerPool
    val latch = new CountDownLatch(10000)
    val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)

    for (i <- 1 to 10000) {
      pool.execute(() => {
        completer1.cell.whenNext(completer2.cell, (x: Immutability) => {
          if (x == Mutable) WhenNext else FalsePred
        }, Mutable)
        latch.countDown()
      })
    }

    latch.await()

    assert(completer1.cell.numNextDependencies == 10000)

    pool.shutdown()
  }

  test("whenNext: One cell with several dependencies on the same cell concurrency test") {
    val pool = new HandlerPool

    for (i <- 1 to 1000) {
      val completer1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      completer1.cell.whenNext(completer2.cell, (imm: Immutability) => imm match {
        case Immutable | ConditionallyImmutable => FalsePred
        case Mutable => WhenNext
      }, Mutable)

      assert(completer1.cell.numTotalDependencies == 1)

      pool.execute(() => completer2.putNext(ConditionallyImmutable))
      pool.execute(() => completer2.putFinal(Mutable))

      val fut = pool.quiescentResolveCell
      Await.result(fut, 2.second)

      assert(completer2.cell.getResult() == Mutable)
      assert(completer1.cell.getResult() == Mutable)
    }

    pool.shutdown()
  }

  test("whenNextComplete") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNextComplete
      else FalsePred
    }, 20)

    cell1.onComplete {
      case Success(v) =>
        assert(v === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    // This will complete `cell1`
    completer2.putNext(10)

    latch.await()

    // cell1 should be completed, so putNext(5) should not succeed
    try {
      completer1.putNext(5)
      assert(false)
    } catch {
      case ise: IllegalStateException => assert(true)
      case e: Exception => assert(false)
    }

    pool.shutdown()
  }

  test("whenNextComplete: dependency 1") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNextComplete
      else FalsePred
    }, 20)

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }

    completer2.putNext(9)
    completer2.putNext(10)

    latch.await()

    pool.shutdown()
  }

  test("whenNextComplete: dependency 2") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "somekey")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "someotherkey")

    val cell1 = completer1.cell
    cell1.whenNext(completer2.cell, (x: Int) => {
      if (x == 10) WhenNextComplete
      else FalsePred
    }, 20)

    cell1.onNext {
      case Success(x) =>
        assert(x === 8)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 20)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    completer2.putNext(10)
    latch2.await()

    pool.shutdown()
  }

  test("put: isFinal == true") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    completer.cell.onComplete {
      case Success(v) =>
        assert(v === 6)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.put(6, true)

    latch.await()
    pool.shutdown()
  }

  test("put: isFinal == false") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool
    val completer = CellCompleter[StringIntKey, Int](pool, "somekey")
    completer.cell.onNext {
      case Success(x) =>
        assert(x === 10)
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.put(10, false)

    latch.await()
    pool.shutdown()
  }

  test("putFinal: result passed to callbacks") {
    val latch = new CountDownLatch(1)

    implicit val setLattice = new Lattice[Set[Int]] {
      def join(curr: Set[Int], next: Set[Int]) = curr ++ next
      def empty = Set.empty[Int]
    }
    val key = new lattice.DefaultKey[Set[Int]]

    val pool = new HandlerPool
    val completer = CellCompleter[key.type, Set[Int]](pool, key)
    val cell = completer.cell
    cell.onComplete {
      case Success(v) =>
        assert(v === Set(3, 4, 5))
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putNext(Set(3, 5))
    completer.putFinal(Set(4))

    latch.await()
    pool.shutdown()
  }

  test("putNext: result passed to callbacks") {
    val latch = new CountDownLatch(1)

    implicit val setLattice = new Lattice[Set[Int]] {
      def join(curr: Set[Int], next: Set[Int]) = curr ++ next
      def empty = Set.empty[Int]
    }
    val key = new lattice.DefaultKey[Set[Int]]

    val pool = new HandlerPool
    val completer = CellCompleter[key.type, Set[Int]](pool, key)
    val cell = completer.cell
    completer.putNext(Set(3, 5))
    cell.onNext {
      case Success(v) =>
        assert(v === Set(3, 4, 5))
        latch.countDown()
      case Failure(e) =>
        assert(false)
        latch.countDown()
    }
    completer.putNext(Set(4))

    latch.await()
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
    cell1.whenComplete(cell2, (x: Int) => x == 1, 1)
    cell2.whenComplete(cell1, (x: Int) => x == 1, 1)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.map(_.key).toString == "List(key1, key2)")
  }

  test("quiescent resolve cycle") {
    val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int](pool, "key1")
    val completer2 = CellCompleter[StringIntKey, Int](pool, "key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenComplete(cell2, (x: Int) => x == 0, 0)
    cell2.whenComplete(cell1, (x: Int) => x == 0, 0)
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
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val cell = completer.cell

    completer.putNext(ConditionallyImmutable)

    val res = cell.getResult

    assert(res == ConditionallyImmutable)
  }

  test("purity analysis with Demo.java: pure methods") {
    val file = new File("core")
    val lib = Project(file)

    val report = PurityAnalysis.doAnalyze(lib, List.empty, () => false).toConsoleString.split("\n")

    val pureMethods = List(
      "pureness.Demo{ public static int pureThoughItUsesField(int,int) }",
      "pureness.Demo{ public static int pureThoughItUsesField2(int,int) }",
      "pureness.Demo{ public static int simplyPure(int,int) }",
      "pureness.Demo{ static int foo(int) }",
      "pureness.Demo{ static int bar(int) }",
      "pureness.Demo{ static int fooBar(int) }",
      "pureness.Demo{ static int barFoo(int) }",
      "pureness.Demo{ static int m1(int) }",
      "pureness.Demo{ static int m2(int) }",
      "pureness.Demo{ static int m3(int) }",
      "pureness.Demo{ static int cm1(int) }",
      "pureness.Demo{ static int cm2(int) }",
      "pureness.Demo{ static int scc0(int) }",
      "pureness.Demo{ static int scc1(int) }",
      "pureness.Demo{ static int scc2(int) }",
      "pureness.Demo{ static int scc3(int) }")

    val finalRes = pureMethods.filter(!report.contains(_))

    assert(finalRes.size == 0, report.mkString("\n"))
  }

  test("purity analysis with Demo.java: impure methods") {
    val file = new File("core")
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
      "static int cpureCalleeCalleeCalleeCallee(int)")

    val finalRes = impureMethods.filter(report.contains(_))

    assert(finalRes.size == 0)
  }

  test("PurityLattice: successful joins") {
    val lattice = Purity.PurityLattice

    val purity = lattice.join(UnknownPurity, Pure)
    assert(purity == Pure)

    val newPurity = lattice.join(purity, Pure)
    assert(newPurity == Pure)
  }

  test("PurityLattice: failed joins") {
    val lattice = Purity.PurityLattice

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

  test("putNext: Successful, using ImmutabilityLattce") {
    val pool = new HandlerPool
    val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val cell = completer.cell

    completer.putNext(Immutable)
    assert(cell.getResult == Immutable)

    completer.putNext(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Should be allowed because it's the same value and is allowed by the lattice
    completer.putFinal(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Even though cell is completed, this should be allowed because it's the same
    // value and is allowed by the lattice
    completer.putNext(ConditionallyImmutable)
    assert(cell.getResult == ConditionallyImmutable)

    // Even though cell is completed, this should be allowed because it wont add any new information
    completer.putNext(Immutable)
    assert(cell.getResult() == ConditionallyImmutable)

    pool.shutdown()
  }

  test("putNext: Failed, using ImmutabilityLattce") {
    val pool = new HandlerPool

    val completer2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
    val cell2 = completer2.cell

    completer2.putFinal(Immutable)

    // Should fail putNext with IllegalStateException because of adding new information
    // to an already complete cell
    try {
      completer2.putNext(ConditionallyImmutable)
      assert(false)
    } catch {
      case ise: IllegalStateException => assert(true)
      case e: Exception => assert(false)
    }

    pool.shutdown()
  }

  test("putNext: concurrency test") {
    val pool = new HandlerPool

    for (i <- 1 to 10000) {
      val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)

      pool.execute(() => completer.putNext(Immutable))
      pool.execute(() => completer.putNext(ConditionallyImmutable))
      pool.execute(() => completer.putNext(Mutable))

      val p = Promise[Boolean]()
      pool.onQuiescent { () => p.success(true) }

      try {
        Await.result(p.future, 2.seconds)
      } catch {
        case t: Throwable => assert(false, s"failure after $i iterations")
      }

      assert(completer.cell.getResult() == Mutable)
    }

    pool.shutdown()
  }

  test("putNext and putFinal: concurrency test") {
    val pool = new HandlerPool

    for (i <- 1 to 10000) {
      val completer = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)

      pool.execute(() => completer.putNext(Immutable))
      pool.execute(() => completer.putNext(ConditionallyImmutable))
      pool.execute(() => completer.putFinal(Mutable))

      val p = Promise[Boolean]()
      pool.onQuiescent { () => p.success(true) }

      try {
        Await.result(p.future, 2.seconds)
      } catch {
        case t: Throwable => assert(false, s"failure after $i iterations")
      }

      assert(completer.cell.getResult() == Mutable)
    }

    pool.shutdown()
  }

  test("New ImmutabilityLattice: successful joins") {
    val lattice = Immutability.ImmutabilityLattice

    val mutability1 = lattice.join(Immutable, ConditionallyImmutable)
    assert(mutability1 == ConditionallyImmutable)

    val mutability2 = lattice.join(ConditionallyImmutable, Mutable)
    assert(mutability2 == Mutable)

    val mutability3 = lattice.join(Immutable, Mutable)
    assert(mutability3 == Mutable)

    val mutability4 = lattice.join(ConditionallyImmutable, Immutable)
    assert(mutability4 == ConditionallyImmutable)

    val mutability5 = lattice.join(Mutable, ConditionallyImmutable)
    assert(mutability5 == Mutable)

    val mutability6 = lattice.join(Mutable, Immutable)
    assert(mutability6 == Mutable)
  }

  test("if exception-throwing tasks should still run quiescent handlers") {
    val intMaxLattice = new Lattice[Int] {
      def join(current: Int, next: Int) = math.max(current, next)
      def empty = 0
    }
    val key = new lattice.DefaultKey[Int]

    val pool = new HandlerPool(unhandledExceptionHandler = { t => /* do nothing */ })
    val completer = CellCompleter[key.type, Int](pool, key)(intMaxLattice)

    pool.execute { () =>
      // NOTE: This will print a stacktrace, but that is fine (not a bug).
      throw new Exception(
        "Even if this happens, quiescent handlers should still run.")
    }

    try {
      Await.result(pool.quiescentIncompleteCells, 1.seconds)
    } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }

    pool.shutdown()
  }

  /*test("ImmutabilityAnalysis: Concurrency") {
    val file = new File("lib")
    val lib = Project(file)

    val manager = lib.get(FPCFAnalysesManagerKey)
    manager.run(ClassExtensibilityAnalysis)
    manager.runAll(
      FieldMutabilityAnalysis
    )

    // Compare every next result received from the same analysis to `report`
    val report = ImmutabilityAnalysis.analyzeWithoutClassExtensibilityAndFieldMutabilityAnalysis(lib, manager).toConsoleString.split("\n")

    for (i <- 0 to 1000) {
      // Next result
      val newReport = ImmutabilityAnalysis.analyzeWithoutClassExtensibilityAndFieldMutabilityAnalysis(lib, manager).toConsoleString.split("\n")

      // Differs between the elements in `report` and `newReport`.
      // If they have the exact same elements, `finalRes` should be an
      // empty list.
      val finalRes = report.filterNot(newReport.toSet)

      assert(finalRes.isEmpty)
    }
  }*/
}
