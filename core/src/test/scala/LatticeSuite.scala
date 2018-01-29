import lattice.{Lattice, LatticeViolationException, NaturalNumberLattice}
import opal._
import opal.Immutability.ImmutabilityLattice
import opal.Purity.PurityLattice
import org.scalatest.FunSuite

class LatticeSuite extends FunSuite {
  test("lteq 1") {
    val l = new NaturalNumberLattice
    assert(l.lteq(1, 2))
  }

  test("lteq 2") {
    val l = new NaturalNumberLattice
    assert(l.lteq(2, 2))
  }

  test("lt 1") {
    val l = new NaturalNumberLattice
    assert(l.lt(1, 2))
  }

  test("lt 2") {
    val l = new NaturalNumberLattice
    assert(!l.lt(2, 1))
  }

  test("gteq 1") {
    val l = new NaturalNumberLattice
    assert(l.gteq(3, 2))
  }

  test("gteq 2") {
    val l = new NaturalNumberLattice
    assert(l.gteq(2, 2))
  }

  test("gt 1") {
    val l = new NaturalNumberLattice
    assert(!l.gt(1, 2))
  }

  test("gt 2") {
    val l = new NaturalNumberLattice
    assert(l.gt(2, 1))
  }

  test("tryCompare 1") {
    val l = new NaturalNumberLattice
    assert(l.tryCompare(1, 2).get < 0)
  }

  test("tryCompare 2") {
    val l = new NaturalNumberLattice
    assert(l.tryCompare(2, 2).get == 0)
  }

  test("tryCompare 3") {
    val l = new NaturalNumberLattice
    assert(l.tryCompare(2, 1).get > 0)
  }

  test("join") {
    val l = new NaturalNumberLattice
    assert(l.join(1, 2) == 2)
  }

  test("trivial 1") {
    object A
    object B
    val l = Lattice.trivial[AnyRef]
    assert(l.lt(null, A))
    assert(l.lteq(null, A))
    assert(!l.gt(null, A))
    assert(!l.gteq(null, A))
    assert(l.tryCompare(A, A).get == 0)
    assert(l.tryCompare(A, null).get > 0)
    assert(l.tryCompare(null, A).get < 0)

    assert(!l.lt(A, B))
    assert(!l.lteq(A, B))
    assert(!l.gt(A, B))
    assert(!l.gteq(A, B))
    assert(l.tryCompare(A, B).isEmpty)
  }

  test("trivial: join 1") {
    object A
    object B
    val l = Lattice.trivial[AnyRef]
    var top = false
    try
      l.join(A, B)
    catch {
      case _: LatticeViolationException[_] => top = true
    }
    assert(top)
  }

  test("trivial: join 2") {
    object A
    object B
    val l = Lattice.trivial[AnyRef]
    assert(l.join(null, A) === A)
  }

  test("client defined lattice 1") {
    val l = PurityLattice
    val A = Pure
    val B = Impure

    assert(l.lt(l.empty, A))
    assert(l.lteq(l.empty, A))
    assert(!l.gt(l.empty, A))
    assert(!l.gteq(l.empty, A))
    assert(l.tryCompare(A, A).get == 0)
    assert(l.tryCompare(l.empty, A).get < 0)

    assert(!l.lt(A, B))
    assert(!l.lteq(A, B))
    assert(!l.gt(A, B))
    assert(!l.gteq(A, B))
    assert(l.tryCompare(A, B).isEmpty)
  }
}