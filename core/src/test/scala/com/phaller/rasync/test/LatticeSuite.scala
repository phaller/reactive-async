package com.phaller.rasync
package test

import com.phaller.rasync.lattice.lattices.NaturalNumberLattice
import com.phaller.rasync.lattice.{ Lattice, PartialOrderingWithBottom }
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
    val l = PartialOrderingWithBottom.trivial[AnyRef]
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

  test("client defined lattice 1") {
    sealed trait Value
    case object Bottom extends Value
    case object A extends Value
    case object B extends Value
    case object Top extends Value

    object L extends Lattice[Value] {
      override def join(v1: Value, v2: Value): Value =
        if (v1 == Bottom) v2
        else if (v2 == Bottom) v1
        else if (v1 == v2) v1
        else Top

      override val bottom: Value = Bottom
    }

    assert(L.lt(L.bottom, A))
    assert(L.lteq(L.bottom, A))
    assert(!L.gt(L.bottom, A))
    assert(!L.gteq(L.bottom, A))
    assert(L.tryCompare(A, A).get == 0)
    assert(L.tryCompare(L.bottom, A).get < 0)

    assert(!L.lt(A, B))
    assert(!L.lteq(A, B))
    assert(!L.gt(A, B))
    assert(!L.gteq(A, B))
    assert(L.tryCompare(A, B).isEmpty)

    assert(L.join(A, B) == Top)
    assert(L.join(Bottom, A) == A)
    assert(L.join(B, Bottom) == B)
    assert(L.join(A, Top) == Top)
    assert(L.join(Top, B) == Top)
    assert(L.join(Bottom, Top) == Top)
    assert(L.join(Top, Bottom) == Top)
    assert(L.join(A, A) == A)
  }
}
