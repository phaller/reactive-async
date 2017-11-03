package latticetest

import org.scalatest.FunSuite

import cell._
import lattice.{ Lattice, LatticeViolationException, Key }

object Util {

  def joinOfTwoElements[T](elem1: T, elem2: T)(lattice: Lattice[T]): T = {
    lattice.join(elem1, elem2)
  }

  def joinOfTwoElements2[T](elem1: T, elem2: T)(implicit lattice: Lattice[T]): T = {
    lattice.join(elem1, elem2)
  }

  def joinOfTwoElements3[T: Lattice](elem1: T, elem2: T): T = {
    val lattice = implicitly[Lattice[T]]
    lattice.join(elem1, elem2)
  }

}

class PowerSetLattice[T] extends Lattice[Set[T]] {

  def join(left: Set[T], right: Set[T]): Set[T] =
    left ++ right

  def empty: Set[T] =
    Set[T]()

}

object PowerSetLattice {

  implicit def mkLattice[T]: PowerSetLattice[T] =
    new PowerSetLattice[T]

}

class PowerSetLatticeSuite extends FunSuite {

  test("join using lattice") {
    val powerSetLattice = new PowerSetLattice[Int]
    val elem1 = Set(1, 2)
    val elem2 = Set(4, 6)
    val result = Util.joinOfTwoElements(elem1, elem2)(powerSetLattice)
    assert(result == Set(1, 2, 4, 6))
  }

  test("join using implicit lattice") {
    implicit val powerSetLattice = new PowerSetLattice[Int]
    val elem1 = Set(1, 2)
    val elem2 = Set(4, 6)
    val result = Util.joinOfTwoElements2(elem1, elem2)
    assert(result == Set(1, 2, 4, 6))
  }

  test("join using implicit lattice 2") {
    import PowerSetLattice._ // imported: implicit def mkLattice[T]: PowerSetLattice[T]
    // type checker knows: PowerSetLattice[T] <: Lattice[Set[T]]
    // type checker knows: calling mkLattice[Int] returns PowerSetLattice[Int] <: Lattice[Set[Int]]
    val elem1 = Set(1, 2)
    val elem2 = Set(4, 6)
    val result = Util.joinOfTwoElements2(elem1, elem2) /* (mkLattice[Int]) */
    assert(result == Set(1, 2, 4, 6))
  }

  test("join using implicit lattice 3") {
    import PowerSetLattice._ // imported: implicit def mkLattice[T]: PowerSetLattice[T]
    // type checker knows: PowerSetLattice[T] <: Lattice[Set[T]]
    // type checker knows: calling mkLattice[Int] returns PowerSetLattice[Int] <: Lattice[Set[Int]]
    val elem1 = Set(1, 2)
    val elem2 = Set(4, 6)
    val result = Util.joinOfTwoElements3(elem1, elem2) /* (mkLattice[Int]) */
    assert(result == Set(1, 2, 4, 6))
  }

  // does not compile, because there is no type class instance for type Int
  /*test("join using implicit lattice 4") {
    import PowerSetLattice._

    val elem1 = 2
    val elem2 = 4
    val result = Util.joinOfTwoElements3(elem1, elem2)
  }*/

}
