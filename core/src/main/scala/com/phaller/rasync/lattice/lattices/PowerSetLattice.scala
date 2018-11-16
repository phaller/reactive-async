package com.phaller.rasync.lattice
package lattices

class PowerSetLattice[T] extends Lattice[Set[T]] {

  def join(left: Set[T], right: Set[T]): Set[T] =
    left ++ right

  val bottom: Set[T] =
    Set[T]()

}
