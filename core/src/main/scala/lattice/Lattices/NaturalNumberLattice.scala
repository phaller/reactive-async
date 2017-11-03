package lattice

object NaturalNumberKey extends DefaultKey[Int]

class NaturalNumberLattice extends Lattice[Int] {
  override def join(current: Int, next: Int): Int = {
    if (next > current) next
    else current
  }

  override def empty: Int = 0
}
