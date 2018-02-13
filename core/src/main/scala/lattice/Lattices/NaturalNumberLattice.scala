package lattice

object NaturalNumberKey extends DefaultKey[Int]

class NaturalNumberLattice extends Lattice[Int] {
  override def join(current: Int, next: Int): Int = {
    if (next > current) next
    else current
  }

  override def lteq(v1: Int, v2: Int): Boolean = v1 <= v2
  override def gteq(v1: Int, v2: Int): Boolean = v1 >= v2
  override def lt(v1: Int, v2: Int): Boolean = v1 < v2
  override def gt(v1: Int, v2: Int): Boolean = v1 > v2
  override def tryCompare(x: Int, y: Int): Option[Int] = Some(x - y)

  override def empty: Int = 0
}
