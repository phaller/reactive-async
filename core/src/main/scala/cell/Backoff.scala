package cell

// Exponential backoff
// Taken, and adapted from ChemistrySet, see: https://github.com/aturon/ChemistrySet

object Backoff {
  val maxCount: Int = 14
  var procs = Runtime.getRuntime.availableProcessors
  def apply() = new Backoff
}
final class Backoff {
  import Backoff._

  var seed: Long = Thread.currentThread.getId
  var count = 0

  def getCount() = count

  // compute6 from j.u.c.
  private def noop(times: Int = 1): Int = {
    var seed: Int = 1
    var n: Int = times
    while (seed == 1 || n > 0) { // need to inspect seed or is optimized away
      seed = seed ^ (seed << 1)
      seed = seed ^ (seed >>> 3)
      seed = seed ^ (seed << 10)
      n -= 1
    }
    seed
  }

  def once() {
    if (count == 0)
      count = 1
    else {
      seed = Random.nextSeed(seed)
      noop(Random.scale(seed, (procs - 1) << (count + 2)))
      if (count < maxCount) count += 1
    }
  }
}

// an unsynchronized, but thread-varying RNG
private final class Random(var seed: Long = 1) {
  def nextSeed {
    seed = Random.nextSeed(seed)
  }

  def next(max: Int): Int = {
    nextSeed
    Random.scale(seed, max)
  }
}
private object Random {
  def nextSeed(oldSeed: Long): Long = {
    var seed = oldSeed
    seed = seed ^ (seed << 13)
    seed = seed ^ (seed >>> 7)
    seed = seed ^ (seed << 17)
    seed
  }
  def scale(seed: Long, max: Int): Int = {
    if (max <= 0) max else {
      val r = (seed % max).toInt
      if (r < 0) -r else r
    }
  }
}
