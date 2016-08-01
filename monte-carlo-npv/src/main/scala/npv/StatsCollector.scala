package npv

class StatsCollector(min: Double, max: Double, numBuckets: Int) {

  var instances: Int = 1
  private val range: Double = max - min
  private var mean: Double = 0
  private var numObs: Int = 0
  val buckets: Array[Int] = Array.ofDim[Int](numBuckets)

  def addObs(obs: Double): Unit = {
    mean = (obs + (numObs * mean)) / (numObs+1)
    numObs += 1
    val bucket: Int = Math.floor(numBuckets * (obs - min)/range).asInstanceOf[Int]
    buckets(bucket) = buckets(bucket) + 1
  }

  def combine(collector: StatsCollector): Unit = {
    instances += collector.instances
    mean = ((numObs * mean) + (collector.numObs * collector.mean)) / numObs + collector.numObs
    numObs += collector.numObs
    for (i <- 0 until numBuckets) {
      buckets(i) = buckets(i) + collector.buckets(i)
    }
  }

  override def toString(): String = {
    val sb = new StringBuilder()
    sb.append("Collected Statistics")
    sb.append(System.lineSeparator())
    sb.append("--------------------")
    sb.append(System.lineSeparator())
    sb.append(System.lineSeparator())
    sb.append(f"Number of instances: $instances%d")
    sb.append(System.lineSeparator())
    sb.append(f"Mean: $mean%2f")
    sb.append(System.lineSeparator())
    sb.append(f"Number of observations: $numObs%d")
    sb.append(System.lineSeparator())
    sb.append("Histogram")
    sb.append(System.lineSeparator())
    for (i <- 0 until numBuckets) {
      sb.append(f" ${i+1}%3d    ${buckets(i)}%d")
      sb.append(System.lineSeparator())
    }
    sb.append(System.lineSeparator())
    sb.toString()
  }

}
