package com.phaller.rasync

import java.util.concurrent.atomic.AtomicLong

import org.opalj.util.Nanoseconds

import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.concurrent.TrieMap

/**
 * Trait that adds timing functionality to a class.
 *
 * @author Andreas Muttscheller
 */
trait TimingUtils {
  val debugCounter = TrieMap.empty[String, AtomicLong]
  val debugTimings = TrieMap.empty[String, AtomicLong]

  @elidable(FINE)
  def startTiming(key: String): Unit = {
    debugTimings.getOrElseUpdate(key, new AtomicLong()).addAndGet(-System.nanoTime)
  }

  @elidable(FINE)
  def endTiming(key: String): Unit = {
    debugTimings(key).addAndGet(System.nanoTime)
  }

  /**
   * Time the execution of `f`
   *
   * @note No elidable annotation here, otherwise the function passed won't be executed!
   *
   * @param key They key to save the time to
   * @param f The function to be executed
   */
  def time(key: String)(f: () => Unit): Unit = {
    startTiming(key)
    f()
    endTiming(key)
  }

  @elidable(FINE)
  def incCounter(key: String, n: Int = 1): Long = {
    debugCounter.getOrElseUpdate(key, new AtomicLong(0)).addAndGet(n.toLong)
  }

  /**
   * Sets the counter for `key` to `n` if `n` is greater than the previous value.
   * @param key The key of the counter
   * @param n Value to be set if greater than previous
   * @return The current value
   */
  @elidable(FINE)
  def maxCounter(key: String, n: Long): Long = {
    debugCounter.getOrElseUpdate(key, new AtomicLong(0)).accumulateAndGet(n, (l, r) => Math.max(l, r))
  }

  @elidable(FINE)
  def timingsToString(title: String = "Timings"): String = {
    val counters = s"\tCounters\n" +
      f"\t\t${"Number"}%10s  ${"Name"}%-90s\n" +
      f"\t\t${"------"}%10s  ${"----"}%-90s\n" +
      debugCounter.toSeq.sortBy(x ⇒ x._1).map { x ⇒
        f"\t\t${x._2.get}%10s  ${x._1}%-90s\n"
      }.mkString

    val total = Nanoseconds(debugTimings.filter(x ⇒ !x._1.contains(".")).values.map(_.get).sum)
    val timings = s"\tTimings\n" +
      f"\t\t${"Function"}%-70s ${"nanos"}%30s ${"seconds"}%15s\n" +
      f"\t\t${"--------"}%-70s ${"-----"}%30s ${"-------"}%15s\n" +
      debugTimings.toSeq.sortBy(x ⇒ x._1).map { x ⇒
        val nanos = Nanoseconds(x._2.get)
        f"\t\t${x._1}%-70s ${nanos.toString}%30s ${nanos.toSeconds.toString}%15s\n"
      }.mkString + "\n" +
      f"\t\t${"Total"}%-70s ${total.toString}%30s ${total.toSeconds.toString}%15s\n" +
      "\nNote: Timings are NOT real time, but CPU time!!! So 2" +
      " seconds CPU time can be 0.5 seconds in real time if 4 cores spent 0.5s on a " +
      "function each!!!\n"

    s"$title(\n" +
      counters +
      "\n" +
      timings +
      "\n" +
      ")"
  }
}