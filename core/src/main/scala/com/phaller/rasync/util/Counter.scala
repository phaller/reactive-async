package com.phaller.rasync.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.concurrent.TrieMap

object Counter {

  private val profilingCounter = TrieMap.empty[String, AtomicLong]

  def inc(key: String, n: Int = 1): Long = {
    profilingCounter.getOrElseUpdate(key, new AtomicLong(0)).addAndGet(n.toLong)
  }

  def dec(key: String, n: Int = 1): Long = {
    profilingCounter.getOrElseUpdate(key, new AtomicLong(0)).addAndGet(-n.toLong)
  }

  def get(key: String): Long = {
    profilingCounter.getOrElse(key, new AtomicLong(0)).get()
  }

  def reset(): Unit = {
    profilingCounter.clear()
  }

  override def toString: String = {
    val counters = s"\tCounters\n" +
      f"\t\t${"Number"}%10s  ${"Name"}%-90s\n" +
      f"\t\t${"------"}%10s  ${"----"}%-90s\n" +
      profilingCounter.toSeq.sortBy(x ⇒ x._1).map { x ⇒
        f"\t\t${x._2.get}%10s  ${x._1}%-90s\n"
      }.mkString

    counters
  }
}

class Statstics[T] {

  private val data: ConcurrentLinkedQueue[T] = new ConcurrentLinkedQueue[T]()

  def add(x: T) = data.add(x)

  def reset(): Unit = {
    data.clear()
  }

  def toArray(): Array[T] = data.toArray.asInstanceOf[Array[T]]

  override def toString: String = {
    data.toArray.asInstanceOf[Array[T]].mkString("\n")
  }
}

object InOutStats extends Statstics[(Int, Int)]
