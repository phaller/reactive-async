package com.phaller.rasync.pool

import com.phaller.rasync.cell.Cell
import com.phaller.rasync.cell.ValueOutcome

import scala.util.Try

/**
 * A scheduling strategy defines priorities for dependency callbacks
 * and cell resolution.
 *
 * Whenever a dependency is triggered due to a change of a dependee cell,
 * a task is added to the handler pool. Those tasks will eventually be picked
 * up and executed, potentielly concurrently to other tasks.
 * Each task added to the pool is assigned a priority as defined by a
 * SchedulingStrategy, where tasks with lower priorities are more likely
 * to be executed earlier. (Note that due to concurrent execution there is
 * no guarantee for any order of execution. Use sequential callbacks to
 * avoid concurrent execution of certain tasks.)
 *
 * If a tasks has been created because of a change in a dependee cell `other` being propgated to
 * a `dependentCell`, `SchedulingStrategy.calcPriority(dependentCell, other)` is called to
 * obtain a priority.
 * If a tasks has been created to complete a `cell` via a `Key`,
 * `SchedulingStrategy.calcPriority(cell)` is called.
 */
trait SchedulingStrategy {
  def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int
  def calcPriority[V](dependentCell: Cell[V], value: Try[V]): Int
  override def toString: String = this.getClass.getSimpleName
}

/** All tasks are of equal priority. */
object DefaultScheduling extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int = 0
  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int = 0
}

object SourcesWithManyTargetsFirst extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    -other.numDependentCells

  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int =
    -cell.numDependentCells
}

object SourcesWithManyTargetsLast extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    other.numDependentCells

  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int =
    cell.numDependentCells
}

object SourcesWithManySourcesFirst extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    -other.numDependencies

  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int =
    -cell.numDependencies
}

object SourcesWithManySourcesLast extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    other.numDependencies

  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int =
    cell.numDependencies
}

object TargetsWithManySourcesFirst extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    -dependentCell.numDependencies

  override def calcPriority[V](cell: Cell[V], value: Try[V]): Int = 0
}

object TargetsWithManySourcesLast extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    dependentCell.numDependencies

  override def calcPriority[V](dependentCell: Cell[V], value: Try[V]): Int = 0
}

object TargetsWithManyTargetsFirst extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    -dependentCell.numDependencies

  override def calcPriority[V](dependentCell: Cell[V], value: Try[V]): Int = 0
}

object TargetsWithManyTargetsLast extends SchedulingStrategy {
  override def calcPriority[V](dependentCell: Cell[V], other: Cell[V], value: Try[ValueOutcome[V]]): Int =
    dependentCell.numDependentCells

  override def calcPriority[V](dependentCell: Cell[V], value: Try[V]): Int = 0
}
