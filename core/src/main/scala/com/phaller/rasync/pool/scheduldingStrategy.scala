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
trait SchedulingStrategy[V, E >: Null] {
  def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int
  def calcPriority(dependentCell: Cell[V, E], value: Try[V]): Int
  override def toString: String = this.getClass.getSimpleName
}

object SchedulingStrategy {
  def apply[V, E >: Null](s: SchedulingStrategy.type): SchedulingStrategy[V, E] = {
    s.getClass.asInstanceOf[Class[SchedulingStrategy[V, E]]].newInstance()
  }
}

/** All tasks are of equal priority. */
class DefaultScheduling[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int = 0
  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int = 0
}

class SourcesWithManyTargetsFirst[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    -other.numDependentCells

  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int =
    -cell.numDependentCells
}

class SourcesWithManyTargetsLast[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    other.numDependentCells

  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int =
    cell.numDependentCells
}

class SourcesWithManySourcesFirst[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    -other.numDependencies

  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int =
    -cell.numDependencies
}

class SourcesWithManySourcesLast[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    other.numDependencies

  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int =
    cell.numDependencies
}

class TargetsWithManySourcesFirst[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    -dependentCell.numDependencies

  override def calcPriority(cell: Cell[V, E], value: Try[V]): Int = 0
}

class TargetsWithManySourcesLast[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    dependentCell.numDependencies

  override def calcPriority(dependentCell: Cell[V, E], value: Try[V]): Int = 0
}

class TargetsWithManyTargetsFirst[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    -dependentCell.numDependencies

  override def calcPriority(dependentCell: Cell[V, E], value: Try[V]): Int = 0
}

class TargetsWithManyTargetsLast[V, E >: Null] extends SchedulingStrategy[V, E] {
  override def calcPriority(dependentCell: Cell[V, E], other: Cell[V, E], value: Try[ValueOutcome[V]]): Int =
    dependentCell.numDependentCells

  override def calcPriority(dependentCell: Cell[V, E], value: Try[V]): Int = 0
}
