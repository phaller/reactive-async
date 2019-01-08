package com.phaller.rasync

import com.phaller.rasync.lattice.Key

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
  def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int
  def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int

}

/** All tasks are of equal priority. */
object DefaultScheduling extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int = 0
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int = 0
}

object OthersWithManySuccessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    -(other.numNextDependentCells + other.numCompleteDependentCells)

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int =
    -(cell.numNextDependentCells + cell.numCompleteDependentCells)
}

object OthersWithManySuccessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    other.numNextDependentCells + other.numCompleteDependentCells

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int =
    cell.numNextDependentCells + cell.numCompleteDependentCells
}

object CellsWithManyPredecessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    -dependentCell.totalCellDependencies.size

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int = 0
}

object CellsWithManyPredecessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    dependentCell.totalCellDependencies.size

  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int = 0
}

object CellsWithManySuccessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    -(dependentCell.numNextDependentCells + dependentCell.numCompleteDependentCells)

  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int = 0
}

object CellsWithManySuccessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    dependentCell.numNextDependentCells + dependentCell.numCompleteDependentCells

  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int = 0
}
