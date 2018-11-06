package com.phaller.rasync.test

import com.phaller.rasync.cell.CellCompleter
import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool.HandlerPool

trait CompleterFactory {
  def mkCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V]): CellCompleter[V]
}

trait ConcurrentCompleterFactory extends CompleterFactory {
  override def mkCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V]): CellCompleter[V] = CellCompleter()(updater, pool)
}

trait SequentialCompleterFactory extends CompleterFactory {
  override def mkCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V]): CellCompleter[V] = CellCompleter(sequential = true)(updater, pool)
}

trait MixedCompleterFactory {
  def mkSeqCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V]): CellCompleter[V] = CellCompleter(sequential = true)(updater, pool)
  def mkConCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V]): CellCompleter[V] = CellCompleter()(updater, pool)
}