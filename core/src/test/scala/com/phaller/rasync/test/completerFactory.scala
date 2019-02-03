package com.phaller.rasync.test

import com.phaller.rasync.cell.CellCompleter
import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool.HandlerPool

trait CompleterFactory {
  def mkCompleter[V, E >: Null](implicit updater: Updater[V], pool: HandlerPool[V, E], e: E = null): CellCompleter[V, E]
  def mkCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V, Null]): CellCompleter[V, Null] = mkCompleter[V, Null]
}

trait ConcurrentCompleterFactory extends CompleterFactory {
  override def mkCompleter[V, E >: Null](implicit updater: Updater[V], pool: HandlerPool[V, E], e: E = null): CellCompleter[V, E] = CellCompleter(entity = e)(updater, pool)
}

trait SequentialCompleterFactory extends CompleterFactory {
  override def mkCompleter[V, E >: Null](implicit updater: Updater[V], pool: HandlerPool[V, E], e: E = null): CellCompleter[V, E] = CellCompleter(entity = e, sequential = true)(updater, pool)
}

trait MixedCompleterFactory {
  def mkSeqCompleter[V, E >: Null](implicit updater: Updater[V], pool: HandlerPool[V, E], e: E = null): CellCompleter[V, E] = CellCompleter[V, E](sequential = true, entity = e)(updater, pool)
  def mkConCompleter[V, E >: Null](implicit updater: Updater[V], pool: HandlerPool[V, E], e: E = null): CellCompleter[V, E] = CellCompleter[V, E](entity = e)(updater, pool)

  def mkSeqCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V, Null]): CellCompleter[V, Null] = mkSeqCompleter[V, Null]
  def mkConCompleter[V](implicit updater: Updater[V], pool: HandlerPool[V, Null]): CellCompleter[V, Null] = mkConCompleter[V, Null]
}