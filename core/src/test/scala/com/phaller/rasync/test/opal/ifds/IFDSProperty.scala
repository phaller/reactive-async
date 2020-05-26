package com.phaller.rasync.test.opal.ifds

import org.opalj.fpcf.Property
import org.opalj.fpcf.PropertyMetaInformation
import org.opalj.value.KnownTypedValue
import org.opalj.tac.DUVar

trait IFDSPropertyMetaInformation[DataFlowFact] extends PropertyMetaInformation

abstract class IFDSProperty[DataFlowFact] extends Property
  with IFDSPropertyMetaInformation[DataFlowFact] {

  /** The type of the TAC domain. */
  type V = DUVar[KnownTypedValue]

  def flows: Map[AbstractIFDSAnalysis.Statement, Set[DataFlowFact]]

  override def equals(that: Any): Boolean = that match {
    case other: IFDSProperty[DataFlowFact] ⇒ flows == other.flows
    case _ ⇒ false
  }

  override def hashCode(): Int = flows.hashCode()
}