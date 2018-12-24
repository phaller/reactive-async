/* BSD 2-Clause License - see OPAL/LICENSE for details. */
package com.phaller.rasync.test.opal.ifds

import java.net.URL

import com.phaller.rasync.pool._
import org.opalj.collection.immutable.RefArray
import org.opalj.br.DeclaredMethod
import org.opalj.br.ObjectType
import org.opalj.br.Method
import org.opalj.br.analyses.SomeProject
import org.opalj.br.analyses.Project
import org.opalj.fpcf.{ PropertyKey, PropertyStore, PropertyStoreContext, PropertyStoreKey }
import org.opalj.fpcf.analyses.AbstractIFDSAnalysis.V
import org.opalj.fpcf.analyses.Statement
import org.opalj.fpcf.properties.{ IFDSProperty, IFDSPropertyMetaInformation }
import org.opalj.fpcf.seq.PKESequentialPropertyStore
import org.opalj.bytecode.JRELibraryFolder
import org.opalj.log.LogContext
import org.opalj.tac._
import org.opalj.util.{ Nanoseconds, PerformanceEvaluation }
import org.scalatest.FunSuite

import scala.collection.immutable.ListSet
import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait Fact

case class Variable(index: Int) extends Fact
case class ArrayElement(index: Int, element: Int) extends Fact
case class InstanceField(index: Int, classType: ObjectType, fieldName: String) extends Fact
case class FlowFact(flow: ListSet[Method]) extends Fact {
  override val hashCode: Int = { flow.foldLeft(1)(_ + _.hashCode() * 31) }
}

/**
 * A simple IFDS taint analysis.
 *
 * @author Dominik Helm
 */
class TestTaintAnalysis(
  parallelism: Int = Runtime.getRuntime.availableProcessors(),
  scheduling: SchedulingStrategy = DefaultScheduling)(
  implicit
  val project: SomeProject) extends AbstractIFDSAnalysis[Fact](parallelism, scheduling) {

  override val property: IFDSPropertyMetaInformation[Fact] = Taint

  override def waitForCompletion(duration: Duration = Duration("10h")): Unit = {
    val fut = pool.quiescentResolveCell
    Await.ready(fut, duration)
  }

  //    Methods below have not been changed when migrating the code to RA

  override def createProperty(result: Map[Statement, Set[Fact]]): IFDSProperty[Fact] = {
    new Taint(result)
  }

  override def normalFlow(stmt: Statement, succ: Statement, in: Set[Fact]): Set[Fact] =
    stmt.stmt.astID match {
      case Assignment.ASTID ⇒
        handleAssignment(stmt, stmt.stmt.asAssignment.expr, in)
      case ArrayStore.ASTID ⇒
        val store = stmt.stmt.asArrayStore
        val definedBy = store.arrayRef.asVar.definedBy
        val index = getConstValue(store.index, stmt.code)
        if (isTainted(store.value, in))
          if (index.isDefined) // Taint known array index
            in ++ definedBy.iterator.map(ArrayElement(_, index.get))
          else // Taint whole array if index is unknown
            in ++ definedBy.iterator.map(Variable)
        else if (index.isDefined && definedBy.size == 1) // Untaint if possible
          in - ArrayElement(definedBy.head, index.get)
        else in
      case _ ⇒ in
    }

  /**
   * Returns true if the expression contains a taint.
   */
  def isTainted(expr: Expr[V], in: Set[Fact]): Boolean = {
    expr.isVar && in.exists {
      case Variable(index) ⇒ expr.asVar.definedBy.contains(index)
      case ArrayElement(index, _) ⇒ expr.asVar.definedBy.contains(index)
      case InstanceField(index, _, _) ⇒ expr.asVar.definedBy.contains(index)
      case _ ⇒ false
    }
  }

  /**
   * Returns the constant int value of an expression if it exists, None otherwise.
   */
  def getConstValue(expr: Expr[V], code: Array[Stmt[V]]): Option[Int] = {
    if (expr.isIntConst) Some(expr.asIntConst.value)
    else if (expr.isVar) {
      val constVals = expr.asVar.definedBy.iterator.map[Option[Int]] { idx ⇒
        if (idx >= 0) {
          val stmt = code(idx)
          if (stmt.astID == Assignment.ASTID && stmt.asAssignment.expr.isIntConst)
            Some(stmt.asAssignment.expr.asIntConst.value)
          else
            None
        } else None
      }.toIterable
      if (constVals.forall(option ⇒ option.isDefined && option.get == constVals.head.get))
        constVals.head
      else None
    } else None
  }

  def handleAssignment(stmt: Statement, expr: Expr[V], in: Set[Fact]): Set[Fact] =
    expr.astID match {
      case Var.ASTID ⇒
        val newTaint = in.collect {
          case Variable(index) if expr.asVar.definedBy.contains(index) ⇒
            Some(Variable(stmt.index))
          case ArrayElement(index, taintIndex) if expr.asVar.definedBy.contains(index) ⇒
            Some(ArrayElement(stmt.index, taintIndex))
          case _ ⇒ None
        }.flatten
        in ++ newTaint
      case ArrayLoad.ASTID ⇒
        val load = expr.asArrayLoad
        if (in.exists {
          // The specific array element may be tainted
          case ArrayElement(index, taintedIndex) ⇒
            val element = getConstValue(load.index, stmt.code)
            load.arrayRef.asVar.definedBy.contains(index) &&
              (element.isEmpty || taintedIndex == element.get)
          // Or the whole array
          case Variable(index) ⇒ load.arrayRef.asVar.definedBy.contains(index)
          case _ ⇒ false
        })
          in + Variable(stmt.index)
        else
          in
      case _ ⇒ in
    }

  override def callFlow(
    stmt: Statement,
    callee: DeclaredMethod,
    in: Set[Fact]): Set[Fact] = {
    val allParams = asCall(stmt.stmt).receiverOption ++ asCall(stmt.stmt).params
    if (callee.name == "sink") {
      if (in.exists {
        case Variable(index) ⇒
          allParams.exists(p ⇒ p.asVar.definedBy.contains(index))
        case _ ⇒ false
      })
        println(s"Found flow: $stmt")
      Set.empty
    } else if (callee.name == "forName" && (callee.declaringClassType eq ObjectType.Class) &&
      callee.descriptor.parameterTypes == RefArray(ObjectType.String)) {
      if (in.exists {
        case Variable(index) ⇒
          asCall(stmt.stmt).params.exists(p ⇒ p.asVar.definedBy.contains(index))
        case _ ⇒ false
      })
        println(s"Found flow: $stmt")
      Set.empty
    } else if ((callee.descriptor.returnType eq ObjectType.Class) ||
      (callee.descriptor.returnType eq ObjectType.Object)) {
      in.collect {
        case Variable(index) ⇒ // Taint formal parameter if actual parameter is tainted
          allParams.zipWithIndex.collect {
            case (param, pIndex) if param.asVar.definedBy.contains(index) ⇒
              Variable(paramToIndex(pIndex, !callee.definedMethod.isStatic))
          }

        case ArrayElement(index, taintedIndex) ⇒
          // Taint element of formal parameter if element of actual parameter is tainted
          allParams.zipWithIndex.collect {
            case (param, pIndex) if param.asVar.definedBy.contains(index) ⇒
              ArrayElement(paramToIndex(pIndex, !callee.definedMethod.isStatic), taintedIndex)
          }
      }.flatten
    } else Set.empty
  }

  override def returnFlow(
    stmt: Statement,
    callee: DeclaredMethod,
    exit: Statement,
    succ: Statement,
    in: Set[Fact]): Set[Fact] = {

    /**
     * Checks whether the formal parameter is of a reference type, as primitive types are
     * call-by-value.
     */
    def isRefTypeParam(index: Int): Boolean =
      if (index == -1) true
      else {
        callee.descriptor.parameterType(
          paramToIndex(index, includeThis = false)).isReferenceType
      }

    if (callee.name == "source" && stmt.stmt.astID == Assignment.ASTID)
      Set(Variable(stmt.index))
    else if (callee.name == "sanitize")
      Set.empty
    else {
      val allParams = (asCall(stmt.stmt).receiverOption ++ asCall(stmt.stmt).params).toSeq
      var flows: Set[Fact] = Set.empty
      for (fact ← in) {
        fact match {
          case Variable(index) if index < 0 && index > -100 && isRefTypeParam(index) ⇒
            // Taint actual parameter if formal parameter is tainted
            val param =
              allParams(paramToIndex(index, !callee.definedMethod.isStatic))
            flows ++= param.asVar.definedBy.iterator.map(Variable)

          case ArrayElement(index, taintedIndex) if index < 0 && index > -100 ⇒
            // Taint element of actual parameter if element of formal parameter is tainted
            val param =
              allParams(paramToIndex(index, !callee.definedMethod.isStatic))
            flows ++= param.asVar.definedBy.iterator.map(ArrayElement(_, taintedIndex))

          case InstanceField(index, declClass, taintedField) if index < 0 && index > -10 ⇒
            // Taint field of actual parameter if field of formal parameter is tainted
            val param =
              allParams(paramToIndex(index, !callee.definedMethod.isStatic))
            flows ++= param.asVar.definedBy.iterator.map(InstanceField(_, declClass, taintedField))
          //case sf: StaticField ⇒ flows += sf
          case FlowFact(flow) ⇒
            val newFlow = flow + stmt.method
            if (entryPoints.contains(declaredMethods(exit.method))) {
              //println(s"flow: "+newFlow.map(_.toJava).mkString(", "))
            } else {
              flows += FlowFact(newFlow)
            }
          case _ ⇒
        }
      }

      // Propagate taints of the return value
      if (exit.stmt.astID == ReturnValue.ASTID && stmt.stmt.astID == Assignment.ASTID) {
        val returnValue = exit.stmt.asReturnValue.expr.asVar
        flows ++= in.collect {
          case Variable(index) if returnValue.definedBy.contains(index) ⇒
            Variable(stmt.index)
          case ArrayElement(index, taintedIndex) if returnValue.definedBy.contains(index) ⇒
            ArrayElement(stmt.index, taintedIndex)
          case InstanceField(index, declClass, taintedField) if returnValue.definedBy.contains(index) ⇒
            InstanceField(stmt.index, declClass, taintedField)
        }
      }

      flows
    }
  }

  /**
   * Converts a parameter origin to the index in the parameter seq (and vice-versa).
   */
  def paramToIndex(param: Int, includeThis: Boolean): Int =
    (if (includeThis) -1 else -2) - param

  override def callToReturnFlow(stmt: Statement, succ: Statement, in: Set[Fact]): Set[Fact] = {
    val call = asCall(stmt.stmt)
    if (call.name == "sanitize") {
      in.filter {
        case Variable(index) ⇒
          !(call.params ++ call.receiverOption).exists { p ⇒
            val definedBy = p.asVar.definedBy
            definedBy.size == 1 && definedBy.contains(index)
          }
        case _ ⇒ true
      }
    } else if (call.name == "forName" && (call.declaringClass eq ObjectType.Class) &&
      call.descriptor.parameterTypes == RefArray(ObjectType.String)) {
      if (in.exists {
        case Variable(index) ⇒
          asCall(stmt.stmt).params.exists(p ⇒ p.asVar.definedBy.contains(index))
        case _ ⇒ false
      }) {
        /*if (entryPoints.contains(declaredMethods(stmt.method))) {
            println(s"flow: "+stmt.method.toJava)
            in
        } else*/
        in ++ Set(FlowFact(ListSet(stmt.method)))
      } else {
        in
      }
    } else {
      in
    }
  }

  val entryPoints: Map[DeclaredMethod, Fact] = (for {
    m ← p.allMethodsWithBody
    if (m.isPublic || m.isProtected) && (m.descriptor.returnType == ObjectType.Object || m.descriptor.returnType == ObjectType.Class)
    index ← m.descriptor.parameterTypes.zipWithIndex.collect { case (pType, index) if pType == ObjectType.String ⇒ index }
  } //yield (declaredMethods(m), null)
  yield declaredMethods(m) → Variable(-2 - index)).toMap
}

class Taint(val flows: Map[Statement, Set[Fact]]) extends IFDSProperty[Fact] {

  override type Self = Taint

  def key: PropertyKey[Taint] = Taint.key
}

object Taint extends IFDSPropertyMetaInformation[Fact] {
  override type Self = Taint

  val key: PropertyKey[Taint] = PropertyKey.create(
    "TestTaint",
    new Taint(Map.empty))
}

class TestTaintAnalysisRunner extends FunSuite {

  test("main") {
    main(null)
  }

  def main(args: Array[String]): Unit = {

    val p0 = Project(new java.io.File(JRELibraryFolder.getAbsolutePath))

    for (
      scheduling <- List(DefaultScheduling, OthersWithManySuccessorsFirst, OthersWithManySuccessorsLast, CellsWithManyPredecessorsFirst, CellsWithManyPredecessorsLast, CellsWithManySuccessorsFirst, CellsWithManySuccessorsLast);
      threads <- List(1, 2, 4, 8, 16, 32)
    ) {
      var result = 0
      var lastAvg = 0L
      PerformanceEvaluation.time(2, 4, 3, {

        implicit val p: Project[URL] = p0.recreate()

        // Using PropertySore here is fine, it is not use during analysis
        p.getOrCreateProjectInformationKeyInitializationData(
          PropertyStoreKey,
          (context: List[PropertyStoreContext[AnyRef]]) ⇒ {
            implicit val lg: LogContext = p.logContext
            val ps = PKESequentialPropertyStore(context: _*)
            PropertyStore.updateDebug(false)
            ps
          })

        // From now on, we may access ps for read operations only
        // We can now start TestTaintAnalysis using IFDS.
        val analysis = new TestTaintAnalysis(threads, scheduling)

        val entryPoints = analysis.entryPoints
        entryPoints.foreach(analysis.forceComputation)
        analysis.waitForCompletion()

        result = 0
        for {
          e ← entryPoints
          fact ← analysis.getResult(e).flows.values.flatten.toSet[Fact]
        } {
          fact match {
            case FlowFact(_) ⇒ result += 1
            case _ ⇒
          }
        }
        println(s"NUM RESULTS =  $result")
      }) { (_, ts) ⇒
        val sTs = ts.map(_.toSeconds).mkString(", ")
        val avg = ts.map(_.timeSpan).sum / ts.size
        if (lastAvg != avg) {
          lastAvg = avg
          val avgInSeconds = new Nanoseconds(lastAvg).toSeconds
          println(s"RES: Scheduling = ${scheduling.getClass.getName}, #threads = $threads, avg = $avgInSeconds;Ts: $sTs")
        }
      }
    }
  }
}
