/* BSD 2-Clause License - see OPAL/LICENSE for details. */
package com.phaller.rasync.test.opal.ifds

import java.io.File
import java.net.URL

import com.phaller.rasync.pool._

import org.opalj.collection.immutable.RefArray
import org.opalj.br.DeclaredMethod
import org.opalj.br.ObjectType
import org.opalj.br.Method
import org.opalj.br.analyses.SomeProject
import org.opalj.br.analyses.Project
import org.opalj.fpcf.{ PropertyKey, PropertyStore, PropertyStoreContext }
import org.opalj.fpcf.seq.PKESequentialPropertyStore
import org.opalj.bytecode.JRELibraryFolder
import org.opalj.log.LogContext
import org.opalj.tac._
import org.opalj.util.{ Nanoseconds, PerformanceEvaluation }
import org.scalatest.FunSuite
import scala.collection.immutable.ListSet
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.phaller.rasync.test.opal.ifds.AbstractIFDSAnalysis.Statement
import com.phaller.rasync.test.opal.ifds.AbstractIFDSAnalysis.V
import com.phaller.rasync.util.Counter
import org.opalj.bytecode

import org.opalj.br.fpcf.PropertyStoreKey
import org.opalj.br.fpcf.FPCFAnalysesManagerKey
import org.opalj.ai.domain.l0.PrimitiveTACAIDomain
import org.opalj.tac.fpcf.analyses.cg.CallGraphDeserializerScheduler

trait Fact extends AbstractIFDSFact

case class Variable(index: Int) extends Fact
//case class ArrayElement(index: Int, element: Int) extends Fact
case class StaticField(classType: ObjectType, fieldName: String) extends Fact
case class InstanceField(index: Int, classType: ObjectType, fieldName: String) extends Fact
case class FlowFact(flow: ListSet[Method]) extends Fact {
  override val hashCode: Int = {
    // HERE, a foldLeft introduces a lot of overhead due to (un)boxing.
    var r = 1
    flow.foreach(f ⇒ r = (r + f.hashCode()) * 31)
    r
  }
}

case object NullFact extends Fact with AbstractIFDSNullFact

/**
 * A simple IFDS taint analysis.
 *
 * @author Dominik Helm
 */
class TestTaintAnalysis(
  parallelism: Int = Runtime.getRuntime.availableProcessors(),
  scheduling: SchedulingStrategy[IFDSProperty[Fact], (DeclaredMethod, Fact)])(
  implicit
  val project: SomeProject) extends AbstractIFDSAnalysis[Fact](parallelism, scheduling)(project) {

  override val property: IFDSPropertyMetaInformation[Fact] = Taint

  override def waitForCompletion(duration: Duration = Duration("10h")): Unit = {
    val fut = pool.quiescentResolveCell
    Await.ready(fut, duration)
    pool.shutdown()
  }

  //    Methods below have not been changed when migrating the code to RA

  override def createProperty(result: Map[Statement, Set[Fact]]): IFDSProperty[Fact] = {
    new Taint(result)
  }

  override def normalFlow(stmt: Statement, succ: Statement, in: Set[Fact]): Set[Fact] =
    stmt.stmt.astID match {
      case Assignment.ASTID ⇒
        handleAssignment(stmt, stmt.stmt.asAssignment.expr, in)
      /*case ArrayStore.ASTID ⇒
                val store = stmt.stmt.asArrayStore
                val definedBy = store.arrayRef.asVar.definedBy
                val index = getConstValue(store.index, stmt.code)
                if (isTainted(store.value, in))
                    if (index.isDefined) // Taint known array index
                        // Instead of using an iterator, we are going to use internal iteration
                        // in ++ definedBy.iterator.map(ArrayElement(_, index.get))
                        definedBy.foldLeft(in) { (c, n) ⇒ c + ArrayElement(n, index.get) }
                    else // Taint whole array if index is unknown
                        // Instead of using an iterator, we are going to use internal iteration:
                        // in ++ definedBy.iterator.map(Variable)
                        definedBy.foldLeft(in) { (c, n) ⇒ c + Variable(n) }
                else in*/
      case PutStatic.ASTID ⇒
        val put = stmt.stmt.asPutStatic
        if (isTainted(put.value, in)) in + StaticField(put.declaringClass, put.name)
        else in
      /*case PutField.ASTID ⇒
                val put = stmt.stmt.asPutField
                if (isTainted(put.value, in)) in + StaticField(put.declaringClass, put.name)
                else in*/
      case PutField.ASTID ⇒
        val put = stmt.stmt.asPutField
        val definedBy = put.objRef.asVar.definedBy
        if (isTainted(put.value, in))
          definedBy.foldLeft(in) { (in, defSite) ⇒
            in + InstanceField(defSite, put.declaringClass, put.name)
          }
        else in
      case _ ⇒ in
    }

  /**
   * Returns true if the expression contains a taint.
   */
  def isTainted(expr: Expr[V], in: Set[Fact]): Boolean = {
    expr.isVar && in.exists {
      case Variable(index) ⇒ expr.asVar.definedBy.contains(index)
      //case ArrayElement(index, _)     ⇒ expr.asVar.definedBy.contains(index)
      case InstanceField(index, _, _) ⇒ expr.asVar.definedBy.contains(index)
      case _ ⇒ false
    }
  }

  /**
   * Returns the constant int value of an expression if it exists, None otherwise.
   */
  /*def getConstValue(expr: Expr[V], code: Array[Stmt[V]]): Option[Int] = {
        if (expr.isIntConst) Some(expr.asIntConst.value)
        else if (expr.isVar) {
            // TODO The following looks optimizable!
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
    }*/

  def handleAssignment(stmt: Statement, expr: Expr[V], in: Set[Fact]): Set[Fact] =
    expr.astID match {
      case Var.ASTID ⇒
        val newTaint = in.collect {
          case Variable(index) if expr.asVar.definedBy.contains(index) ⇒
            Some(Variable(stmt.index))
          /*case ArrayElement(index, taintIndex) if expr.asVar.definedBy.contains(index) ⇒
                        Some(ArrayElement(stmt.index, taintIndex))*/
          case _ ⇒ None
        }.flatten
        in ++ newTaint
      /*case ArrayLoad.ASTID ⇒
                val load = expr.asArrayLoad
                if (in.exists {
                    // The specific array element may be tainted
                    case ArrayElement(index, taintedIndex) ⇒
                        val element = getConstValue(load.index, stmt.code)
                        load.arrayRef.asVar.definedBy.contains(index) &&
                            (element.isEmpty || taintedIndex == element.get)
                    // Or the whole array
                    case Variable(index) ⇒ load.arrayRef.asVar.definedBy.contains(index)
                    case _               ⇒ false
                })
                    in + Variable(stmt.index)
                else
                    in*/
      case GetStatic.ASTID ⇒
        val get = expr.asGetStatic
        if (in.contains(StaticField(get.declaringClass, get.name)))
          in + Variable(stmt.index)
        else in
      /*case GetField.ASTID ⇒
                val get = expr.asGetField
                if (in.contains(StaticField(get.declaringClass, get.name)))
                    in + Variable(stmt.index)
                else in*/
      case GetField.ASTID ⇒
        val get = expr.asGetField
        if (in.exists {
          // The specific field may be tainted
          case InstanceField(index, _, taintedField) ⇒
            taintedField == get.name && get.objRef.asVar.definedBy.contains(index)
          // Or the whole object
          case Variable(index) ⇒ get.objRef.asVar.definedBy.contains(index)
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
    val allParams = asCall(stmt.stmt).allParams
    if (callee.name == "sink")
      if (in.exists {
        case Variable(index) ⇒
          allParams.exists(p ⇒ p.asVar.definedBy.contains(index))
        case _ ⇒ false
      }) {
        println(s"Found flow: $stmt")
      }
    if (callee.name == "forName" && (callee.declaringClassType eq ObjectType.Class) &&
      callee.descriptor.parameterTypes == RefArray(ObjectType.String))
      if (in.exists {
        case Variable(index) ⇒
          asCall(stmt.stmt).params.exists(p ⇒ p.asVar.definedBy.contains(index))
        case _ ⇒ false
      }) {
        println(s"Found flow: $stmt")
      }
    if (true || (callee.descriptor.returnType eq ObjectType.Class) ||
      (callee.descriptor.returnType eq ObjectType.Object) ||
      (callee.descriptor.returnType eq ObjectType.String)) {
      var facts = Set.empty[Fact]
      in.foreach {
        case Variable(index) ⇒ // Taint formal parameter if actual parameter is tainted
          allParams.iterator.zipWithIndex.foreach {
            case (param, pIndex) if param.asVar.definedBy.contains(index) ⇒
              facts += Variable(paramToIndex(pIndex, !callee.definedMethod.isStatic))
            case _ ⇒ // Nothing to do
          }

        /*case ArrayElement(index, taintedIndex) ⇒
                    // Taint element of formal parameter if element of actual parameter is tainted
                    allParams.zipWithIndex.collect {
                        case (param, pIndex) if param.asVar.definedBy.contains(index) ⇒
                            ArrayElement(paramToIndex(pIndex, !callee.definedMethod.isStatic), taintedIndex)
                    }*/

        case InstanceField(index, declClass, taintedField) ⇒
          // Taint field of formal parameter if field of actual parameter is tainted
          // Only if the formal parameter is of a type that may have that field!
          allParams.iterator.zipWithIndex.foreach {
            case (param, pIndex) if param.asVar.definedBy.contains(index) &&
              (paramToIndex(pIndex, !callee.definedMethod.isStatic) != -1 ||
                classHierarchy.isSubtypeOf(declClass, callee.declaringClassType)) ⇒
              facts += InstanceField(paramToIndex(pIndex, !callee.definedMethod.isStatic), declClass, taintedField)
            case _ ⇒ // Nothing to do
          }
        case sf: StaticField ⇒
          facts += sf
      }
      facts
    } else Set.empty
  }

  override def returnFlow(
    stmt: Statement,
    callee: DeclaredMethod,
    exit: Statement,
    succ: Statement,
    in: Set[Fact]): Set[Fact] = {
    if (callee.name == "source" && stmt.stmt.astID == Assignment.ASTID)
      Set(Variable(stmt.index))
    else if (callee.name == "sanitize")
      Set.empty
    else {
      val call = asCall(stmt.stmt)
      val allParams = call.allParams
      var flows: Set[Fact] = Set.empty
      in.foreach {
        /*case ArrayElement(index, taintedIndex) if index < 0 && index > -100 ⇒
                        // Taint element of actual parameter if element of formal parameter is tainted
                        val param =
                            allParams(paramToIndex(index, !callee.definedMethod.isStatic))
                        flows ++= param.asVar.definedBy.iterator.map(ArrayElement(_, taintedIndex))*/

        case InstanceField(index, declClass, taintedField) if index < 0 && index > -255 ⇒
          // Taint field of actual parameter if field of formal parameter is tainted
          val param = allParams(paramToIndex(index, !callee.definedMethod.isStatic))
          param.asVar.definedBy.foreach { defSite ⇒
            flows += InstanceField(defSite, declClass, taintedField)
          }

        case sf: StaticField ⇒
          flows += sf

        case FlowFact(flow) ⇒
          val newFlow = flow + stmt.method
          if (entryPoints.contains(declaredMethods(exit.method))) {
            //println(s"flow: "+newFlow.map(_.toJava).mkString(", "))
          } else {
            flows += FlowFact(newFlow)
          }

        case _ ⇒
      }

      // Propagate taints of the return value
      if (exit.stmt.astID == ReturnValue.ASTID && stmt.stmt.astID == Assignment.ASTID) {
        val returnValue = exit.stmt.asReturnValue.expr.asVar
        in.foreach {
          case Variable(index) if returnValue.definedBy.contains(index) ⇒
            flows += Variable(stmt.index)
          /*case ArrayElement(index, taintedIndex) if returnValue.definedBy.contains(index) ⇒
                        ArrayElement(stmt.index, taintedIndex)*/
          case InstanceField(index, declClass, taintedField) if returnValue.definedBy.contains(index) ⇒
            flows += InstanceField(stmt.index, declClass, taintedField)

          case _ ⇒ // nothing to do
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

  /**
   * If forName is called, we add a FlowFact.
   */
  override def nativeCall(statement: Statement, callee: DeclaredMethod, successor: Statement, in: Set[Fact]): Set[Fact] = {
    /* val allParams = asCall(statement.stmt).allParams
        if (statement.stmt.astID == Assignment.ASTID && in.exists {
            case Variable(index) ⇒
                allParams.zipWithIndex.exists {
                    case (param, _) if param.asVar.definedBy.contains(index) ⇒ true
                    case _                                                   ⇒ false
                }
            /*case ArrayElement(index, _) ⇒
                allParams.zipWithIndex.exists {
                    case (param, _) if param.asVar.definedBy.contains(index) ⇒ true
                    case _                                                   ⇒ false
                }*/
            case _ ⇒ false
        }) Set(Variable(statement.index))
        else*/ Set.empty
  }

  val entryPoints: Map[DeclaredMethod, Fact] = (for {
    m ← project.allMethodsWithBody
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

object TestTaintAnalysisRunner extends FunSuite {

  def main(args: Array[String]): Unit = {

    val p0 = Project(new File(args(args.length - 1))) //bytecode.RTJar)

    com.phaller.rasync.pool.SchedulingStrategy

    // Using PropertySore here is fine, it is not use during analysis
    p0.getOrCreateProjectInformationKeyInitializationData(
      PropertyStoreKey,
      (context: List[PropertyStoreContext[AnyRef]]) ⇒ {
        implicit val lg: LogContext = p0.logContext
        val ps = PKESequentialPropertyStore(context: _*)
        PropertyStore.updateDebug(false)
        ps
      })

    p0.getOrCreateProjectInformationKeyInitializationData(
      LazyDetachedTACAIKey,
      (m: Method) ⇒ new PrimitiveTACAIDomain(p0, m))

    PerformanceEvaluation.time {
      val manager = p0.get(FPCFAnalysesManagerKey)
      manager.runAll(new CallGraphDeserializerScheduler(new File(args(args.length - 2))))
    } { t ⇒ println(s"CG took ${t.toSeconds}") }

    for (
      scheduling ← List(
        new DefaultScheduling[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new SourcesWithManyTargetsFirst[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new SourcesWithManyTargetsLast[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new TargetsWithManySourcesFirst[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new TargetsWithManySourcesLast[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new TargetsWithManyTargetsFirst[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new TargetsWithManyTargetsLast[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new SourcesWithManySourcesFirst[IFDSProperty[Fact], (DeclaredMethod, Fact)],
        new SourcesWithManySourcesLast[IFDSProperty[Fact], (DeclaredMethod, Fact)]);
      threads ← List(1, 2, 4, 8, 10, 16, 20, 32, 40)
    ) {
      var result = 0
      var analysis: TestTaintAnalysis = null
      var entryPoints: Map[DeclaredMethod, Fact] = null
      var ts: List[Long] = List.empty
      for (i ← (0 until 5)) {
        PerformanceEvaluation.time({
          implicit val p: Project[URL] = p0 //.recreate(k ⇒ k == PropertyStoreKey.uniqueId || k == DeclaredMethodsKey.uniqueId)
          Counter.reset()

          // From now on, we may access ps for read operations only
          // We can now start TestTaintAnalysis using IFDS.
          analysis = new TestTaintAnalysis(threads, scheduling)

          entryPoints = analysis.entryPoints
          entryPoints.foreach(analysis.forceComputation)
          analysis.waitForCompletion()
        }) { t ⇒

          result = 0
          for {
            e ← entryPoints
            fact ← analysis.getResult(e).flows.values.flatten.toSet[Fact]
          } {
            fact match {
              case FlowFact(flow) ⇒
                result += 1; println(s"flow: " + flow.map(_.toJava).mkString(", "))
              case _ ⇒
            }
          }
          println(Counter.toString)
          println(s"NUM RESULTS =  $result")
          println(s"time = ${t.toSeconds}")

          ts ::= t.timeSpan
        }
      }
    }
      val lastAvg = ts.sum / ts.size
      println(s"AVG,${scheduling.getClass.getSimpleName},$threads,$lastAvg")
    }
  }
}
