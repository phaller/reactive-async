package com.phaller.rasync
package test
package opal

import java.net.URL

import com.phaller.rasync.cell._
import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool.{ HandlerPool, SchedulingStrategy }
import scala.concurrent.Await
import scala.concurrent.duration._

import org.opalj.Success

import org.opalj.br.{ ClassFile, Method }
import org.opalj.br.analyses.{ BasicReport, Project, ProjectAnalysisApplication }
import org.opalj.br.instructions.GETFIELD
import org.opalj.br.instructions.GETSTATIC
import org.opalj.br.instructions.PUTFIELD
import org.opalj.br.instructions.PUTSTATIC
import org.opalj.br.instructions.MONITORENTER
import org.opalj.br.instructions.MONITOREXIT
import org.opalj.br.instructions.NEW
import org.opalj.br.instructions.NEWARRAY
import org.opalj.br.instructions.MULTIANEWARRAY
import org.opalj.br.instructions.ANEWARRAY
import org.opalj.br.instructions.AALOAD
import org.opalj.br.instructions.AASTORE
import org.opalj.br.instructions.ARRAYLENGTH
import org.opalj.br.instructions.LALOAD
import org.opalj.br.instructions.IALOAD
import org.opalj.br.instructions.CALOAD
import org.opalj.br.instructions.BALOAD
import org.opalj.br.instructions.BASTORE
import org.opalj.br.instructions.CASTORE
import org.opalj.br.instructions.IASTORE
import org.opalj.br.instructions.LASTORE
import org.opalj.br.instructions.SASTORE
import org.opalj.br.instructions.SALOAD
import org.opalj.br.instructions.DALOAD
import org.opalj.br.instructions.FALOAD
import org.opalj.br.instructions.FASTORE
import org.opalj.br.instructions.DASTORE
import org.opalj.br.instructions.INVOKEDYNAMIC
import org.opalj.br.instructions.INVOKESTATIC
import org.opalj.br.instructions.INVOKESPECIAL
import org.opalj.br.instructions.INVOKEVIRTUAL
import org.opalj.br.instructions.INVOKEINTERFACE
import org.opalj.br.instructions.MethodInvocationInstruction
import org.opalj.br.instructions.NonVirtualMethodInvocationInstruction
import org.opalj.bytecode.JRELibraryFolder
import scala.util.Try

import com.phaller.rasync.pool.DefaultScheduling
import com.phaller.rasync.pool.SourcesWithManySourcesFirst
import com.phaller.rasync.pool.SourcesWithManySourcesLast
import com.phaller.rasync.pool.SourcesWithManyTargetsFirst
import com.phaller.rasync.pool.SourcesWithManyTargetsLast
import com.phaller.rasync.pool.TargetsWithManySourcesFirst
import com.phaller.rasync.pool.TargetsWithManySourcesLast
import com.phaller.rasync.pool.TargetsWithManyTargetsFirst
import com.phaller.rasync.pool.TargetsWithManyTargetsLast
import com.phaller.rasync.test.opal.ifds.Fact
import com.phaller.rasync.test.opal.ifds.IFDSProperty

import org.opalj.util.PerformanceEvaluation
import org.opalj.br.DeclaredMethod

// A strategy tailored to PurityAnalysis
object PurityStrategy extends SchedulingStrategy[Purity, Null] {
  override def calcPriority(dependentCell: Cell[Purity, Null], other: Cell[Purity, Null], value: Try[ValueOutcome[Purity]]): Int = value match {
    case scala.util.Success(FinalOutcome(Impure)) => -1
    case _ => 1
  }

  override def calcPriority(dependentCell: Cell[Purity, Null], value: Try[Purity]): Int = value match {
    case scala.util.Success(Pure) => 0
    case _ => -1
  }
}

object PurityAnalysis extends ProjectAnalysisApplication {

  override def main(args: Array[String]): Unit = {
    val lib = Project(new java.io.File(args(args.length - 1))) //JRELibraryFolder.getAbsolutePath))

    println("Heap size: " + Runtime.getRuntime().maxMemory())

    schedulingStrategy = new DefaultScheduling[Purity, Null]
    PerformanceEvaluation.time {
      val report = PurityAnalysis.doAnalyze(lib.recreate(), List.empty, () => false)
    } { t ⇒ println(s"DefaultScheduling(Warmup),${t.timeSpan}") }

    for {
      scheduling ← List(
        new DefaultScheduling[Purity, Null],
        new SourcesWithManyTargetsFirst[Purity, Null],
        new SourcesWithManyTargetsLast[Purity, Null],
        new TargetsWithManySourcesFirst[Purity, Null],
        new TargetsWithManySourcesLast[Purity, Null],
        new TargetsWithManyTargetsFirst[Purity, Null],
        new TargetsWithManyTargetsLast[Purity, Null],
        new SourcesWithManySourcesFirst[Purity, Null],
        new SourcesWithManySourcesLast[Purity, Null],
        PurityStrategy)
      i ← (0 until 5)
    } {
      val p = lib.recreate()
      schedulingStrategy = scheduling
      PerformanceEvaluation.time {
        val report = PurityAnalysis.doAnalyze(p, List.empty, () => false)
      } { t ⇒ println(s"$scheduling,${t.timeSpan}") }
      //println(report.toConsoleString.split("\n").slice(0, 2).mkString("\n"))
    }
  }

  var schedulingStrategy: SchedulingStrategy[Purity, Null] = null

  override def doAnalyze(
    project: Project[URL],
    parameters: Seq[String] = List.empty,
    isInterrupted: () ⇒ Boolean): BasicReport = {

    val startTime = System.currentTimeMillis // Used for measuring execution time
    // 1. Initialization of key data structures (one cell(completer) per method)
    implicit val pool: HandlerPool[Purity, Null] = new HandlerPool(key = PurityKey, parallelism = 10, schedulingStrategy = schedulingStrategy)
    var methodToCell = Map.empty[Method, Cell[Purity, Null]]
    for {
      classFile <- project.allProjectClassFiles
      method <- classFile.methods
    } {
      val cell = pool.mkCell(_ => {
        analyze(project, methodToCell, classFile, method)
      })(Updater.partialOrderingToUpdater)
      methodToCell = methodToCell + ((method, cell))
    }

    val middleTime = System.currentTimeMillis

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles.par
      method <- classFile.methods
    } {
      methodToCell(method).trigger()
    }
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 30.minutes)
    pool.shutdown()

    val endTime = System.currentTimeMillis

    val setupTime = middleTime - startTime
    val analysisTime = endTime - middleTime
    val combinedTime = endTime - startTime

    val pureMethods = methodToCell.filter(_._2.getResult() match {
      case Pure => true
      case _ => false
    }).keys

    val pureMethodsInfo = pureMethods.map(m => m.toJava).toList.sorted

    BasicReport(s"pure methods analysis:\nPURE=${pureMethods.size}\n\n" + pureMethodsInfo.mkString("\n") +
      s"\nSETUP TIME: $setupTime" +
      s"\nANALYIS TIME: $analysisTime" +
      s"\nCOMBINED TIME: $combinedTime")
  }

  /**
   * Determines the purity of the given method.
   */
  def analyze(
    project: Project[URL],
    methodToCell: Map[Method, Cell[Purity, Null]],
    classFile: ClassFile,
    method: Method): Outcome[Purity] = {
    import project.nonVirtualCall

    val cell = methodToCell(method)

    if ( // Due to a lack of knowledge, we classify all native methods or methods that
    // belong to a library (and hence lack the body) as impure...
    method.body.isEmpty /*HERE: method.isNative || "isLibraryMethod(method)"*/ ||
      // for simplicity we are just focusing on methods that do not take objects as parameters
      method.parameterTypes.exists(!_.isBaseType)) {
      return FinalOutcome(Impure)
    }

    val dependencies = scala.collection.mutable.Set.empty[Method]
    val declaringClassType = classFile.thisType
    val methodDescriptor = method.descriptor
    val methodName = method.name
    val body = method.body.get
    val instructions = body.instructions
    val maxPC = instructions.size

    var currentPC = 0
    while (currentPC < maxPC) {
      val instruction = instructions(currentPC)

      (instruction.opcode: @scala.annotation.switch) match {
        case GETSTATIC.opcode ⇒
          val GETSTATIC(declaringClass, fieldName, fieldType) = instruction
          import project.resolveFieldReference
          resolveFieldReference(declaringClass, fieldName, fieldType) match {

            case Some(field) if field.isFinal ⇒ NoOutcome
            /* Nothing to do; constants do not impede purity! */

            // case Some(field) if field.isPrivate /*&& field.isNonFinal*/ ⇒
            // check if the field is effectively final

            case _ ⇒
              return FinalOutcome(Impure);
          }

        case INVOKESPECIAL.opcode | INVOKESTATIC.opcode ⇒ instruction match {

          case MethodInvocationInstruction(`declaringClassType`, _, `methodName`, `methodDescriptor`) ⇒
          // We have a self-recursive call; such calls do not influence
          // the computation of the method's purity and are ignored.
          // Let's continue with the evaluation of the next instruction.

          case mii: NonVirtualMethodInvocationInstruction ⇒

            nonVirtualCall(method.classFile.thisType, mii) match {

              case Success(callee) ⇒
                /* Recall that self-recursive calls are handled earlier! */
                dependencies.add(callee)

              case _ /* Empty or Failure */ ⇒

                // We know nothing about the target method (it is not
                // found in the scope of the current project).
                return FinalOutcome(Impure)
            }

        }

        case NEW.opcode |
          GETFIELD.opcode |
          PUTFIELD.opcode | PUTSTATIC.opcode |
          NEWARRAY.opcode | MULTIANEWARRAY.opcode | ANEWARRAY.opcode |
          AALOAD.opcode | AASTORE.opcode |
          BALOAD.opcode | BASTORE.opcode |
          CALOAD.opcode | CASTORE.opcode |
          SALOAD.opcode | SASTORE.opcode |
          IALOAD.opcode | IASTORE.opcode |
          LALOAD.opcode | LASTORE.opcode |
          DALOAD.opcode | DASTORE.opcode |
          FALOAD.opcode | FASTORE.opcode |
          ARRAYLENGTH.opcode |
          MONITORENTER.opcode | MONITOREXIT.opcode |
          INVOKEDYNAMIC.opcode | INVOKEVIRTUAL.opcode | INVOKEINTERFACE.opcode ⇒
          return FinalOutcome(Impure)

        case _ ⇒
        /* All other instructions (IFs, Load/Stores, Arith., etc.) are pure. */
      }
      currentPC = body.pcOfNextInstruction(currentPC)
    }

    // Every method that is not identified as being impure is (conditionally) pure.
    if (dependencies.isEmpty) {
      FinalOutcome(Pure)
    } else {
      cell.when(dependencies.map(methodToCell))(c)
      NextOutcome(UnknownPurity) // == NoOutcome
    }
  }

  def c(v: Iterable[(Cell[Purity, Null], Try[ValueOutcome[Purity]])]): Outcome[Purity] = {
    // If any dependee is Impure, the dependent Cell is impure.
    // Otherwise, we do not know anything new.
    // Exception will be rethrown.
    if (v.collectFirst({
      case (_, scala.util.Success(FinalOutcome(Impure))) => true
      case (_, scala.util.Failure(_)) => true
    }).isDefined)
      FinalOutcome(Impure)
    else NoOutcome
  }
}
