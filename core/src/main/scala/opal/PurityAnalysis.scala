package opal

import java.net.URL

import scala.collection.JavaConverters._

import scala.concurrent.Await
import scala.concurrent.duration._

import cell.{ HandlerPool, CellCompleter, Cell }
import org.opalj.Success
import org.opalj.br.{ ClassFile, PC, Method, MethodWithBody }
import org.opalj.br.analyses.{ BasicReport, DefaultOneStepAnalysis, Project }
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

object PurityAnalysis extends DefaultOneStepAnalysis {

  override def doAnalyze(
    project: Project[URL],
    parameters: Seq[String] = List.empty,
    isInterrupted: () ⇒ Boolean): BasicReport = {

    val startTime = System.currentTimeMillis // Used for measuring execution time
    // 1. Initialization of key data structures (one cell(completer) per method)
    val pool = new HandlerPool()
    var methodToCellCompleter = Map.empty[Method, CellCompleter[PurityKey.type, Purity]]
    for {
      classFile <- project.allProjectClassFiles
      method <- classFile.methods
    } {
      val cellCompleter = CellCompleter[PurityKey.type, Purity](pool, PurityKey)
      methodToCellCompleter = methodToCellCompleter + ((method, cellCompleter))
    }

    val middleTime = System.currentTimeMillis

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles.par
      method <- classFile.methods
    } {
      pool.execute(() => analyze(project, methodToCellCompleter, classFile, method))
    }
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 30.minutes)
    pool.shutdown()

    val endTime = System.currentTimeMillis

    val setupTime = middleTime - startTime
    val analysisTime = endTime - middleTime
    val combinedTime = endTime - startTime

    val pureMethods = methodToCellCompleter.filter(_._2.cell.getResult match {
      case Pure => true
      case _ => false
    }).map(_._1)

    val pureMethodsInfo = pureMethods.map(m => m.toJava).toList.sorted

    BasicReport("pure methods analysis:\n" + pureMethodsInfo.mkString("\n") +
      s"\nSETUP TIME: $setupTime" +
      s"\nANALYIS TIME: $analysisTime" +
      s"\nCOMBINED TIME: $combinedTime")
  }

  /**
   * Determines the purity of the given method.
   */
  def analyze(
    project: Project[URL],
    methodToCellCompleter: Map[Method, CellCompleter[PurityKey.type, Purity]],
    classFile: ClassFile,
    method: Method): Unit = {

    import project.nonVirtualCall

    val cellCompleter = methodToCellCompleter(method)

    if ( // Due to a lack of knowledge, we classify all native methods or methods that
    // belong to a library (and hence lack the body) as impure...
    method.body.isEmpty /*HERE: method.isNative || "isLibraryMethod(method)"*/ ||
      // for simplicity we are just focusing on methods that do not take objects as parameters
      method.parameterTypes.exists(!_.isBaseType)) {
      cellCompleter.putFinal(Impure)
      return ;
    }

    var hasDependencies = false
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

            case Some(field) if field.isFinal ⇒
            /* Nothing to do; constants do not impede purity! */

            // case Some(field) if field.isPrivate /*&& field.isNonFinal*/ ⇒
            // check if the field is effectively final

            case _ ⇒
              cellCompleter.putFinal(Impure)
              return ;
          }

        case INVOKESPECIAL.opcode | INVOKESTATIC.opcode ⇒ instruction match {

          case MethodInvocationInstruction(`declaringClassType`, _, `methodName`, `methodDescriptor`) ⇒
          // We have a self-recursive call; such calls do not influence
          // the computation of the method's purity and are ignored.
          // Let's continue with the evaluation of the next instruction.

          case mii: NonVirtualMethodInvocationInstruction ⇒

            nonVirtualCall(mii) match {

              case Success(callee) ⇒
                /* Recall that self-recursive calls are handled earlier! */

                val targetCellCompleter = methodToCellCompleter(callee)
                hasDependencies = true
                cellCompleter.cell.whenComplete(targetCellCompleter.cell, (p: Purity) => p == Impure, Impure)

              case _ /* Empty or Failure */ ⇒

                // We know nothing about the target method (it is not
                // found in the scope of the current project).
                cellCompleter.putFinal(Impure)
                return ;
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
          cellCompleter.putFinal(Impure)
          return ;

        case _ ⇒
        /* All other instructions (IFs, Load/Stores, Arith., etc.) are pure. */
      }
      currentPC = body.pcOfNextInstruction(currentPC)
    }

    // Every method that is not identified as being impure is (conditionally)pure.
    if (!hasDependencies) {
      cellCompleter.putFinal(Pure)
      //println("Immediately Pure Method: "+method.toJava(classFile))
    }
  }
}
