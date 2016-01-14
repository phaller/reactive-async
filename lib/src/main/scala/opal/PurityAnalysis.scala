package opal

import java.net.URL

import scala.collection.JavaConverters._

import cell.{HandlerPool, CellCompleter, Cell, Key}
import org.opalj.br.{ClassFile, PC, Method, MethodWithBody}
import org.opalj.br.analyses.{BasicReport, DefaultOneStepAnalysis, Project}
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


object PurenessKey extends Key[Purity] {
  def resolve: Purity = Pure

  override def toString = "Pureness"
}

sealed trait Purity

case object Pure extends Purity

case object Impure extends Purity


object PurityAnalysis extends DefaultOneStepAnalysis {

  val pureMethods = new java.util.concurrent.ConcurrentLinkedQueue[Method]()

  override def doAnalyze(
                          project: Project[URL],
                          parameters: Seq[String] = List.empty,
                          isInterrupted: () ⇒ Boolean
                        ): BasicReport = {

    // 1. Initialization of key data structures (one cell(completer) per method)
    val pool = new HandlerPool()
    var methodToCellCompleter = Map.empty[Method, CellCompleter[PurenessKey.type, Purity]]
    for {
      classFile <- project.allProjectClassFiles
      method@MethodWithBody(body) <- classFile.methods
    } {
      val cellCompleter = CellCompleter[PurenessKey.type, Purity](pool, PurenessKey)
      methodToCellCompleter = methodToCellCompleter + ((method, cellCompleter))
    }

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles
      method@MethodWithBody(body) <- classFile.methods
    } {
      pool.execute(() => {
        analyze(project,methodToCellCompleter,classFile, method)
      })
    }


    BasicReport("pure methods analysis:\n"+pureMethods.asScala.map(_.toJava).mkString("\n"))
  }

  /**
    * Determines the purity of the given method.
    */
  def analyze(
               project: Project[URL],
               methodToCellCompleter: Map[Method, CellCompleter[PurenessKey.type, Purity]],
               classFile : ClassFile,
               method: Method
             ): Unit = {
    val cellCompleter = methodToCellCompleter(method)

    if (
    // Due to a lack of knowledge, we classify all native methods or methods that
    // belong to a library (and hence lack the body) as impure...
      method.body.isEmpty /*HERE: method.isNative || "isLibraryMethod(method)"*/ ||
        // for simplicity we are just focusing on methods that do not take objects as parameters
        method.parameterTypes.exists(!_.isBaseType)
    ) {
      cellCompleter.putFinal(Impure)
      return;
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
          import project.classHierarchy.resolveFieldReference
          resolveFieldReference(declaringClass, fieldName, fieldType, project) match {

            case Some(field) if field.isFinal ⇒
            /* Nothing to do; constants do not impede purity! */

           // case Some(field) if field.isPrivate /*&& field.isNonFinal*/ ⇒
           // check if the field is effectively final

            case _ ⇒
              cellCompleter.putFinal(Impure)
              return ;
          }

        case INVOKESPECIAL.opcode | INVOKESTATIC.opcode ⇒ instruction match {

          case MethodInvocationInstruction(`declaringClassType`, `methodName`, `methodDescriptor`) ⇒
          // We have a self-recursive call; such calls do not influence
          // the computation of the method's purity and are ignored.
          // Let's continue with the evaluation of the next instruction.

          case MethodInvocationInstruction(declaringClassType, methodName, methodDescriptor) ⇒
            import project.classHierarchy.lookupMethodDefinition
            val calleeOpt =
              lookupMethodDefinition(
                declaringClassType.asObjectType /* this is safe...*/ ,
                methodName,
                methodDescriptor,
                project
              )
            calleeOpt match {
              case None ⇒
                // We know nothing about the target method (it is not
                // found in the scope of the current project).
                cellCompleter.putFinal(Impure)
                return ;

              case Some(callee) ⇒
                /* Recall that self-recursive calls are handled earlier! */

                val targetCellCompleter = methodToCellCompleter(callee)
                hasDependencies = true
                cellCompleter.cell.whenComplete(targetCellCompleter.cell,_ == Impure,Impure)
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
      println("Pure method: "+method.toJava(classFile))
      pureMethods.add(method)
    }
  }
}

