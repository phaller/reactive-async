package opal

import java.net.URL

import org.opalj.fpcf._

import scala.collection.JavaConverters._

import scala.concurrent.Await
import scala.concurrent.duration._

import cell._
import org.opalj.br.{ Field, ClassFile, ObjectType }
import org.opalj.br.analyses.{ BasicReport, DefaultOneStepAnalysis, Project, PropertyStoreKey }
import org.opalj.br.analyses.TypeExtensibilityKey
import org.opalj.fpcf.analyses.FieldMutabilityAnalysis
import org.opalj.fpcf.properties.FieldMutability

object ImmutabilityAnalysis extends DefaultOneStepAnalysis {

  override def doAnalyze(
    project: Project[URL],
    parameters: Seq[String] = List.empty,
    isInterrupted: () ⇒ Boolean): BasicReport = {
    // Run ClassExtensibilityAnalysis
    val projectStore = project.get(PropertyStoreKey)
    val manager = project.get(FPCFAnalysesManagerKey)
    //manager.runAll(
    //FieldMutabilityAnalysis
    // REPLACED                ObjectImmutabilityAnalysis
    // REPLACED                TypeImmutabilityAnalysis
    //)

    val startTime = System.currentTimeMillis // Used for measuring execution time

    // 1. Initialization of key data structures (two cell(completer) per class file)
    // One for Object Immutability and one for Type Immutability.
    val pool = new HandlerPool()

    // classFileToObjectTypeCellCompleter._1 = ObjectImmutability
    // classFileToObjectTypeCellCompleter._2 = TypeImmutability
    var classFileToObjectTypeCellCompleter =
      Map.empty[ClassFile, (CellCompleter[ImmutabilityKey.type, Immutability], CellCompleter[ImmutabilityKey.type, Immutability])]
    for {
      classFile <- project.allProjectClassFiles
    } {
      val cellCompleter1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      val cellCompleter2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      classFileToObjectTypeCellCompleter = classFileToObjectTypeCellCompleter + ((classFile, (cellCompleter1, cellCompleter2)))
    }

    val middleTime = System.currentTimeMillis

    // java.lang.Object is by definition immutable
    val objectClassFileOption = project.classFile(ObjectType.Object)
    objectClassFileOption.foreach { cf =>
      classFileToObjectTypeCellCompleter(cf)._1.putFinal(Immutable)
      classFileToObjectTypeCellCompleter(cf)._2.putFinal(Mutable)
    }

    // All interfaces are by definition immutable
    val allInterfaces = project.allProjectClassFiles.par.filter(cf => cf.isInterfaceDeclaration).toList
    allInterfaces.foreach(cf => classFileToObjectTypeCellCompleter(cf)._1.putFinal(Immutable))

    val classHierarchy = project.classHierarchy
    import classHierarchy.allSubtypes
    import classHierarchy.rootTypes
    import classHierarchy.isInterface
    // All classes that do not have complete superclass information are mutable
    // due to the lack of knowledge.
    val typesForWhichItMayBePossibleToComputeTheMutability = allSubtypes(ObjectType.Object, reflexive = true)
    val unexpectedRootTypes = rootTypes.filter(rt ⇒ (rt ne ObjectType.Object) && !isInterface(rt).isNo)
    unexpectedRootTypes.map(rt ⇒ allSubtypes(rt, reflexive = true)).flatten.view.
      filter(ot ⇒ !typesForWhichItMayBePossibleToComputeTheMutability.contains(ot)).
      foreach(ot ⇒ project.classFile(ot) foreach { cf ⇒
        classFileToObjectTypeCellCompleter(cf)._1.putFinal(Mutable)
      })

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles.par
    } {
      pool.execute(() => {
        if (!classFileToObjectTypeCellCompleter(classFile)._1.cell.isComplete)
          objectImmutabilityAnalysis(project, classFileToObjectTypeCellCompleter, manager, classFile)
      })
      pool.execute(() => {
        if (!classFileToObjectTypeCellCompleter(classFile)._2.cell.isComplete)
          typeImmutabilityAnalysis(project, classFileToObjectTypeCellCompleter, manager, classFile)
      })
    }
    pool.whileQuiescentResolveDefault
    pool.shutdown()

    val endTime = System.currentTimeMillis

    val setupTime = middleTime - startTime
    val analysisTime = endTime - middleTime
    val combinedTime = endTime - startTime

    /* Fixes the results so the output looks good */
    val resultClassFiles = project.allProjectClassFiles.par.filter(!allInterfaces.contains(_))
    val mutableClassFilesInfo = for {
      cf <- resultClassFiles if (classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() == Mutable)
    } yield (cf.thisType.toJava + " => " +
      classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() + "Object => " +
      classFileToObjectTypeCellCompleter(cf)._2.cell.getResult() + "Type")

    val immutableClassFilesInfo = for {
      cf <- resultClassFiles if (classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() == Immutable)
    } yield (cf.thisType.toJava + " => " +
      classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() + "Object => " +
      classFileToObjectTypeCellCompleter(cf)._2.cell.getResult() + "Type")

    val conditionallyImmutableClassFilesInfo = for {
      cf <- resultClassFiles if (classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() == ConditionallyImmutable)
    } yield (cf.thisType.toJava + " => " +
      classFileToObjectTypeCellCompleter(cf)._1.cell.getResult() + "Object => " +
      classFileToObjectTypeCellCompleter(cf)._2.cell.getResult() + "Type")

    val sortedClassFilesInfo = (immutableClassFilesInfo.toList.sorted ++
      conditionallyImmutableClassFilesInfo.toList.sorted ++ mutableClassFilesInfo.toList.sorted)
    BasicReport(sortedClassFilesInfo.mkString("\n") +
      s"\nSETUP TIME: $setupTime" +
      s"\nANALYIS TIME: $analysisTime" +
      s"\nCOMBINED TIME: $combinedTime")
  }

  /**
   * This function is used for the tests, to not run the external analyses several times.
   *
   * @param project
   * @param manager
   * @return
   */
  def analyzeWithoutClassExtensibilityAndFieldMutabilityAnalysis(
    project: Project[URL],
    manager: FPCFAnalysesManager): BasicReport = {
    // 1. Initialization of key data structures (two cell(completer) per class file)
    // One for Object Immutability and one for Type Immutability.
    val pool = new HandlerPool()

    // classFileToObjectTypeCellCompleter._1 = ObjectImmutability
    // classFileToObjectTypeCellCompleter._2 = TypeImmutability
    var classFileToObjectTypeCellCompleter =
      Map.empty[ClassFile, (CellCompleter[ImmutabilityKey.type, Immutability], CellCompleter[ImmutabilityKey.type, Immutability])]
    for {
      classFile <- project.allProjectClassFiles
    } {
      val cellCompleter1 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      val cellCompleter2 = CellCompleter[ImmutabilityKey.type, Immutability](pool, ImmutabilityKey)
      classFileToObjectTypeCellCompleter = classFileToObjectTypeCellCompleter + ((classFile, (cellCompleter1, cellCompleter2)))
    }

    // java.lang.Object is by definition immutable
    val objectClassFileOption = project.classFile(ObjectType.Object)
    objectClassFileOption.foreach { cf =>
      classFileToObjectTypeCellCompleter(cf)._1.putFinal(Immutable)
      classFileToObjectTypeCellCompleter(cf)._2.putFinal(Mutable) // Should the TypeImmutability be Mutable?
    }

    // All interfaces are by definition immutable
    val allInterfaces = project.allProjectClassFiles.par.filter(cf => cf.isInterfaceDeclaration).toList
    allInterfaces.foreach(cf => classFileToObjectTypeCellCompleter(cf)._1.putFinal(Immutable))

    val classHierarchy = project.classHierarchy
    import classHierarchy.allSubtypes
    import classHierarchy.rootTypes
    import classHierarchy.isInterface
    // All classes that do not have complete superclass information are mutable
    // due to the lack of knowledge.
    val typesForWhichItMayBePossibleToComputeTheMutability = allSubtypes(ObjectType.Object, reflexive = true)
    val unexpectedRootTypes = rootTypes.filter(rt ⇒ (rt ne ObjectType.Object) && !isInterface(rt).isNo)
    unexpectedRootTypes.map(rt ⇒ allSubtypes(rt, reflexive = true)).flatten.view.
      filter(ot ⇒ !typesForWhichItMayBePossibleToComputeTheMutability.contains(ot)).
      foreach(ot ⇒ project.classFile(ot) foreach { cf ⇒
        classFileToObjectTypeCellCompleter(cf)._1.putFinal(Mutable)
      })

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles.par
    } {
      pool.execute(() => {
        if (!classFileToObjectTypeCellCompleter(classFile)._1.cell.isComplete)
          objectImmutabilityAnalysis(project, classFileToObjectTypeCellCompleter, manager, classFile)
      })
      pool.execute(() => {
        if (!classFileToObjectTypeCellCompleter(classFile)._2.cell.isComplete)
          typeImmutabilityAnalysis(project, classFileToObjectTypeCellCompleter, manager, classFile)
      })
    }
    pool.whileQuiescentResolveCell
    pool.shutdown()

    /* Fixes the results so the output looks good */
    val mutableClassFilesInfo = for {
      (cf, (objImmutability, typeImmutability)) <- classFileToObjectTypeCellCompleter if (objImmutability.cell.getResult() == Mutable)
    } yield (cf.thisType.toJava + " => " + objImmutability.cell.getResult() + "Object => " + typeImmutability.cell.getResult() + "Type")

    val immutableClassFilesInfo = for {
      (cf, (objImmutability, typeImmutability)) <- classFileToObjectTypeCellCompleter if (objImmutability.cell.getResult() == Immutable)
    } yield (cf.thisType.toJava + " => " + objImmutability.cell.getResult() + "Object => " + typeImmutability.cell.getResult() + "Type")

    val conditionallyImmutableClassFilesInfo = for {
      (cf, (objImmutability, typeImmutability)) <- classFileToObjectTypeCellCompleter if (objImmutability.cell.getResult() == ConditionallyImmutable)
    } yield (cf.thisType.toJava + " => " + objImmutability.cell.getResult() + "Object => " + typeImmutability.cell.getResult() + "Type")

    val sortedClassFilesInfo = (immutableClassFilesInfo.toList.sorted ++
      conditionallyImmutableClassFilesInfo.toList.sorted ++ mutableClassFilesInfo.toList.sorted)
    BasicReport(sortedClassFilesInfo)
  }

  /**
   *  Determines a class files' ObjectImmutability.
   */
  def objectImmutabilityAnalysis(
    project: Project[URL],
    classFileToObjectTypeCellCompleter: Map[ClassFile, (CellCompleter[ImmutabilityKey.type, Immutability], CellCompleter[ImmutabilityKey.type, Immutability])],
    manager: FPCFAnalysesManager,
    cf: ClassFile): Unit = {
    val cellCompleter = classFileToObjectTypeCellCompleter(cf)._1

    val classHierarchy = project.classHierarchy
    val directSuperTypes = classHierarchy.directSupertypes(cf.thisType)

    // Check fields to determine ObjectImmutability
    val nonFinalInstanceFields = cf.fields.collect { case f if !f.isStatic && !f.isFinal => f }

    if (!nonFinalInstanceFields.isEmpty)
      cellCompleter.putFinal(Mutable)

    // If the cell hasn't already been completed with an ObjectImmutability, then it is
    // dependent on FieldMutability and its superclasses
    if (!cellCompleter.cell.isComplete) {
      if (cf.fields.exists(f => !f.isStatic && f.fieldType.isArrayType))
        cellCompleter.putNext(ConditionallyImmutable)
      else {
        val fieldTypes: Set[ObjectType] =
          cf.fields.collect {
            case f if !f.isStatic && f.fieldType.isObjectType => f.fieldType.asObjectType
          }.toSet

        val hasUnresolvableDependencies =
          fieldTypes.exists { t =>
            project.classFile(t) match {
              case None => true /* we have an unresolved dependency */
              case Some(classFile) => false /* do nothing */
            }
          }

        if (hasUnresolvableDependencies)
          cellCompleter.putNext(ConditionallyImmutable)
        else {
          val finalInstanceFields = cf.fields.collect { case f if !f.isStatic && f.isFinal => f }
          finalInstanceFields.foreach { f =>
            if (f.fieldType.isObjectType) {
              project.classFile(f.fieldType.asObjectType) match {
                case Some(classFile) =>
                  val fieldTypeCell = classFileToObjectTypeCellCompleter(classFile)._2.cell
                  cellCompleter.cell.whenNext(
                    fieldTypeCell,
                    (fieldImm: Immutability) => fieldImm match {
                      case Mutable | ConditionallyImmutable => WhenNext
                      case Immutable => FalsePred
                    },
                    ConditionallyImmutable)
                case None => /* Do nothing */
              }
            }
          }
        }
      }
      // Check with superclass to determine ObjectImmutability
      val directSuperClasses = directSuperTypes.
        filter(superType => project.classFile(superType) != None).
        map(superType => project.classFile(superType).get)

      directSuperClasses foreach { superClass =>
        cellCompleter.cell.whenNext(
          classFileToObjectTypeCellCompleter(superClass)._1.cell,
          (imm: Immutability) => imm match {
            case Immutable => FalsePred
            case Mutable => WhenNextComplete
            case ConditionallyImmutable => WhenNext
          },
          Some(_))
      }
    }
  }

  /**
   *  Determines a class files' TypeImmutability.
   */
  def typeImmutabilityAnalysis(
    project: Project[URL],
    classFileToObjectTypeCellCompleter: Map[ClassFile, (CellCompleter[ImmutabilityKey.type, Immutability], CellCompleter[ImmutabilityKey.type, Immutability])],
    manager: FPCFAnalysesManager,
    cf: ClassFile): Unit = {
    val typeExtensibility = project.get(TypeExtensibilityKey)
    val cellCompleter = classFileToObjectTypeCellCompleter(cf)._2
    val isExtensible = typeExtensibility(cf.thisType)
    if (isExtensible.isYesOrUnknown)
      cellCompleter.putFinal(Mutable)

    val classHierarchy = project.classHierarchy
    val directSubtypes = classHierarchy.directSubtypesOf(cf.thisType)

    if (!cellCompleter.cell.isComplete) {
      // If this class file doesn't have subtypes, then the TypeImmutability is the same as
      // the ObjectImmutability
      if (cf.isFinal || directSubtypes.isEmpty) {
        cellCompleter.cell.whenNext(
          classFileToObjectTypeCellCompleter(cf)._1.cell,
          (imm: Immutability) => imm match {
            case Immutable => FalsePred
            case Mutable => WhenNextComplete
            case ConditionallyImmutable => WhenNext
          },
          Some(_))
      } else {
        val unavailableSubtype = directSubtypes.find(t ⇒ project.classFile(t).isEmpty)
        if (unavailableSubtype.isDefined)
          cellCompleter.putFinal(Mutable)

        if (!cellCompleter.cell.isComplete) {
          // Check subclasses to determine TypeImmutability
          val directSubclasses = directSubtypes map { subtype ⇒ project.classFile(subtype).get }
          directSubclasses foreach { subclass =>
            cellCompleter.cell.whenNext(
              classFileToObjectTypeCellCompleter(subclass)._2.cell,
              (imm: Immutability) => imm match {
                case Immutable => FalsePred
                case Mutable => WhenNextComplete
                case ConditionallyImmutable => WhenNext
              },
              Some(_))
          }
        }
      }
    }
  }
}
