/* BSD 2-Clause License - see OPAL/LICENSE for details. */
package com.phaller.rasync.test.opal.ifds

import java.util.concurrent.ConcurrentHashMap

import com.phaller.rasync.lattice.{Key, Lattice}
import com.phaller.rasync.cell._
import com.phaller.rasync.pool.{HandlerPool, SchedulingStrategy}
import scala.collection.{Set ⇒ SomeSet}

import org.opalj.br.DeclaredMethod
import org.opalj.br.Method
import org.opalj.br.ObjectType
import org.opalj.br.analyses.{DeclaredMethods, DeclaredMethodsKey, Project}
import org.opalj.br.cfg.BasicBlock
import org.opalj.br.cfg.CFG
import org.opalj.br.cfg.CFGNode
import org.opalj.tac.DUVar
import org.opalj.tac.Stmt
import org.opalj.tac.VirtualMethodCall
import org.opalj.tac.NonVirtualMethodCall
import org.opalj.tac.StaticMethodCall
import org.opalj.tac.Assignment
import org.opalj.tac.StaticFunctionCall
import org.opalj.tac.NonVirtualFunctionCall
import org.opalj.tac.VirtualFunctionCall
import org.opalj.tac.TACStmts
import org.opalj.tac.Expr
import org.opalj.tac.Call
import org.opalj.tac.ExprStmt
import org.opalj.tac.TACode
import org.opalj.tac.TACMethodParameter
import scala.annotation.tailrec

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.Duration

import org.opalj.value.ValueInformation
import scala.util.Try

import com.phaller.rasync.test.opal.ifds.AbstractIFDSAnalysis.Statement
import com.phaller.rasync.test.opal.ifds.AbstractIFDSAnalysis.V

import org.opalj.fpcf.FinalEP
import org.opalj.br.fpcf.PropertyStoreKey
import org.opalj.br.fpcf.cg.properties.Callees
import org.opalj.tac.LazyDetachedTACAIKey
import org.opalj.tac.cg.CHACallGraphKey
import org.opalj.tac.cg.RTACallGraphKey
import org.opalj.tac.Return
import org.opalj.tac.ReturnValue

/**
 * The super type of all IFDS facts.
 *
 */
trait AbstractIFDSFact

/**
 *
 * The super type of all null facts.
 *
 */
trait AbstractIFDSNullFact extends AbstractIFDSFact

/**
 * A framework for IFDS analyses.
 *
 * @tparam DataFlowFact The type of flow facts the concrete analysis wants to track
 *
 * @author Dominik Helm
 * @author Jan Kölzer (adaption to Reactive Async)
 */
// The `scheduling` is only for testing. In production, one would create a HandlerPool with the best scheduling for IFDS
abstract class AbstractIFDSAnalysis[DataFlowFact <: AbstractIFDSFact](parallelism: Int, scheduling: SchedulingStrategy[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)])(implicit project: Project[_]) {

    private val tacProvider: Method ⇒ TACode[TACMethodParameter, DUVar[ValueInformation]] = project.get(LazyDetachedTACAIKey)
    val classHierarchy = project.classHierarchy

    // [p. ackland] "Both resolve and fallback return the empty set for each cell because on quiescence we know that no more propagations will be made and the cell can be completed."
    object TheKey extends Key[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)] {
        override def resolve(cells: Iterable[Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)]]): Iterable[(Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)], IFDSProperty[DataFlowFact])] = {
            val p = createProperty(Map.empty)
            cells.map((_, p))
        }

        override def fallback(cells: Iterable[Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)]]): Iterable[(Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)], IFDSProperty[DataFlowFact])] = {
            val p = createProperty(Map.empty)
            cells.map((_, p))
        }
    }

    implicit object TheLattice extends Lattice[IFDSProperty[DataFlowFact]] {
        override def join(v1: IFDSProperty[DataFlowFact], v2: IFDSProperty[DataFlowFact]): IFDSProperty[DataFlowFact] =
            createProperty(mergeMaps(v1.flows, v2.flows))

        override val bottom: IFDSProperty[DataFlowFact] = createProperty(Map.empty)
    }

    implicit val pool: HandlerPool[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)] = new HandlerPool[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)](key = TheKey, parallelism = parallelism, schedulingStrategy = scheduling)

    // Each cell represents a Method + the flow facts currently known.
    // The following maps maps (method,fact) to cells
    private val mfToCell = TrieMap.empty[(DeclaredMethod, DataFlowFact), Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)]]

    /**
     * Provides the concrete property key (that must be unique for every distinct concrete analysis
     * and the lower bound for the IFDSProperty.
     */
    val property: IFDSPropertyMetaInformation[DataFlowFact]

    /** Creates the concrete IFDSProperty. */
    def createProperty(result: Map[Statement, Set[DataFlowFact]]): IFDSProperty[DataFlowFact]

    /**
     * Computes the DataFlowFacts valid after statement `stmt` on the CFG edge to statement `succ`
     * if the DataFlowFacts `in` held before `stmt`.
     */
    def normalFlow(stmt: Statement, succ: Statement, in: Set[DataFlowFact]): Set[DataFlowFact]

    /**
     * Computes the DataFlowFacts valid on entry to method `callee` when it is called from statement
     * `stmt` if the DataFlowFacts `in` held before `stmt`.
     */
    def callFlow(stmt: Statement, callee: DeclaredMethod, in: Set[DataFlowFact]): Set[DataFlowFact]

    /**
     * Computes the DataFlowFacts valid on the CFG edge from statement `stmt` to `succ` if `callee`
     * was invoked by `stmt` and DataFlowFacts `in` held before the final statement `exit` of
     * `callee`.
     */
    def returnFlow(stmt: Statement, callee: DeclaredMethod, exit: Statement, succ: Statement, in: Set[DataFlowFact]): Set[DataFlowFact]

    /**
     * Computes the DataFlowFacts valid on the CFG edge from statement `stmt` to `succ` irrespective
     * of the call in `stmt` if the DataFlowFacts `in` held before `stmt`.
     */
    def callToReturnFlow(stmt: Statement, succ: Statement, in: Set[DataFlowFact]): Set[DataFlowFact]

    /**
     * Computes the data flow for a summary edge of a native method call.
     *
     * @param call The statement, which invoked the call.
     * @param callee The method, called by `call`.
     * @param successor The statement, which will be executed after the call.
     * @param in Some facts valid before the `call`.
     * @return The facts valid after the call, excluding the call-to-return flow.
     */
    def nativeCall(call: Statement, callee: DeclaredMethod, successor: Statement, in: Set[DataFlowFact]): Set[DataFlowFact]

    implicit protected[this] val declaredMethods: DeclaredMethods = project.get(DeclaredMethodsKey)

    class State(
        val declaringClass:       ObjectType,
        val method:               Method,
        val source:               (DeclaredMethod, DataFlowFact),
        val code:                 Array[Stmt[V]],
        val cfg:                  CFG[Stmt[V], TACStmts[V]],
        var pendingIfdsCallSites: Map[(DeclaredMethod, DataFlowFact), Set[(BasicBlock, Int)]],
        var pendingIfdsDependees: Map[Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)], Outcome[IFDSProperty[DataFlowFact]]] = Map.empty,
        var incomingFacts:        Map[BasicBlock, Set[DataFlowFact]]                                                                         = Map.empty,
        var outgoingFacts:        Map[BasicBlock, Map[CFGNode, Set[DataFlowFact]]]                                                           = Map.empty
    )

    def waitForCompletion(d: Duration): Unit

    /** Wait for completion before calling this method. */
    def getResult(e: (DeclaredMethod, DataFlowFact)): IFDSProperty[DataFlowFact] =
        cell(e).getResult()

    /** Map (method, fact) pairs to cells. A new cell is created, if it does not exist yet. See also mf() for the reverse direction. */
    private def cell(source: (DeclaredMethod, DataFlowFact)): Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)] = {
        // Can performance be improved if we first check, if mfToCell.isDefinedAt(source) first?
        val c = pool.mkSequentialCell(c ⇒ performAnalysis(source), source)
        mfToCell.putIfAbsent(source, c)
            .getOrElse(c)
    }

    /** Start the computation for a source. Afterwards, wait for completion.*/
    def forceComputation(source: (DeclaredMethod, DataFlowFact)): Unit =
        cell(source).trigger()

    /**
     * Performs IFDS analysis for one specific entity, i.e. one DeclaredMethod/DataFlowFact pair.
     */
    def performAnalysis(source: (DeclaredMethod, DataFlowFact)): Outcome[IFDSProperty[DataFlowFact]] = {
        val (declaredMethod, sourceFact) = source

        // Deal only with single defined methods for now
        if (!declaredMethod.hasSingleDefinedMethod)
            return FinalOutcome(createProperty(Map.empty));

        val method = declaredMethod.definedMethod
        val declaringClass: ObjectType = method.classFile.thisType

        // If this is not the method's declaration, but a non-overwritten method in a subtype,
        // don't re-analyze the code
        if (declaringClass ne declaredMethod.declaringClassType)
            return baseMethodResult(source);

        // get a default TAC
        val tac = tacProvider(method)
        val cfg = tac.cfg
        val code = tac.instructions

        implicit val state: State =
            new State(declaringClass, method, source, code, cfg, Map(source → Set.empty))

        // Start processing at the start of the cfg with the given source fact
        val start = cfg.startBlock
        state.incomingFacts += start → Set(sourceFact)
        process(mutable.Queue((start, Set(sourceFact), None, None, None)))

        createResult()
    }

    /**
     * Processes a queue of BasicBlocks where new DataFlowFacts are available.
     */
    def process(
        initialWorklist: mutable.Queue[(BasicBlock, Set[DataFlowFact], Option[Int], Option[Method], Option[DataFlowFact])]
    )(implicit state: State): Unit = {
        val worklist = initialWorklist

        while (worklist.nonEmpty) {
            val (bb, in, callIndex, callee, dataFlowFact) = worklist.dequeue()
            val oldOut = state.outgoingFacts.getOrElse(bb, Map.empty)
            val nextOut = analyzeBasicBlock(bb, in, callIndex, callee, dataFlowFact)
            val allOut = mergeMaps(oldOut, nextOut)
            state.outgoingFacts = state.outgoingFacts.updated(bb, allOut)

            for (successor ← bb.successors) {
                if (successor.isExitNode) {
                    // Handle self-dependencies: Propagate new information to self calls
                    if ((nextOut.getOrElse(successor, Set.empty) -- oldOut.getOrElse(successor, Set.empty)).nonEmpty) {
                        val source = state.source
                        reAnalyzeCalls(state.pendingIfdsCallSites(source), source._1.definedMethod, Some(source._2))
                    }
                } else {
                    val succ = (if (successor.isBasicBlock) {
                        successor
                    } else { // Skip CatchNodes directly to their handler BasicBlock
                        assert(successor.isCatchNode)
                        assert(successor.successors.size == 1)
                        successor.successors.head
                    }).asBasicBlock

                    val nextIn = nextOut.getOrElse(successor, Set.empty)
                    val oldIn = state.incomingFacts.getOrElse(succ, Set.empty)
                    state.incomingFacts = state.incomingFacts.updated(succ, oldIn ++ nextIn)
                    val newIn = nextIn -- oldIn
                    if (newIn.nonEmpty) {
                        worklist.enqueue((succ, newIn, None, None, None))
                    }
                }
            }
        }
    }

    /**
     * Gets, for an ExitNode of the CFG, the DataFlowFacts valid on each CFG edge from a
     * statement to that ExitNode.
     */
    def collectResult(node: CFGNode)(implicit state: State): Map[Statement, Set[DataFlowFact]] =
        node.predecessors.foldLeft(Map.empty[Statement, Set[DataFlowFact]]) { (curMap, nextBB: CFGNode) ⇒
            val bb = nextBB.asBasicBlock
            if (state.outgoingFacts.get(bb).flatMap(_.get(node)).isDefined) {
                val index = bb.endPC
                curMap + (Statement(
                    state.method,
                    bb,
                    state.code(index),
                    index,
                    state.code,
                    state.cfg
                ) → state.outgoingFacts(bb)(node))
            } else curMap
        }

    /**
     * Creates the analysis result from the current state.
     */
    def createResult()(implicit state: State): Outcome[IFDSProperty[DataFlowFact]] = {
        val propertyValue = createProperty(mergeMaps(
            collectResult(state.cfg.normalReturnNode),
            collectResult(state.cfg.abnormalReturnNode)
        ))

        var dependees = state.pendingIfdsDependees.keys

        if (dependees.isEmpty) {
            FinalOutcome(propertyValue)
        } else {
            val thisCell = cell(state.source)
            thisCell.when(dependees.toSeq: _*)(cont)
            NextOutcome(propertyValue)
        }
    }

    def cont(updates: Iterable[(Cell[IFDSProperty[DataFlowFact], (DeclaredMethod, DataFlowFact)], Try[ValueOutcome[IFDSProperty[DataFlowFact]]])])(implicit state: State): Outcome[IFDSProperty[DataFlowFact]] = {
        handleCallUpdates(updates.map(_._1.entity))
        createResult()
    }

    def analyzeBasicBlock(
        basicBlock:            BasicBlock,
        in:                    Set[DataFlowFact],
        calleeWithUpdateIndex: Option[Int],
        calleeWithUpdate:      Option[Method],
        calleeWithUpdateFact:  Option[DataFlowFact]
    )(
        implicit
        state: State
    ): Map[CFGNode, Set[DataFlowFact]] = {

        /*
         * Collects information about a statement.
         *
         * @param index The statement's index.
         * @return A tuple of the following elements:
         *         statement: The statement at `index`.
         *         calees: The methods possibly called at this statement, if it contains a call.
         *                 If `index` equals `calleeWithUpdateIndex`, only `calleeWithUpdate` will be returned.
         *         calleeFact: If `index` equals `calleeWithUpdateIndex`, only `calleeWithUpdateFact` will be returned, None otherwise.
         */
        def collectInformation(
            index: Int
        ): (Statement, Option[SomeSet[Method]], Option[DataFlowFact]) = {
            val stmt = state.code(index)
            val statement = Statement(state.method, basicBlock, stmt, index, state.code, state.cfg)
            val calleesO =
                if (calleeWithUpdateIndex.contains(index)) calleeWithUpdate.map(Set(_)) else getCalleesIfCallStatement(basicBlock, index)
            val calleeFact = if (calleeWithUpdateIndex.contains(index)) calleeWithUpdateFact else None
            (statement, calleesO, calleeFact)
        }

        var flows: Set[DataFlowFact] = in
        var index = basicBlock.startPC

        // Iterate over all statements but the last one, only keeping the resulting DataFlowFacts.
        while (index < basicBlock.endPC) {
            val (statement, calleesO, calleeFact) = collectInformation(index)
            flows = if (calleesO.isEmpty) {
                val successor =
                    Statement(state.method, basicBlock, state.code(index + 1), index + 1, state.code, state.cfg)
                normalFlow(statement, successor, flows)
            } else
                // Inside a basic block, we only have one successor --> Take the head
                handleCall(basicBlock, statement, calleesO.get, flows, calleeFact).values.head
            index += 1
        }

        // Analyze the last statement for each possible successor statement.
        val (statement, calleesO, callFact) = collectInformation(basicBlock.endPC)
        var result: Map[CFGNode, Set[DataFlowFact]] =
            if (calleesO.isEmpty) {
                var result: Map[CFGNode, Set[DataFlowFact]] = Map.empty
                for (node ← basicBlock.successors) {
                    result += node → normalFlow(statement, firstStatement(node), flows)
                }
                result
            } else {
                handleCall(basicBlock, statement, calleesO.get, flows, callFact).map(entry ⇒ entry._1.node → entry._2)
            }

        // Propagate the null fact.
        result = result.map(result ⇒ result._1 → (propagateNullFact(in, result._2)))
        result
    }

    /** Gets the expression from an assingment/expr statement. */
    def getExpression(stmt: Stmt[V]): Expr[V] = stmt.astID match {
        case Assignment.ASTID ⇒ stmt.asAssignment.expr
        case ExprStmt.ASTID   ⇒ stmt.asExprStmt.expr
        case _                ⇒ throw new UnknownError("Unexpected statement")
    }

    /**
     * Gets the set of all methods possibly called at some statement.
     *
     * @param basicBlock The basic block containing the statement.
     * @param index The statement's index.
     * @return All methods possibly called at the statement index or None, if the statement does not contain a call.
     */
    def getCalleesIfCallStatement(basicBlock: BasicBlock, index: Int)(implicit state: State): Option[SomeSet[Method]] = {
        val statement = state.code(index)
        val pc = statement.pc
        statement.astID match {
            case StaticMethodCall.ASTID | NonVirtualMethodCall.ASTID | VirtualMethodCall.ASTID ⇒ Some(getCallees(basicBlock, pc))
            case Assignment.ASTID | ExprStmt.ASTID ⇒ getExpression(statement).astID match {
                case StaticFunctionCall.ASTID | NonVirtualFunctionCall.ASTID | VirtualFunctionCall.ASTID ⇒
                    val cs = getCallees(basicBlock, pc)
                    if(cs.exists(_.name != getExpression(statement).asFunctionCall.name)){
                        println("strange")
                        getCallees(basicBlock, pc)
                    }
                    Some(getCallees(basicBlock, pc))
                case _ ⇒ None
            }
            case _ ⇒ None
        }
    }

    implicit val ps = project.get(PropertyStoreKey)

    /**
     * Gets the set of all methods possibly called at some call statement.
     *
     * @param basicBlock The basic block containing the call.
     * @param pc The call's program counter.
     * @return All methods possibly called at the statement index.
     */
    def getCallees(basicBlock: BasicBlock, pc: Int)(implicit state: State): SomeSet[Method] = {
        val FinalEP(_, callees) = ps(declaredMethods(state.method), Callees.key)
        definedMethods(callees.directCallees(pc))
    }

    /**
     * Maps some declared methods to their defined methods.
     *
     * @param declaredMethods Some declared methods.
     * @return All defined methods of `declaredMethods`.
     */
    def definedMethods(declaredMethods: Iterator[DeclaredMethod]): SomeSet[Method] = {
        val result = scala.collection.mutable.Set.empty[Method]
        declaredMethods.filter(declaredMethod ⇒ declaredMethod.hasSingleDefinedMethod || declaredMethod.hasMultipleDefinedMethods).foreach(declaredMethod ⇒
            declaredMethod.foreachDefinedMethod(defineMethod ⇒ result.add(defineMethod)))
        result
    }

    def reAnalyzeCalls(callSites: Set[(BasicBlock, Int)], callee: Method, fact: Option[DataFlowFact])(implicit state: State): Unit = {
        val queue: mutable.Queue[(BasicBlock, Set[DataFlowFact], Option[Int], Option[Method], Option[DataFlowFact])] =
            mutable.Queue.empty
        for ((block, index) ← callSites)
            queue.enqueue(
                (
                    block,
                    state.incomingFacts(block),
                    Some(index),
                    Some(callee),
                    fact
                )
            )
        process(queue)
    }

    /** See handleCallUpdate(e) */
    // This is a copy of handleCallUpdate(e) that loops over a set of updates more efficiently.
    def handleCallUpdates(es: Iterable[(DeclaredMethod, DataFlowFact)])(implicit state: State): Unit = {
        val queue: mutable.Queue[(BasicBlock, Set[DataFlowFact], Option[Int], Option[Method], Option[DataFlowFact])] =
            mutable.Queue.empty
        for (
            e ← es;
            blocks = state.pendingIfdsCallSites(e);
            (block, callSite) ← blocks
        ) queue.enqueue(
            (
                block,
                state.incomingFacts(block),
                Some(callSite),
                Some(e._1.definedMethod),
                Some(e._2)
            )
        )
        process(queue)
    }

    /**
     * Processes a statement with a call.
     *
     * @param basicBlock The basic block that contains the statement
     * @param call The call statement.
     * @param callees All possible callees of the call.
     * @param in The facts valid before the call statement.
     * @param calleeWithUpdateFact If present, the `callees` will only be analyzed with this fact instead of the facts returned by callFlow.
     * @return A map, mapping from each successor statement of the `call` to the facts valid at their start.
     */
    def handleCall(
        basicBlock:           BasicBlock,
        call:                 Statement,
        callees:              SomeSet[Method],
        in:                   Set[DataFlowFact],
        calleeWithUpdateFact: Option[DataFlowFact]
    )(implicit state: State): Map[Statement, Set[DataFlowFact]] = {
        val successors = successorStatements(call, basicBlock)
        // Facts valid at the start of each successor
        var summaryEdges: Map[Statement, Set[DataFlowFact]] = Map.empty

        // If calleeWithUpdateFact is present, this means that the basic block already has been analyzed with the `in` facts.
        if (calleeWithUpdateFact.isEmpty)
            for (successor ← successors) {
                summaryEdges += successor → propagateNullFact(in, callToReturnFlow(call, successor, in))
            }

        for (calledMethod ← callees) {
            val callee = declaredMethods(calledMethod)
            if (callee.definedMethod.isNative) {
                // We cannot analyze native methods. Let the concrete analysis decide what to do.
                for {
                    successor ← successors
                } summaryEdges += successor → (summaryEdges(successor) ++ nativeCall(call, callee, successor, in))
            } else {
                val callToStart =
                    if (calleeWithUpdateFact.isDefined) calleeWithUpdateFact.toSet
                    else propagateNullFact(in, callFlow(call, callee, in))
                var allNewExitFacts: Map[Statement, Set[DataFlowFact]] = Map.empty
                // Collect exit facts for each input fact separately
                for (fact ← callToStart) {
                    /*
          * If this is a recursive call with the same input facts, we assume that the call only produces the facts that are already known.
          * The call site is added to `pendingIfdsCallSites`, so that it will be re-evaluated if new output facts become known for the input fact.
          */
                    if ((calledMethod eq state.method) && fact == state.source._2) {
                        val newDependee =
                            state.pendingIfdsCallSites.getOrElse(state.source, Set.empty) + ((basicBlock, call.index))
                        state.pendingIfdsCallSites = state.pendingIfdsCallSites.updated(state.source, newDependee)
                        allNewExitFacts = mergeMaps(
                            allNewExitFacts,
                            mergeMaps(
                                collectResult(state.cfg.normalReturnNode),
                                collectResult(state.cfg.abnormalReturnNode)
                            )
                        )
                    } else {
                        val e = (callee, fact)
                        val c = cell(e)

                        val callFlows = if (c.isComplete)
                            FinalOutcome(c.getResult())
                        else
                            NextOutcome(c.getResult())

                        val oldValue = state.pendingIfdsDependees.get(c)

                        val oldExitFacts: Map[Statement, Set[DataFlowFact]] = oldValue match {
                            case Some(NextOutcome(p)) ⇒ p.flows
                            case _                    ⇒ Map.empty
                        }
                        val exitFacts: Map[Statement, Set[DataFlowFact]] = callFlows match {
                            case FinalOutcome(p) ⇒
                                state.pendingIfdsDependees -= c
                                p.flows
                            case NextOutcome(p) ⇒
                                val newDependee =
                                    state.pendingIfdsCallSites.getOrElse(e, Set.empty) + ((basicBlock, call.index))
                                state.pendingIfdsCallSites = state.pendingIfdsCallSites.updated(e, newDependee)
                                state.pendingIfdsDependees += c → callFlows
                                p.flows
                        }
                        // Only process new facts that are not in `oldExitFacts`
                        allNewExitFacts = mergeMaps(allNewExitFacts, mapDifference(exitFacts, oldExitFacts))
                        /*
                         * If new exit facts were discovered for the callee-fact-pair, all call sites depending on this pair have to be re-evaluated.
                         * oldValue is undefined if the callee-fact pair has not been queried before or returned a FinalEP.
                         */
                        if (oldValue.isDefined && oldExitFacts != exitFacts) {
                            reAnalyzeCalls(state.pendingIfdsCallSites(e), e._1.definedMethod, Some(e._2))
                        }
                    }
                }

                // Map facts valid on each exit statement of the callee back to the caller
                for {
                    successor ← successors if successor.node.isBasicBlock || successor.node.isNormalReturnExitNode
                    exitStatement ← allNewExitFacts.keys if exitStatement.stmt.astID == Return.ASTID ||
                        exitStatement.stmt.astID == ReturnValue.ASTID
                } summaryEdges += successor → (summaryEdges.getOrElse(successor, Set.empty[DataFlowFact]) ++
                    returnFlow(call, callee, exitStatement, successor, allNewExitFacts.getOrElse(exitStatement, Set.empty)))

                for {
                    successor ← successors if successor.node.isCatchNode || successor.node.isAbnormalReturnExitNode
                    exitStatement ← allNewExitFacts.keys if exitStatement.stmt.astID != Return.ASTID &&
                        exitStatement.stmt.astID != ReturnValue.ASTID
                } summaryEdges += successor → (summaryEdges.getOrElse(successor, Set.empty[DataFlowFact]) ++
                    returnFlow(call, callee, exitStatement, successor, allNewExitFacts.getOrElse(exitStatement, Set.empty)))
            }
        }
        summaryEdges
    }

    /**
     * Determines the successor statements for one source statement.
     *
     * @param statement The source statement.
     * @param basicBlock The basic block containing the source statement.
     * @return All successors of `statement`.
     */
    def successorStatements(statement: Statement, basicBlock: BasicBlock)(implicit state: State): Set[Statement] = {
        val index = statement.index
        if (index == basicBlock.endPC) for (successorBlock ← basicBlock.successors) yield firstStatement(successorBlock)
        else {
            val nextIndex = index + 1
            Set(Statement(statement.method, basicBlock, statement.code(nextIndex), nextIndex, statement.code, statement.cfg))
        }
    }

    /**
     * If `from` contains a null fact, it will be added to `to`.
     *
     * @param from The set, which may contain the null fact initially.
     * @param to The set, to which the null fact may be added.
     * @return `to` with the null fact added, if it is contained in `from`.
     */
    def propagateNullFact(from: Set[DataFlowFact], to: Set[DataFlowFact]): Set[DataFlowFact] = {
        val nullFact = from.find(_.isInstanceOf[AbstractIFDSNullFact])
        if (nullFact.isDefined) to + nullFact.get
        else to
    }

    /**
     * Merges two maps that have sets as values.
     *
     * @param map1 The first map.
     * @param map2 The second map.
     * @return A map containing the keys of both map. Each key is mapped to the union of both maps' values.
     */
    def mergeMaps[S, T](map1: Map[S, Set[T]], map2: Map[S, Set[T]]): Map[S, Set[T]] = {
        var result = map1
        for ((key, values) ← map2) {
            result = result.updated(key, result.getOrElse(key, Set.empty) ++ values)
        }
        result
    }

    /**
     * Computes the difference of two maps that have sets as their values.
     *
     * @param minuend The map, from which elements will be removed.
     * @param subtrahend The map, whose elements will be removed from `minuend`.
     * @return A map, containing the keys and values of `minuend`.
     *         The values of the result only contain those elements not present in `subtrahend` for the same key.
     */
    def mapDifference[S, T](minuend: Map[S, Set[T]], subtrahend: Map[S, Set[T]]): Map[S, Set[T]] = {
        var result = minuend
        for ((key, values) ← subtrahend) {
            result = result.updated(key, result(key) -- values)
        }
        result
    }

    /**
     * Gets the Call for a statement that contains a call (MethodCall Stmt or ExprStmt/Assigment
     * with FunctionCall)
     * @param stmt
     * @return
     */
    def asCall(stmt: Stmt[V]): Call[V] = stmt.astID match {
        case Assignment.ASTID ⇒ stmt.asAssignment.expr.asFunctionCall
        case ExprStmt.ASTID   ⇒ stmt.asExprStmt.expr.asFunctionCall
        case _                ⇒ stmt.asMethodCall
    }

    /**
     * Gets the first statement of a BasicBlock or the first statement of the handler BasicBlock of
     * a CatchNode.
     */
    @tailrec
    private def firstStatement(node: CFGNode)(implicit state: State): Statement = {
        if (node.isBasicBlock) {
            val index = node.asBasicBlock.startPC
            Statement(state.method, node, state.code(index), index, state.code, state.cfg)
        } else if (node.isCatchNode) {
            firstStatement(node.successors.head)
        } else if (node.isExitNode) {
            Statement(state.method, node, null, 0, state.code, state.cfg)
        } else throw new IllegalArgumentException(s"Unknown node type: $node")
    }

    /**
     * Retrieves and commits the methods result as calculated for its declaring class type for the
     * current DefinedMethod that represents the non-overwritten method in a subtype.
     */
    def baseMethodResult(source: (DeclaredMethod, DataFlowFact)): Outcome[IFDSProperty[DataFlowFact]] = {
        // set up a dependency and return that we do not have computed any information at this point.
        val (declaredMethod, sourceFact) = source
        val dm = declaredMethod.asDefinedMethod
        val thisCell = cell((dm, sourceFact))

        val decl = declaredMethods(source._1.definedMethod)
        val baseCell = cell((decl, sourceFact))
        thisCell.when(baseCell)(_.head._2.get)

        NoOutcome // we do not have any information yet but solely depend on the base class
    }

    val entryPoints: Map[DeclaredMethod, DataFlowFact]
}

object AbstractIFDSAnalysis {

    /** The type of the TAC domain. */
    type V = DUVar[ValueInformation]

    case class Statement(
            method: Method,
            node:   CFGNode,
            stmt:   Stmt[V],
            index:  Int,
            code:   Array[Stmt[V]],
            cfg:    CFG[Stmt[V], TACStmts[V]]
    ) {
        override def hashCode(): Int = method.hashCode() * 31 + index

        override def equals(o: Any): Boolean = {
            o match {
                case s: Statement ⇒ s.index == index && s.method == method
                case _            ⇒ false
            }
        }

        override def toString: String = s"${method.toJava}"
    }
}

