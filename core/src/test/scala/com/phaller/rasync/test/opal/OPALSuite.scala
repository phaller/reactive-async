//package com.phaller.rasync
//package test
//package opal
//
//import org.scalatest.FunSuite
//
//import org.opalj.br.analyses.Project
//
//import java.io.File
//
//class OPALSuite extends FunSuite {
//
//  test("purity analysis with Demo.java: pure methods") {
//    val file = new File("core")
//    val lib = Project(file)
//
//    val report = PurityAnalysis.doAnalyze(lib, List.empty, () => false).toConsoleString.split("\n")
//
//    val pureMethods = List(
//      "pureness.Demo{ public static int pureThoughItUsesField(int,int) }",
//      "pureness.Demo{ public static int pureThoughItUsesField2(int,int) }",
//      "pureness.Demo{ public static int simplyPure(int,int) }",
//      "pureness.Demo{ static int foo(int) }",
//      "pureness.Demo{ static int bar(int) }",
//      "pureness.Demo{ static int fooBar(int) }",
//      "pureness.Demo{ static int barFoo(int) }",
//      "pureness.Demo{ static int m1(int) }",
//      "pureness.Demo{ static int m2(int) }",
//      "pureness.Demo{ static int m3(int) }",
//      "pureness.Demo{ static int cm1(int) }",
//      "pureness.Demo{ static int cm2(int) }",
//      "pureness.Demo{ static int scc0(int) }",
//      "pureness.Demo{ static int scc1(int) }",
//      "pureness.Demo{ static int scc2(int) }",
//      "pureness.Demo{ static int scc3(int) }")
//
//    val finalRes = pureMethods.filter(!report.contains(_))
//
//    assert(finalRes.size == 0, report.mkString("\n"))
//  }
//
//  test("purity analysis with Demo.java: impure methods") {
//    val file = new File("core")
//    val lib = Project(file)
//
//    val report = PurityAnalysis.doAnalyze(lib, List.empty, () => false).toConsoleString.split("\n")
//
//    val impureMethods = List(
//      "public static int impure(int)",
//      "static int npfoo(int)",
//      "static int npbar(int)",
//      "static int mm1(int)",
//      "static int mm2(int)",
//      "static int mm3(int)",
//      "static int m1np(int)",
//      "static int m2np(int)",
//      "static int m3np(int)",
//      "static int cpure(int)",
//      "static int cpureCallee(int)",
//      "static int cpureCalleeCallee1(int)",
//      "static int cpureCalleeCallee2(int)",
//      "static int cpureCalleeCalleeCallee(int)",
//      "static int cpureCalleeCalleeCalleeCallee(int)")
//
//    val finalRes = impureMethods.filter(report.contains(_))
//
//    assert(finalRes.size == 0)
//  }
//
//  /*test("ImmutabilityAnalysis: Concurrency") {
//    val file = new File("lib")
//    val lib = Project(file)
//
//    val manager = lib.get(FPCFAnalysesManagerKey)
//    manager.run(ClassExtensibilityAnalysis)
//    manager.runAll(
//      FieldMutabilityAnalysis
//    )
//
//    // Compare every next result received from the same analysis to `report`
//    val report = ImmutabilityAnalysis.analyzeWithoutClassExtensibilityAndFieldMutabilityAnalysis(lib, manager).toConsoleString.split("\n")
//
//    for (i <- 0 to 1000) {
//      // Next result
//      val newReport = ImmutabilityAnalysis.analyzeWithoutClassExtensibilityAndFieldMutabilityAnalysis(lib, manager).toConsoleString.split("\n")
//
//      // Differs between the elements in `report` and `newReport`.
//      // If they have the exact same elements, `finalRes` should be an
//      // empty list.
//      val finalRes = report.filterNot(newReport.toSet)
//
//      assert(finalRes.isEmpty)
//    }
//  }*/
//
//}
