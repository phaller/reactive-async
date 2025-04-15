import Dependencies._ // see project/Dependencies.scala
import Version._ // see project/Version.scala

val buildVersion = "0.2.2-SNAPSHOT"
ThisBuild / organization := "com.phaller"
ThisBuild / licenses += ("BSD 2-Clause", url("http://opensource.org/licenses/BSD-2-Clause"))

def commonSettings = Seq(
  ThisBuild / version := buildVersion,
  scalaVersion := buildScalaVersion,
  logBuffered := false,
  Test / parallelExecution := false,
  ThisBuild / resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

def noPublish = Seq(
  publish := {},
  publishLocal := {}
)

lazy val core: Project = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "reactive-async",
    libraryDependencies += scalaTest,
    libraryDependencies += opalCommon,
    libraryDependencies += opalTAC,
    scalacOptions += "-feature"
  )

lazy val npv: Project = (project in file("monte-carlo-npv"))
  .settings(commonSettings: _*)
  .settings(
    name := "reactive-async-npv",
    scalacOptions += "-feature",
    publish / skip := true
  )
  .dependsOn(core)

lazy val Benchmark = config("bench") extend Test

lazy val bench: Project = (project in file("bench"))
  .settings(commonSettings: _*)
  .settings(
    name := "reactive-async-bench",
    libraryDependencies += scalaTest,
    libraryDependencies += opalCommon,
//    libraryDependencies += opalAI % Test,
    libraryDependencies += scalaMeter,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    publish / skip := true
  )
  .configs(
    Benchmark
  )
  .settings(
    inConfig(Benchmark)(Defaults.testSettings): _*
  )
  .dependsOn(core)

ThisBuild / javaOptions ++= Seq("-Xmx27G", "-Xms1024m", "-XX:ThreadStackSize=2048")
