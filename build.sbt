import Dependencies._ // see project/Dependencies.scala
import Util._         // see project/Util.scala

val buildVersion = "0.2.0-SNAPSHOT"
organization in ThisBuild := "com.phaller"

def commonSettings = Seq(
  version in ThisBuild := buildVersion,
  scalaVersion := buildScalaVersion,
  logBuffered := false,
  parallelExecution in Test := false,
  resolvers in ThisBuild += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

def noPublish = Seq(
  publish := {},
  publishLocal := {}
)

lazy val core: Project = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async",
    libraryDependencies += scalaTest,
    libraryDependencies += opalCommon,
    libraryDependencies += opalAI % Test,
    scalacOptions += "-feature"
  )

lazy val npv: Project = (project in file("monte-carlo-npv")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-npv",
    scalacOptions += "-feature"
  ).
  dependsOn(core)

lazy val Benchmark = config("bench") extend Test

lazy val bench: Project = (project in file("bench")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-bench",
    libraryDependencies += scalaTest,
    libraryDependencies += opalCommon,
    libraryDependencies += opalAI % Test,
    libraryDependencies += scalaMeter,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
  ).configs(
    Benchmark
  ).settings(
    inConfig(Benchmark)(Defaults.testSettings): _*
  ).
  dependsOn(core)
