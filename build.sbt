import Dependencies._ // see project/Dependencies.scala
import Util._         // see project/Util.scala

val buildVersion = "0.2.1-SNAPSHOT"
organization in ThisBuild := "com.phaller"
licenses in ThisBuild += ("BSD 2-Clause", url("http://opensource.org/licenses/BSD-2-Clause"))

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
    libraryDependencies += opalAI,
    libraryDependencies += opalBR,
    scalacOptions += "-feature"
  )

lazy val npv: Project = (project in file("monte-carlo-npv")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-npv",
    scalacOptions += "-feature",
    skip in publish := true
  ).
  dependsOn(core)

lazy val Benchmark = config("bench") extend Test

lazy val bench: Project = (project in file("bench")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-bench",
    libraryDependencies += scalaTest,
    libraryDependencies += opalCommon,
//    libraryDependencies += opalAI % Test,
    libraryDependencies += scalaMeter,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
    skip in publish := true
  ).configs(
    Benchmark
  ).settings(
    inConfig(Benchmark)(Defaults.testSettings): _*
  ).
  dependsOn(core)

javaOptions in ThisBuild ++= Seq("-Xmx27G", "-Xms1024m", "-XX:ThreadStackSize=2048")
