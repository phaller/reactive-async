import Dependencies._ // see project/Dependencies.scala
import Util._         // see project/Util.scala

val buildVersion = "0.1.0-SNAPSHOT"

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

lazy val lib: Project = (project in file("lib")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-lib",
    libraryDependencies += scalaTest,
    libraryDependencies += opal,
    libraryDependencies += opalFixpoint,
    scalacOptions += "-feature"
  )

lazy val Benchmark = config("bench") extend Test

lazy val bench: Project = (project in file("bench")).
  settings(commonSettings: _*).
  settings(
    name := "reactive-async-bench",
    libraryDependencies += scalaTest,
    libraryDependencies += opal,
    libraryDependencies += opalFixpoint,
    libraryDependencies += scalaMeter,
    testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
  ).configs(
    Benchmark
  ).settings(
    inConfig(Benchmark)(Defaults.testSettings): _*
  ).
  dependsOn(lib)
