import Dependencies._ // see project/Dependencies.scala
import Util._         // see project/Util.scala

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

val buildVersion = "0.1.0-SNAPSHOT"

def commonSettings = Seq(
  version in ThisBuild := buildVersion,
  scalaVersion := buildScalaVersion,
  parallelExecution in Test := false
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
    libraryDependencies += opal
  )
