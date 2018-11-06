import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  lazy val opalCommon = "de.opal-project" %% "common" % "2.1.0-SNAPSHOT"
  lazy val opalAI = "de.opal-project" %% "abstract-interpretation-framework" % "2.1.0-SNAPSHOT" % "test"
  lazy val opalBR = "de.opal-project" %% "bytecode-representation" % "2.1.0-SNAPSHOT" % "test"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.9"
}
