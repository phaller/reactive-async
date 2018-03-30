import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  lazy val opalCommon = "de.opal-project" %% "common" % "1.0.0"
  lazy val opalAI = "de.opal-project" %% "abstract-interpretation-framework" % "1.0.0"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.9"
}
