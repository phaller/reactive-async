import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  lazy val opal = "de.opal-project" % "abstract-interpretation-framework_2.11" % "0.9.0-SNAPSHOT"
  lazy val opalFixpoint = "de.opal-project" % "fixpoint-computations-framework-analyses_2.11" % "0.9.0-SNAPSHOT"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.7"
}
