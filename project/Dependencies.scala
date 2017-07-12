import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  lazy val opal = "de.opal-project" % "abstract-interpretation-framework_2.11" % "0.8.14"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.7"
}
