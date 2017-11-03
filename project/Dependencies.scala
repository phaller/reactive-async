import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4" % "test"
  lazy val opal = "de.opal-project" % "abstract-interpretation-framework_2.12" % "1.0.0"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.8.2"
}
