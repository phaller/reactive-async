import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  lazy val opalCommon = "de.opal-project" %% "common" % "3.0.0-SNAPSHOT"
  lazy val opalTAC = "de.opal-project" %% "three-address-code" % "3.0.0-SNAPSHOT" % "test"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.9"
}
