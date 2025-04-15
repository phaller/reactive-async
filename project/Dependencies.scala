import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.9" % "test"
  lazy val opalCommon = "de.opal-project" %% "common" % "5.0.0"
  lazy val opalTAC = "de.opal-project" %% "three-address-code" % "5.0.0" % "test"
  lazy val scalaMeter = "com.storm-enroute" %% "scalameter" % "0.21"
}
