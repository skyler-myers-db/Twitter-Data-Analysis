import Dependencies._

ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.revature"
ThisBuild / organizationName := "revature"

lazy val root = (project in file("."))
  .settings(
    name := "dogecoin-vs-bitcoin",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
    libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    libraryDependencies += "commons-io" % "commons-io" % "2.8.0"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
