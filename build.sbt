ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.11"

lazy val root = (project in file("."))
  .settings(
    name := "learnSparkScala"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"

