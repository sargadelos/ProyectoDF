import sbt._
import sbt.Keys._


lazy val allResolvers = Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
)

val sparkVersion = "2.0.0"
val sparkCassandraVersion = "1.6.0"

lazy val AllLibraryDependencies =
  Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.4.17",
    "com.typesafe.akka" %% "akka-remote" % "2.4.17",
    "com.typesafe.akka" %% "akka-cluster" % "2.4.17",
    "com.typesafe.akka" %% "akka-cluster-tools" % "2.4.17",
    "com.typesafe.akka" %% "akka-contrib" % "2.4.17",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % sparkCassandraVersion
  )


lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  resolvers := allResolvers,
  libraryDependencies := AllLibraryDependencies
)


lazy val raiz = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "raiz"
  )
  .aggregate(comun, nodoDF)
  .dependsOn(comun, nodoDF)

lazy val comun = (project in file("comun")).
  settings(commonSettings: _*).
  settings(
    name := "comun"
  )

lazy val nodoDF = (project in file("nodoDF")).
  settings(commonSettings: _*).
  settings(
    name := "nodoDF"
  )
  .aggregate(comun)
  .dependsOn(comun)

