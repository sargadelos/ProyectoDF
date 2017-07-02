
import sbt._
import sbt.Keys._

lazy val allResolvers = Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
)

val sparkVersion = "2.1.0"
val sparkCassandraVersion = "1.6.0"

lazy val AllLibraryDependencies =
  Seq(
    "org.scala-lang" % "scala-compiler" % "2.11.8",
    "org.apache.curator" % "curator-framework" % "2.6.0",
    "org.apache.curator" % "curator-recipes" % "2.6.0",
    "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",
    "com.typesafe.akka" %% "akka-actor" % "2.4.8",
    "com.typesafe.akka" %% "akka-remote" % "2.4.17",
    "com.typesafe.akka" %% "akka-cluster" % "2.4.17",
    "com.typesafe.akka" %% "akka-cluster-tools" % "2.4.17",
    "com.typesafe.akka" %% "akka-contrib" % "2.4.8",
    ("org.apache.spark" %% "spark-core" % sparkVersion)
      .exclude("commons-beanutils", "commons-beanutils")
      .exclude("commons-beanutils", "commons-beanutils-core")
      .exclude("javax.inject", "javax.inject")
      .exclude("aopalliance", "aopalliance")
      .exclude("org.apache.hadoop", "hadoop-yarn-common")
      .exclude("org.apache.hadoop", "hadoop-yarn-api")
      .exclude("org.spark-project.spark", "unused"),
   ("org.apache.spark" %% "spark-sql" % sparkVersion)
      .exclude("commons-beanutils", "commons-beanutils")
      .exclude("commons-beanutils", "commons-beanutils-core")
      .exclude("org.apache.hadoop", "hadoop-yarn-common")
      .exclude("javax.inject", "javax.inject")
      .exclude("aopalliance", "aopalliance")
      .exclude("org.apache.hadoop", "hadoop-yarn-api")
      .exclude("org.spark-project.spark", "unused")
    //    "com.datastax.spark" % "spark-cassandra-connector_2.11" % sparkCassandraVersion  )
  )

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("org","codehaus", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.last
  case PathList("commons-beanutils", "commons-beanutils", xs @ _*) => MergeStrategy.last
  case PathList("commons-cli", "commons-cli", xs @ _*) => MergeStrategy.last
  case PathList("commons-collections", "commons-collections", xs @ _*) => MergeStrategy.last
  case PathList("commons-net", "commons-net",xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

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
    name := "nodoDF",
    mainClass in (Compile, run) := Some("proyectoDF.cluster.nodo.nodoDataFederationApp")
  )
  .aggregate(comun)
  .dependsOn(comun)

