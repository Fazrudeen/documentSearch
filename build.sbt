// // project settings
name := "documentSearch"

// PLEASE NOTE: when changing the version number, also update version number in
// the file scripts/edge_archive.sh
version := "1.1.15-SNAPSHOT"

scalaVersion := "2.11.8"

// dependency settings
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.1"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5"
//libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "5.1.0"


//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "0.12.0"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

// // assembly settings
mainClass in assembly := Some("tgt.marketing.casestudy.documentSearch")
assemblyJarName := s"${name.value}_2.11-${version.value}.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "sevlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifact in (Compile, assembly) ~= { art =>
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)