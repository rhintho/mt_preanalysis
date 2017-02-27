name := "MT_PreAnalysis"

version := "0.2a"

scalaVersion := "2.10.6"

// Apache Spark (Core und Dataframe-/SQL-Support)
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "1.6.0" % "provided"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.6"



// Erweiterung zur Nutzung von CSV-Dateien
libraryDependencies += "com.databricks" %% "spark-csv" % "latest.integration"

// Testframework
libraryDependencies += "org.scalactic" %% "scalactic" % "latest.integration"
libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"
