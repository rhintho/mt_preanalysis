name := "MT_Preanalytics"

version := "0.1a"

scalaVersion := "2.11.8"

// Apache Spark (Core und Dataframe-/SQL-Support)
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11"  % "1.6.0" % "provided"

// Erweiterung zur Nutzung von CSV-Dateien
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "latest.integration"

// Testframework
libraryDependencies += "org.scalactic" % "scalactic_2.11" % "latest.integration"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "latest.integration" % "test"

// Plot-Funktion
libraryDependencies += "com.quantifind" % "wisp_2.11" % "latest.integration"
