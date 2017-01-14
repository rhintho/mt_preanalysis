package de.beuth

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  def startTransformation(url: String, sensorType: String, targetPath: String): Unit = {
    // Parameter definieren
    val csvFormat = "com.databricks.spark.csv"

    // Spark initialisieren
    val conf = new SparkConf().setAppName("MT_PreAnalytics")
    val sc   = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Dataframe erzeugen
    val df = sqlContext.read
      .format(csvFormat)
      .option("header", "false")
//      .option("inferSchema", "true")
      .load(url)
    // Tabelle umbenennen
    df.registerTempTable("sensordata")

    // TODO Alle Sensor-IDs identifizieren

    // Filterabfrage an Dataframe ausführen
    val result = sqlContext.sql(getSQLQuery(sensorType))
    result.show() // Einblenden zur Überprüfung

    // neue CSV-Datei rausschreiben, durch überführen in eine RDD
    val rddResult = result.rdd.map(x => x.mkString(","))
    rddResult.saveAsTextFile(targetPath + "_" + sensorType)
  }

  def getSQLQuery(sensorType: String): String = {
    var sqlQuery = ""
    if (sensorType == "ABA") {
      sqlQuery = "SELECT C0, C1, C2, C3, C6 FROM sensordata"
    } else if (sensorType == "PZS") {
      sqlQuery = "SELECT C0, C1, C2, C6 FROM sensordata"
    }
    sqlQuery
  }

}
