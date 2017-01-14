package de.beuth

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  def startTransformation(): Unit = {
    // Spark initialisieren
    val conf = new SparkConf().setAppName("MT_PreAnalytics")
    val sc   = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Dataframe erzeugen
    val df = sqlContext.read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")
      .load(url)
    // Tabelle umbenennen
    df.registerTempTable("sensordata")

    // Filterabfrage an Dataframe ausführen
    val result = sqlContext.sql(getSQLQuery(sensorType, sensorId))
    result.show() // Einblenden zur Überprüfung
    println("Total: " + result.count())   // Zeilenanzahl anzeigen

    // neue CSV-Datei rausschreiben, durch überführen in eine RDD
    val rddResult = result.rdd.map(x => x.mkString(","))
    rddResult.saveAsTextFile(targetPath + "_" + sensorType + "_" + sensorId)
  }

  def getSQLQuery(sensorType: String, sensorId: String): String = {
    var sqlQuery = ""
    if (sensorType == "ABA") {
      sqlQuery = "SELECT C0, C1, C2, C3, C6 FROM sensordata WHERE C0 = '" + sensorId + "'"
    } else if (sensorType == "PZS") {
      sqlQuery = "SELECT C0, C1, C2, C6 FROM sensordata WHERE C0 = '" + sensorId + "'"
    }
    sqlQuery
  }

}
