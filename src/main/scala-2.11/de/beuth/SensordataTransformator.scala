package de.beuth

import org.apache.log4j.{Level, LogManager, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.sql.Timestamp

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger(SensordataTransformator.getClass)
  log.setLevel(Level.DEBUG)

  val startTimestamp: Timestamp = Timestamp.valueOf("2009-01-01 00:00:00")
  val endTimestamp: Timestamp   = Timestamp.valueOf("2014-12-31 23:45:00")

  def startTransformation(dataPath: String, sensorType: String, targetPath: String, gpsDataPath: String): Unit = {
    log.debug("Start der Analyse wird eingeleitet ...")
    // Datenformat definieren
    val csvFormat = "com.databricks.spark.csv"

    // Spark initialisieren mit dem SQL-Kontext
    val conf = new SparkConf().setAppName("MT_PreAnalytics")
    val sc   = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Dataframes erzeugen
//    val originData = createOriginDataDF(sqlContext, csvFormat, dataPath, sensorType)
//    val gpsData = createGPSReferenceDF(sqlContext, csvFormat, gpsDataPath)

    // Identifizieren aller vorkommenden Sensor-IDs
//    val sensorIds = identifyAllSensorIds(sqlContext, originData)

    // neue CSV-Datei rausschreiben, durch überführen in eine RDD
//    val rddResult = result.rdd.map(x => x.mkString(","))
//    rddResult.saveAsTextFile(targetPath + "_" + sensorType)
  }

  private def identifyAllSensorIds(sqlContext: SQLContext, originData: DataFrame): DataFrame = {
    val sensorIds = sqlContext.sql("SELECT DISTINCT sensor_id FROM sensordata ORDER BY sensor_id ASC")
    sensorIds.registerTempTable("sensor_ids")
    sensorIds
  }

  private def createOriginDataDF(sqlContext: SQLContext, csvFormat: String,
                                 url: String, sensorType: String): DataFrame = {
    val originData = sqlContext.read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
    // Nur relevante Spalten auswählen
    val shortendData = originData.select("C0", "C1", "C2", "C6")
    // Spalten benennen
    val columnNames = Seq("sensor_id", "timestamp", "registration", "velocity")
    val renamedData = shortendData.toDF(columnNames: _*)
    renamedData.registerTempTable("sensordata")
    renamedData
  }

  private def createGPSReferenceDF(sqlContext: SQLContext, csvFormat: String, url: String): DataFrame = {
    val gpsData = sqlContext.read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
    // Tabelle und Spalten umbenennen
    val columnNames = Seq("sensor_id", "latitude", "longitude")
    val renamedData = gpsData.toDF(columnNames: _*)
    renamedData.registerTempTable("gpsdata")
    renamedData
  }
}
