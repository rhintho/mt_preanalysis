package de.beuth

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}
import org.apache.spark.sql.UDFRegistration
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp

import de.beuth.unit.TimeSegment

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("SensordataTransformator")
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

    def udf1 = udf((timestamp: String) => assignTimeSegment(timestamp))

    // Dataframes erzeugen
    val originData = createOriginDataDF(sqlContext, csvFormat, dataPath, sensorType)
//      .registerTempTable("originData")
//    val gpsData = createGPSReferenceDF(sqlContext, csvFormat, gpsDataPath)

    val oneSensor = originData
      .filter(originData("sensor_id").equalTo(281))
      .select("sensor_id", "timestamp", "registration", "velocity")
      .withColumn("timestamp", udf1(originData("timestamp")))
//    val oneSensor = sqlContext.sql("SELECT sensor_id, udf1(timestamp) FROM originData")
    oneSensor.show(40)


    // Identifizieren aller vorkommenden Sensor-IDs
//    val sensorIds = identifyAllSensorIds(sqlContext, originData)

    // neue CSV-Datei rausschreiben, durch überführen in eine RDD
//    val rddResult = result.rdd.map(x => x.mkString(","))
//    rddResult.saveAsTextFile(targetPath + "_" + sensorType)
  }

//  // Registrierung aller UserDefinedFunctions
//  def udfAssignTimeSegment(timestamp: Timestamp): UserDefinedFunction = {
//    udf((timestamp: Timestamp) => assignTimeSegment(timestamp))
//  }

  private def assignTimeSegment(timestamp: String): Timestamp = {
    val ntimestamp = Timestamp.valueOf(timestamp)
    TimeSegment.getTimestampSegment(ntimestamp)
  }

  private def identifyAllSensorIds(sqlContext: SQLContext, originData: DataFrame): DataFrame = {
    val sensorIds = sqlContext.sql("SELECT DISTINCT sensor_id FROM sensordata ORDER BY sensor_id ASC")
    sensorIds.registerTempTable("sensor_ids")
    sensorIds
  }

  private def createOriginDataDF(sqlContext: SQLContext, csvFormat: String,
                                 url: String, sensorType: String): DataFrame = {
    sqlContext
      .read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Nur relevante Spalten auswählen und signifikant benennen
      .select("C0", "C1", "C2", "C6")
      .withColumnRenamed("C0", "sensor_id")
      .withColumnRenamed("C1", "timestamp")
      .withColumnRenamed("C2", "registration")
      .withColumnRenamed("C6", "velocity")
  }

  private def createGPSReferenceDF(sqlContext: SQLContext, csvFormat: String, url: String): DataFrame = {
    sqlContext
      .read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Tabelle und Spalten umbenennen
      .withColumnRenamed("C0", "sensor_id")
      .withColumnRenamed("C1", "latitude")
      .withColumnRenamed("C2", "longitude")
  }
}
