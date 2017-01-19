package de.beuth

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
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

  def startTransformation(dataPath: String, sensorType: String, targetPath: String,
                          timeInterval: Int, gpsDataPath: String): Unit = {
    log.debug("Start der Analyse wird eingeleitet ...")
    // Datenformat definieren
    val csvFormat = "com.databricks.spark.csv"

    // Spark initialisieren mit dem SQL-Kontext
    val conf = new SparkConf().setAppName("MT_PreAnalytics")
    val sc  = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Dataframes erzeugen
    val originData = createOriginDataDataframe(sqlContext, csvFormat, dataPath, sensorType)
    val gpsData = createGPSReferenceDataframe(sqlContext, csvFormat, gpsDataPath)
    // Beide Dataframes miteinander kombinieren, um jedem Sensor seine GPS-Koordinaten zuzuordnen
    val sensorData = joinOriginDataWithGPSData(originData, gpsData)
    // Ursprüngliche Dataframe danach nicht mehr von Nutzen
    originData.unpersist()
    gpsData.unpersist()

    // Identifizieren aller vorkommenden Sensor-IDs
    val sensorIds = identifyAllSensorIds(sqlContext, sensorData)

    sensorIds.collect.foreach(row => identifySingleSensor(row.getInt(0), sensorData, targetPath))
  }

  private def identifySingleSensor(sensorId: Int, sensorData: DataFrame, targetPath: String): Unit = {
    val singleSensor = sensorData.filter(sensorData.col("sensor_id").equalTo(sensorId))
    singleSensor.show(40)

    // TODO Aggregation der Zeit und Fehler noch implementieren

//    val rddSingleSensor = singleSensor.rdd.map(row => row.mkString(","))
//    rddSingleSensor.saveAsTextFile(targetPath + "sensor_" + sensorId)
//    log.debug("Datei erfolgreich geschrieben (sensor_" + sensorId + ")")
  }

  private def assignTimeSegment(timestamp: String): Timestamp = {
    val ts = Timestamp.valueOf(timestamp)
    TimeSegment.getTimestampSegment(ts)
  }
  // Registrierung als User Defined Function
  private def udfAssignTimeSegment = udf((timestamp: String) => assignTimeSegment(timestamp))

  private def udfCreateErrorColumn = udf(() => -1)
  private def udfCreateWeatherColumn = udf(() => "sunshine")
  private def udfCreateTemperatureColumn = udf(() => 27)

  private def identifyAllSensorIds(sqlContext: SQLContext, sensorData: DataFrame): DataFrame = {
    sensorData.select("sensor_id").distinct().orderBy("sensor_id")
  }

  private def joinOriginDataWithGPSData(originData: DataFrame, gpsData: DataFrame): DataFrame = {
    gpsData
      .join(originData, originData("sensor_id").equalTo(gpsData("gps_sensor_id")))
      .select(
        "sensor_id",
        "timestamp",
        "registration",
        "velocity",
        "error",
        "weather",
        "temperature",
        "latitude",
        "longitude")
  }

  private def createOriginDataDataframe(sqlContext: SQLContext, csvFormat: String,
                                        url: String, sensorType: String): DataFrame = {
    val sensorData = sqlContext
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

    sensorData
      .withColumn("timestamp", udfAssignTimeSegment(sensorData("timestamp")))
      .withColumn("error", udfCreateErrorColumn())
      .withColumn("weather", udfCreateWeatherColumn())
      .withColumn("temperature", udfCreateTemperatureColumn())
  }

  private def createGPSReferenceDataframe(sqlContext: SQLContext, csvFormat: String, url: String): DataFrame = {
    sqlContext
      .read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Tabelle und Spalten umbenennen
      .withColumnRenamed("C0", "gps_sensor_id")
      .withColumnRenamed("C1", "latitude")
      .withColumnRenamed("C2", "longitude")
  }
}
