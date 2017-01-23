package de.beuth

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp

import de.beuth.unit.TimeSegment
import de.beuth.util.WeatherAnalyzer

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  var timeInterval: Int = 1

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("SensordataTransformator")
  log.setLevel(Level.DEBUG)

  def startTransformation(dataPath: String, sensorType: String, targetPath: String, timeInterval: Int,
                          gpsDataPath: String, temperatureDataPath: String, rainfallDataPath: String,
                          sensorId: Int): Unit = {
    log.debug("Start der Analyse wird eingeleitet ...")
    // Datenformat definieren
    val csvFormat = "com.databricks.spark.csv"
    this.timeInterval = timeInterval

    // Spark initialisieren mit dem SQL-Kontext
    val conf = new SparkConf().setAppName("MT_PreAnalytics")
    val sc  = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Dataframes erzeugen
    val originData = createOriginDataDataframe(sqlContext, csvFormat, dataPath, sensorType, sensorId)
    val gpsData = createGPSReferenceDataframe(sqlContext, csvFormat, gpsDataPath)
    val weatherData = WeatherAnalyzer.createWeatherDataframe(sqlContext, temperatureDataPath,
                                                             rainfallDataPath, csvFormat)

    // Beide Dataframes miteinander kombinieren, um jedem Sensor seine GPS-Koordinaten zuzuordnen
    val sensorData = joinWithGPSAndWeatherData(originData, gpsData, weatherData)

    // Ursprüngliche Dataframe danach nicht mehr von Nutzen
    originData.unpersist()
    gpsData.unpersist()
    weatherData.unpersist()

    // Identifizieren aller vorkommenden Sensor-IDs
//    val sensorIds = identifyAllSensorIds(sqlContext, sensorData)
//    sensorIds.collect.foreach(row => identifySingleSensor(row.getInt(0), sensorData,
//                                                          targetPath, sensorType, sqlContext))
    identifySingleSensor(sensorId, sensorData, targetPath, sensorType, sqlContext)
  }

  private def identifySingleSensor(sensorId: Int, sensorData: DataFrame,
                                   targetPath: String, sensorType: String, sQLContext: SQLContext): Unit = {
    val singleSensor = createSingleSensorDataframe(sensorData, sensorId)
    val avgVelocity = createSingleSensorForVelocityDataframe(sensorData, sensorId)
    val resultSensor = joinSingleSensorData(singleSensor, avgVelocity)
    saveResult(resultSensor, targetPath, sensorId, sensorType)
  }

  private def saveResult(result: DataFrame, targetPath: String, sensorId: Int, sensorType: String): Unit = {
    val rddResult = result.rdd.map(row => row.mkString(","))
    rddResult.saveAsTextFile(targetPath + "sensor_" + sensorType + "_" + sensorId)
    log.debug("Datei erfolgreich geschrieben (sensor_" + sensorType + "_" + sensorId + ")")
  }

  // Registrierung aller User Defined Function
  private def udfAssignTimeSegment = udf((timestamp: String) => assignTimeSegment(timestamp))
  private def udfCreateErrorColumn = udf(() => -1)
  private def udfCreateVelocityColumn = udf(() => 0.0)
  private def udfCreateRegistrationColumn = udf(() => 0.0)
  private def udfCalcCompleteness = udf((count:Int) => ((count.toDouble / this.timeInterval) * 100).round)
  private def udfCalcVelocity = udf((v1: String, v2: String) => {if (v2 == null) v1 else v2})

  private def identifyAllSensorIds(sqlContext: SQLContext, sensorData: DataFrame): DataFrame = {
    sensorData.select("sensor_id").distinct().orderBy("sensor_id")
  }

  private def assignTimeSegment(timestamp: String): Timestamp = {
    val ts = Timestamp.valueOf(timestamp)
    TimeSegment.getTimestampSegment(ts)
  }

  private def createSingleSensorForVelocityDataframe(sensorData: DataFrame, sensorId: Int): DataFrame = {
    sensorData
      .filter(sensorData("sensor_id").equalTo(sensorId))
      .select("sensor_id", "timestamp", "velocity")
      .filter(sensorData("velocity").gt(0))
      .groupBy("sensor_id", "timestamp")
      .agg(Map(
        "velocity" -> "avg"
      ))
      .withColumnRenamed("avg(velocity)", "v_velocity")
      .withColumnRenamed("sensor_id", "v_sensor_id")
      .withColumnRenamed("timestamp", "v_timestamp")
  }

  private def createSingleSensorDataframe(sensorData: DataFrame, sensorId: Int): DataFrame = {
    val singleSensor = sensorData
      .filter(sensorData.col("sensor_id").equalTo(sensorId))
      .groupBy("sensor_id", "timestamp", "latitude", "longitude", "temperature", "rainfall")
      .agg(Map(
        "registration" -> "sum",
        "completeness" -> "count"
      ))
      .withColumnRenamed("sum(registration)", "registration")
      .withColumnRenamed("count(completeness)", "completeness")

    singleSensor
      .withColumn("completeness", udfCalcCompleteness(singleSensor("completeness")))
      .withColumn("velocity", udfCreateVelocityColumn())
  }

  private def joinSingleSensorData(singleSensor: DataFrame, velocity: DataFrame): DataFrame = {
    val joinedVelocity = singleSensor
      .join(velocity, singleSensor("timestamp").equalTo(velocity("v_timestamp")), "left_outer")
      .select(
        "sensor_id",
        "timestamp",
        "velocity",
        "v_velocity",
        "registration",
        "latitude",
        "longitude",
        "temperature",
        "rainfall",
        "completeness"
      )

    joinedVelocity
      .withColumn("velocity", udfCalcVelocity(joinedVelocity("velocity"), joinedVelocity("v_velocity")))
      .select(
        "sensor_id",
        "timestamp",
        "registration",
        "velocity",
        "rainfall",
        "temperature",
        "latitude",
        "longitude",
        "completeness"
      )
      .orderBy("timestamp")
  }

  private def joinWithGPSAndWeatherData(originData: DataFrame, gpsData: DataFrame,
                                        weatherData: DataFrame): DataFrame = {
    val stage = gpsData
      .join(originData, originData("sensor_id").equalTo(gpsData("gps_sensor_id")))

    stage
      .join(weatherData,
            stage("timestamp").substr(0, 13).equalTo(weatherData("weather_timestamp").substr(0, 13)),
            "left_outer")
      .select(
        "sensor_id",
        "timestamp",
        "registration",
        "velocity",
        "completeness",
        "latitude",
        "longitude",
        "temperature",
        "rainfall"
      )
  }

  private def createOriginDataDataframe(sqlContext: SQLContext, csvFormat: String,
                                        url: String, sensorType: String, sensorId: Int): DataFrame = {
    if (sensorType == "PZS")
      createOriginDataframeFromPZS(sqlContext, csvFormat, url, sensorId)
    else if (sensorType == "ABA")
      createOriginDataframeFromABA(sqlContext, csvFormat, url, sensorId)
    else
      null
  }

  private def createOriginDataframeFromABA(sqlContext: SQLContext, csvFormat: String, url: String,
                                           sensorId: Int): DataFrame = {
    val sensorData = sqlContext
      .read
      .format(csvFormat)
      .option("header", "false")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Nur relevante Spalten auswählen und signifikant benennen
      .select("C0", "C1", "C6")
      .withColumnRenamed("C0", "sensor_id")
      .withColumnRenamed("C1", "timestamp")
      .withColumnRenamed("C6", "velocity")

    sensorData
      .filter(sensorData("sensor_id").equalTo(sensorId))
      .withColumn("timestamp", udfAssignTimeSegment(sensorData("timestamp")))
      .withColumn("completeness", udfCreateErrorColumn())
      .withColumn("registration", udfCreateRegistrationColumn())
  }

  private def createOriginDataframeFromPZS(sqlContext: SQLContext, csvFormat: String, url: String,
                                           sensorId: Int): DataFrame = {
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
      .filter(sensorData("sensor_id").equalTo(sensorId))
      .withColumn("timestamp", udfAssignTimeSegment(sensorData("timestamp")))
      .withColumn("completeness", udfCreateErrorColumn())
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
