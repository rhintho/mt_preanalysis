package de.beuth

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp

import de.beuth.unit.TimeSegment

/**
  * Created by Sebastian Urbanek on 14.01.17.
  */
object SensordataTransformator {

  var timeInterval: Int = 1

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("SensordataTransformator")
  log.setLevel(Level.DEBUG)

  def startTransformation(dataPath: String, sensorType: String, targetPath: String,
                          timeInterval: Int, gpsDataPath: String): Unit = {
    log.debug("Start der Analyse wird eingeleitet ...")
    // Datenformat definieren
    val csvFormat = "com.databricks.spark.csv"
    this.timeInterval = timeInterval

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
    sensorIds.collect.foreach(row => identifySingleSensor(row.getInt(0), sensorData,
                                                          targetPath, sensorType, sqlContext))
  }

  private def identifySingleSensor(sensorId: Int, sensorData: DataFrame,
                                   targetPath: String, sensorType: String, sQLContext: SQLContext): Unit = {
    val singleSensor = createSingleSensorDataframe(sensorData, sensorId)
    val avgVelocity = createSingleSensorForVelocityDataframe(sensorData, sensorId)
    val resultSensor = joinSingleSensorData(singleSensor, avgVelocity)
    resultSensor.show(100)
    saveResult(resultSensor, targetPath, sensorId, sensorType)
  }

  private def saveResult(result: DataFrame, targetPath: String, sensorId: Int, sensorType: String): Unit = {
    val rddResult = result.rdd.map(row => row.mkString(","))
    rddResult.saveAsTextFile(targetPath + "sensor_" + sensorType + "_" + sensorId)
    log.debug("Datei erfolgreich geschrieben (sensor_" + sensorType + "_" + sensorId + ")")
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
  private def udfCreateVelocityColumn = udf(() => 0.toDouble)
  private def udfCreateRegistrationColumn = udf(() => 0.toDouble)
  private def udfCalcCompleteness = udf((count:Int) => ((count.toDouble / this.timeInterval) * 100).round)
  private def udfCalcVelocity = udf((v1: String, v2: String) => {if (v2 == null) v1 else v2})

  private def identifyAllSensorIds(sqlContext: SQLContext, sensorData: DataFrame): DataFrame = {
    sensorData.select("sensor_id").distinct().orderBy("sensor_id")
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
      .groupBy("sensor_id", "timestamp", "latitude", "longitude", "temperature", "weather")
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
        "weather",
        "completeness"
      )

    joinedVelocity
      .withColumn("velocity", udfCalcVelocity(joinedVelocity("velocity"), joinedVelocity("v_velocity")))
      .select(
        "sensor_id",
        "timestamp",
        "registration",
        "velocity",
        "weather",
        "temperature",
        "latitude",
        "longitude",
        "completeness"
      )
      .orderBy("timestamp")
  }

  private def joinOriginDataWithGPSData(originData: DataFrame, gpsData: DataFrame): DataFrame = {
    gpsData
      .join(originData, originData("sensor_id").equalTo(gpsData("gps_sensor_id")))
      .select(
        "sensor_id",
        "timestamp",
        "registration",
        "velocity",
        "completeness",
        "weather",
        "temperature",
        "latitude",
        "longitude")
  }

  private def createOriginDataDataframe(sqlContext: SQLContext, csvFormat: String,
                                        url: String, sensorType: String): DataFrame = {
    if (sensorType == "PZS")
      createOriginDataframeFromPZS(sqlContext, csvFormat, url)
    else if (sensorType == "ABA")
      createOriginDataframeFromABA(sqlContext, csvFormat, url)
    else
      null
  }

  private def createOriginDataframeFromABA(sqlContext: SQLContext, csvFormat: String, url: String): DataFrame = {
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
      .withColumn("timestamp", udfAssignTimeSegment(sensorData("timestamp")))
      .withColumn("completeness", udfCreateErrorColumn())
      .withColumn("weather", udfCreateWeatherColumn())
      .withColumn("temperature", udfCreateTemperatureColumn())
      .withColumn("registration", udfCreateRegistrationColumn())
  }

  private def createOriginDataframeFromPZS(sqlContext: SQLContext, csvFormat: String, url: String): DataFrame = {
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
      .withColumn("completeness", udfCreateErrorColumn())
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
