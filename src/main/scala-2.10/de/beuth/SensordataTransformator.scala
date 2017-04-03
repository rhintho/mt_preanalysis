package de.beuth

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp

import de.beuth.unit.TimeSegment
import de.beuth.util.WeatherAnalyzer

/**
  * Kapselung der Funktionen zur Voraggregation der Datenquellen.
  */
object SensordataTransformator {

  var timeInterval: Int = 1

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("SensordataTransformator")
  log.setLevel(Level.DEBUG)

  // Hauptfunktion setzt die Ereignisse in Gang und startet Spark
  def startTransformation(dataPath: String, sensorType: String, targetPath: String, timeInterval: Int,
                          gpsDataPath: String, temperatureDataPath: String, rainfallDataPath: String,
                          sensorId: Int): Unit = {
    log.debug("Start der Analyse wird eingeleitet ...")
    // Datenformat definieren
    val csvFormat = "com.databricks.spark.csv"
    // Allgemeine Werte setzen
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

    // Finales Dataframe erzeugen und auf Festplatte speichern
    val resultData = createResultData(sensorId, sensorData, sensorType, sqlContext)
    resultData.show(200)
    sensorData.unpersist()
    saveResultDataframe(resultData, targetPath, sensorId, sensorType)
  }

  // Erzeugt das finale Datenset zum Speichern
  private def createResultData(sensorId: Int, sensorData: DataFrame,
                               sensorType: String, sQLContext: SQLContext): DataFrame = {
    val singleSensor = createSingleSensorDataframe(sensorData, sensorId)
    val avgVelocity = createSingleSensorForVelocityDataframe(sensorData, sensorId)
    joinSingleSensorData(singleSensor, avgVelocity)
  }

  // Funktion zum Speichern der aggregierten Funktionen als CSV
  private def saveResultDataframe(result: DataFrame, targetPath: String, sensorId: Int, sensorType: String): Unit = {
    val rddResult = result.rdd.map(row => row.mkString(","))
    rddResult.saveAsTextFile(targetPath + "sensor_" + sensorType + "_" + sensorId)
    log.debug("Datei erfolgreich geschrieben (sensor_" + sensorType + "_" + sensorId + ")")
  }

  // Übrigbleibsel aus dem Entwicklungsprozess
  private def saveLIBSVM(libsvm: DataFrame, targetPath: String, sensorId: Int, sensorType: String): Unit = {
    val preRdd = libsvm.rdd
      .map(r => (
        r.getInt(4), 1 + ":" + r.getInt(0), 2 + ":" + r.getLong(1), 3 + ":" + r.getDouble(2), 4 + ":" + r.getString(3),
        5 + ":" + r.getDouble(5), 6 + ":" + r.getDouble(6), 7 + ":" + r.getDouble(7), 8 + ":" + r.getDouble(8),
        9 + ":" + r.getLong(9)
      ))
      .map(r => r.toString().replace("(", "").replace(")", ""). replace(",", " "))
    val rdd = preRdd
    rdd.saveAsTextFile(targetPath + "sensor_" + sensorType + "_" + sensorId + ".libsvm")
    log.debug("LIBSVM erfolgreich geschrieben (als sensor_" + sensorType + "_" + sensorId + ".libsvm)")
  }

  // Registrierung aller User Defined Function
  private def udfAssignTimeSegment = udf((timestamp: String) => assignTimeSegment(timestamp))
  private def udfCreateErrorColumn = udf(() => -1)
  private def udfCreateVelocityColumn = udf(() => 0.0)
  private def udfCreateRegistrationColumn = udf(() => 0.0)
  private def udfCalcCompleteness = udf((count:Int) => ((count.toDouble / this.timeInterval) * 100).round)
  private def udfCalcVelocity = udf((v1: String, v2: String) => {if (v2 == null) v1 else v2})
  private def udfTransformTimestampToUnixtime = udf((timestamp: Timestamp) => timestamp.getTime)

  private def identifyAllSensorIds(sqlContext: SQLContext, sensorData: DataFrame): DataFrame = {
    sensorData.select("sensor_id").distinct().orderBy("sensor_id")
  }

  // Funktion zur Zuordnung eines Zeitstempel zu einem Zeitabschnitt
  private def assignTimeSegment(timestamp: String): Timestamp = {
    val ts = Timestamp.valueOf(timestamp)
    TimeSegment.getTimestampSegment(ts)
  }

  // Übrigbleibsel
  private def transformIntoLIBSVM(sqlContext: SQLContext, sensorData: DataFrame): DataFrame = {
    sensorData
      .withColumn("timestamp", udfTransformTimestampToUnixtime(sensorData("timestamp")))
  }

  // Funktion zur Erzeugung des DataFrames mit den aggregierten Geschwindigkeitswerten
  private def createSingleSensorForVelocityDataframe(sensorData: DataFrame, sensorId: Int): DataFrame = {
    sensorData
//      .filter(sensorData("sensor_id").equalTo(sensorId))
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

  // Funktion zur Aggregation des Belegung und Vollständigkeit
  private def createSingleSensorDataframe(sensorData: DataFrame, sensorId: Int): DataFrame = {
    val singleSensor = sensorData
//      .filter(sensorData.col("sensor_id").equalTo(sensorId))
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

  //Funktion für JOIN der aggregierten Daten mit den aggregierten Geschwindigkeitsdaten
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

  // Funktion für JOIN für GPS und Wetter Daten
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

  // Funktion zur Unterscheidung des Ausgangdatentype
  private def createOriginDataDataframe(sqlContext: SQLContext, csvFormat: String,
                                        url: String, sensorType: String, sensorId: Int): DataFrame = {
    if (sensorType == "PZS")
      createOriginDataframeFromPZS(sqlContext, csvFormat, url, sensorId)
    else if (sensorType == "ABA")
      createOriginDataframeFromABA(sqlContext, csvFormat, url, sensorId)
    else
      null
  }

  // Funktion zur Erzeugung eines DataFrame für den ABA-Typ
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

  // Funktion zur Erzeugung eines DataFrame für den PZS-Typ
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

  // Funktion zur Erzeugung eines DataFrames für die GPS Koordinaten
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
