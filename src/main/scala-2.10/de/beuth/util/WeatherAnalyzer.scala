package de.beuth.util

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Sebastian Urbanek on 22.01.17.
  */
object WeatherAnalyzer {

  def createWeatherDataframe(sqlContext: SQLContext, temperatureDataPath: String, rainfallDataPath: String,
                             csvFormat: String): DataFrame = {
    val temperatureData = createTemperatureDataframe(sqlContext, temperatureDataPath, csvFormat)
    val rainfallData = createRainfallDataframe(sqlContext, rainfallDataPath, csvFormat)

    temperatureData
      .join(rainfallData, temperatureData("td_timestamp").equalTo(rainfallData("rfd_timestamp")))
      .select("td_timestamp", "temperature", "rainfall")
      .withColumnRenamed("td_timestamp", "weather_timestamp")
      .orderBy("weather_timestamp")
  }

  private def createTemperatureDataframe(sqlContext: SQLContext, url: String, csvFormat: String): DataFrame = {
    sqlContext
      .read
      .format(csvFormat)
      .option("header", "true")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Nur relevante Spalten auswählen und signifikant benennen
      .select("Datum", "Wert")
      .withColumnRenamed("Datum", "td_timestamp")
      .withColumnRenamed("Wert", "temperature")
  }

  private def createRainfallDataframe(sqlContext: SQLContext, url: String, csvFormat: String): DataFrame = {
    sqlContext
      .read
      .format(csvFormat)
      .option("header", "true")
      .option("inferSchema", "true")  // automatisches Erkennen der Datentypen
      .load(url)
      // Nur relevante Spalten auswählen und signifikant benennen
      .select("Datum", "Wert")
      .withColumnRenamed("Datum", "rfd_timestamp")
      .withColumnRenamed("Wert", "rainfall")
  }
}
