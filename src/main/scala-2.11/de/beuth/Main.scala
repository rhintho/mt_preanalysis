package de.beuth

/**
  * Created by Sebastian Urbanek on 08.01.17.
  */
object Main extends App {

  // Einstiegspunkt des Programms
  override def main (args: Array[String]): Unit = {
    // Parameter auslesen
    val url = args.apply(0)
    val sensorType = args.apply(1)
    val targetPath = args.apply(2)
    val csvFormat = "com.databricks.spark.csv"


  }
}
