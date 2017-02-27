package de.beuth

import de.beuth.inspector.ArgumentInspector
import de.beuth.unit.TimeSegment

/**
  * Created by Sebastian Urbanek on 08.01.17.
  */
object Main extends App {
  // Einstiegspunkt des Programms
  override def main (args: Array[String]): Unit = {
    try {
      // Parameter auslesen
      val dataPath = args.apply(0)
      val sensorType = args.apply(1)
      val targetPath = args.apply(2)
      val timeInterval = args.apply(3).toInt
      val gpsDataPath = args.apply(4)
      val temperatureDataPath = args.apply(5)
      val rainfallDataPath = args.apply(6)
      val sensorId = args.apply(7).toInt

      // Überprüfung ob alle Argumente gültig sind.
      if (ArgumentInspector.inspectArguments(dataPath, sensorType, targetPath, timeInterval, gpsDataPath,
                                             temperatureDataPath, rainfallDataPath, sensorId)) {
        // Argumente gültig, weitere Bearbeitung erlaubt.
        println("Alle Parameter sind korrekt. Spark-Programm wird gestartet ...")
        TimeSegment.setTimeInterval(timeInterval)
        println("Zeit-Intervall erfolgreich auf " + timeInterval + " Min. gesetzt.")

        SensordataTransformator.startTransformation(dataPath, sensorType, targetPath, timeInterval, gpsDataPath,
                                                    temperatureDataPath, rainfallDataPath, sensorId)
        // TODO muss noch weiter getestet werden, ob man eine weitere Analyse im Anschluss durchführen kann
      } else {
        // Argumente ungültig. Fehler ausgeben und weitere Bearbeitung beenden.
        System.err.println("Arguments invalid!")
        System.err.println(ArgumentInspector.errorMessage)
      }
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        System.err.println("No arguments found.\n" + ArgumentInspector.errorMessage)
    }
  }
}
