package de.beuth

import de.beuth.inspector.ArgumentInspector
import org.apache.log4j.{Logger, LogManager}

/**
  * Created by Sebastian Urbanek on 08.01.17.
  */
object Main extends App {
  // Einstiegspunkt des Programms
  override def main (args: Array[String]): Unit = {
    // Logger initialisieren
    val log = LogManager.getLogger(Main.getClass)

    try {
      // Parameter auslesen
      val dataPath = args.apply(0)
      val sensorType = args.apply(1)
      val targetPath = args.apply(2)
      val gpsDataPath = args.apply(3)

      // Überprüfung ob alle Argumente gültig sind.
      if (ArgumentInspector.inspectArguments(dataPath, sensorType, targetPath, gpsDataPath)) {
        // Argumente gültig, weitere Bearbeitung erlaubt.
        log.debug("Alle Parameter sind korrekt. Spark-Programm wird gestartet ...")
        SensordataTransformator.startTransformation(dataPath, sensorType, targetPath, gpsDataPath)

      } else {
        // Argumente ungültig. Fehler ausgeben und weitere Bearbeitung beenden.
        log.error("Arguments invalid!")
        log.error(ArgumentInspector.errorMessage)
      }
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        log.error("No arguments found.\n" + ArgumentInspector.errorMessage)
    }
  }
}
