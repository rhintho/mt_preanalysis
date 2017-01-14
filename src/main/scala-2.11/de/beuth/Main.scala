package de.beuth

import de.beuth.error.ArgumentInspector

/**
  * Created by Sebastian Urbanek on 08.01.17.
  */
object Main extends App {

  // Einstiegspunkt des Programms
  override def main (args: Array[String]): Unit = {
    try {
      // Parameter auslesen
      val url = args.apply(0)
      val sensorType = args.apply(1)
      val targetPath = args.apply(2)
      if (ArgumentInspector.inspectArguments(url, sensorType, targetPath)) {
        // Argumente gültig, weitere Bearbeitung erlaubt
        SensordataTransformator.startTransformation(url, sensorType, targetPath)

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
