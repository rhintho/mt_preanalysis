package de.beuth.error

import java.nio.file.{Files, Paths}

/**
  * Objekt zur Überprüfung der einzelnen Argumente bei Programmaufruf. Sollten diese Parameter bereits nicht
  * stimmen, ist es sinnvoll die weitere Bearbeitung abzubrechen und eine Nachricht an den Nutzer zu senden.
  */
object ArgumentInspector {

  val errorMessage: String = "Please use following options after spark-submit JAR " +
                             "[path to CSV-Files] [Sensortype] [path to target]"

  def inspectArguments(url: String, sensorType: String, targetPath: String): Boolean = {
    inspectURL(url) &&
    inspectSensorType(sensorType) &&
    inspectTargetPath(targetPath)
  }

  private def inspectTargetPath(targetPath: String): Boolean = {
    !targetPath.contains(".") && Files.exists(Paths.get(targetPath))
  }

  private def inspectSensorType(sensorType: String): Boolean = {
    sensorType.toUpperCase.equals("PZS") || sensorType.toUpperCase.equals("ABA")
  }

  private def inspectURL(url: String): Boolean = {
    Files.exists(Paths.get(url))
  }
}
