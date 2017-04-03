package de.beuth.unit

import java.sql.Timestamp

import org.apache.log4j.{Level, LogManager, Logger}

object TimeSegment {

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("TimeSegment")
  log.setLevel(Level.DEBUG)

  private var timeInterval: Long = 1L
  private var startTimestamp: Timestamp = Timestamp.valueOf("2009-01-01 00:00:00")
  private var endTimestamp: Timestamp = Timestamp.valueOf("2014-12-31 23:59:00")

  def getTimestampSegment(timestamp: Timestamp): Timestamp = {
    getTime(getTimeSegment(timestamp))
  }

  def setTimeInterval(timeInterval: Int): Long = {
    this.timeInterval = (timeInterval * 60 * 1000).toLong
    this.startTimestamp = getTimestampSegment(this.startTimestamp)
    this.endTimestamp = getTimestampSegment(this.endTimestamp)

    log.debug("New timeinterval at " + this.timeInterval + " ms")
    log.debug("New start timestamp at " + this.startTimestamp)
    log.debug("New end timestamp at " + this.endTimestamp)

    this.timeInterval
  }

  def getNextTimestep(timestamp: Timestamp): Timestamp = {
    val msOld = timestamp.getTime
    val msNew = msOld + this.timeInterval
    timestamp.setTime(msNew)
    timestamp
  }

  private def getTimeSegment(timestamp: Timestamp): Long = {
    timestamp.getTime - (timestamp.getTime % timeInterval)
  }

  private def getTime(timeSegment: Long): Timestamp = {
    val timestamp = Timestamp.valueOf("1970-01-01 00:00:00")
    timestamp.setTime(timeSegment)
    timestamp
  }
}
