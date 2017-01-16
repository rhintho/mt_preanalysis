package de.beuth.unit

import java.sql.Timestamp

/**
  * Created by Sebastian Urbanek on 16.01.17.
  */
object TimeSegment {

  private val timeInterval: Long = 900000L

  def getTimestampSegment(timestamp: Timestamp): Timestamp = {
    getTime(getTimeSegment(timestamp))
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
