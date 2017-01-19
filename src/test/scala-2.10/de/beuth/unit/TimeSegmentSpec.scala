package de.beuth.unit

import java.sql.Timestamp

import org.scalatest.FlatSpec

/**
  * Created by Sebastian Urbanek on 16.01.17.
  */
class TimeSegmentSpec extends FlatSpec {
  val time1: Timestamp = Timestamp.valueOf("2009-01-01 00:00:00")
  val time2: Timestamp = Timestamp.valueOf("2009-01-01 00:00:01")
  val time3: Timestamp = Timestamp.valueOf("2009-01-01 00:14:59")
  val time4: Timestamp = Timestamp.valueOf("2009-01-01 00:15:00")
  val time5: Timestamp = Timestamp.valueOf("2014-12-31 23:59:59")
  val time6: Timestamp = Timestamp.valueOf("2015-01-01 00:00:01")

//  val timeSegment1: Long = TimeSegment.getTimeSegment(time1)
//  val timeSegment2: Long = TimeSegment.getTimeSegment(time2)
//  val timeSegment3: Long = TimeSegment.getTimeSegment(time3)
//  val timeSegment4: Long = TimeSegment.getTimeSegment(time4)
//  val timeSegment5: Long = TimeSegment.getTimeSegment(time5)
//  val timeSegment6: Long = TimeSegment.getTimeSegment(time6)

  val timestamp1: Timestamp = Timestamp.valueOf("2009-01-01 00:00:00")
  val timestamp4: Timestamp = Timestamp.valueOf("2009-01-01 00:15:00")
  val timestamp5: Timestamp = Timestamp.valueOf("2014-12-31 23:45:00")
  val timestamp6: Timestamp = Timestamp.valueOf("2015-01-01 00:00:00")

  "getTimestampSegment()" should "calculate the correct time segment." in {
    TimeSegment.setTimeInterval(15)
    assert(TimeSegment.getTimestampSegment(time1) == timestamp1)
    assert(TimeSegment.getTimestampSegment(time2) == timestamp1)
    assert(TimeSegment.getTimestampSegment(time3) == timestamp1)
    assert(TimeSegment.getTimestampSegment(time4) == timestamp4)
    assert(TimeSegment.getTimestampSegment(time5) == timestamp5)
    assert(TimeSegment.getTimestampSegment(time6) == timestamp6)
  }

  "setTimeInterval()" should "set the new time interval and calculates new start- and endtimestamp." in {
    TimeSegment.setTimeInterval(15)
  }

  "getNextTimestep()" should "return the next timestamp." in {
    TimeSegment.setTimeInterval(15)
    assert(TimeSegment.getNextTimestep(time1) == Timestamp.valueOf("2009-01-01 00:15:00"))
  }

  //  "TimeSegment" should "calculate the unix time correctly." in {
  //    assert(timeSegment1 == timestamp1.getTime)
  //    assert(timeSegment2 == timestamp1.getTime)
  //    assert(timeSegment3 == timestamp1.getTime)
  //    assert(timeSegment4 == timestamp4.getTime)
  //    assert(timeSegment5 == timestamp5.getTime)
  //    assert(timeSegment6 == timestamp6.getTime)
  //  }
  //
  //  it should "recalculate the original timestamp from given timesegment." in {
  //    var testTime = Timestamp.valueOf("2009-01-01 00:00:00")
  //
  //    testTime.setTime(timeSegment1)
  //    assert(testTime == timestamp1)
  //
  //    testTime.setTime(timeSegment2)
  //    assert(testTime == timestamp1)
  //
  //    testTime.setTime(timeSegment3)
  //    assert(testTime == timestamp1)
  //
  //    testTime.setTime(timeSegment4)
  //    assert(testTime == timestamp4)
  //
  //    testTime.setTime(timeSegment5)
  //    assert(testTime == timestamp5)
  //
  //    testTime.setTime(timeSegment6)
  //    assert(testTime == timestamp6)
  //  }
}
