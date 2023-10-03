import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Instant
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.api.common.functions.JoinFunction
import java.lang
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.ReduceFunction

object exampleApp {

  def countEventWindowById(): Unit = {

    final case class Booking(geoRegion: String, eventTime: Instant, bookingCount: Int)

    class CountByWindow extends WindowFunction[Booking, String, String, TimeWindow] {
      override def apply(
        key: String,
        window: TimeWindow,
        input: lang.Iterable[Booking],
        out: Collector[String]): Unit = {

          val res: Iterable[Int] = input.asScala.map(_.bookingCount)

          out.collect(s"key: [ $key ] \n " +
                      s"  window: [${window.getStart} - ${window.getEnd}] \n" +
                      s"   max bookingCount: ${res.max} \n" +
                      s"   min bookingCount: ${res.min}")
        }
    }

    val env = StreamExecutionEnvironment
      .getExecutionEnvironment    

    val startTime: Instant =
      Instant.parse("2023-07-15T00:00:00.000Z")

    val eventTime = (millis: Long) => startTime.plusMillis(millis)

    val clicks = List(
      Booking("Domestic", eventTime(1000L), 4),
      Booking("International", eventTime(1000L), 6),
      Booking("Domestic", eventTime(1000L), 10),
      Booking("International", eventTime(1200L), 7),
      Booking("International", eventTime(1800L), 12),
      Booking("International", eventTime(1500L), 4),
      Booking("International", eventTime(2000L), 1),
      Booking("Domestic", eventTime(2100L), 0),
      Booking("Domestic", eventTime(3000L), 2),
      Booking("International", eventTime(6000L), 8),
      Booking("International", eventTime(6000L), 1),
      Booking("Domestic", eventTime(6700L), 1),
      Booking("International", eventTime(7200L), 5),
      Booking("Domestic", eventTime(8000L), 3),
      Booking("International", eventTime(8100L), 6),
      Booking("Domestic", eventTime(8400L), 14),
      Booking("International", eventTime(9000L), 2),
      Booking("International", eventTime(9000L), 4),
    ).asJava

    val stream = env
      .fromCollection(clicks)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000L))
          .withTimestampAssigner(new SerializableTimestampAssigner[Booking] {
            override def extractTimestamp(element: Booking, recordTimestamp: Long): Long = {
              element.eventTime.toEpochMilli
            }
          }))
      .keyBy(new KeySelector[Booking, String] {
        override def getKey(value: Booking): String = value.geoRegion
      })
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new CountByWindow())

    stream.print()

    env.execute()
    
  }

  def main(args: Array[String]): Unit = {
    countEventWindowById()
  }

}