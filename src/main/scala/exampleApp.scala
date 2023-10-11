import scala.math.abs
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.time.Instant
import scala.jdk.CollectionConverters._
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import generator._
import org.apache.flink.streaming.api.functions.source
import scala.jdk.CollectionConverters.IterableHasAsScala
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.common.state.MapState
import org.apache.flink.api.common.state.MapStateDescriptor
import scala.jdk.CollectionConverters.IterableHasAsScala
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}

package object generator {

  case class Event(
    storeName: String,
    appID: String,
    EventTypeName: String,
    time: java.time.Instant)

  class EventGenerator(
      batchSize: Int,
      baseTime: java.time.Instant,
      millisBtwEvents: Int)
    extends SourceFunction[Event] {

    @volatile private var isRunning = true

    private def generateEvent(id: Long): Seq[Event] = {
      val events = (1 to batchSize)
        .map(_ => {
          val eventTime = abs(id - scala.util.Random.between(0L, 4L))
        
          Event(
            EventGenerator.getStore,
            EventGenerator.getAppId,
            EventGenerator.getEventType,
            baseTime.plusSeconds(eventTime)
          )}
        )

      events
    }


    private def run(
        startId: Long,
        ctx: SourceFunction.SourceContext[Event])
      : Unit = {

      while (isRunning) {
        generateEvent(startId).foreach(ctx.collect)
        Thread.sleep(batchSize * millisBtwEvents)
        run(startId + batchSize, ctx)
      }
    }


    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = run(0, ctx)

    override def cancel(): Unit = isRunning = false
  }

  object EventGenerator {


    private val store: Vector[String] = Vector(
      "HuaweiAppStore",
      "AppleAppStore",
      "GooglePlay",
      "RuStore")


    private val appId: Vector[String] = Vector(
      "2616001479570096825", 
      "4091758100234781984", 
      "9102759165103862720", 
      "1045619379553910805")

    private val eventType: Vector[String] = Vector(
      "page_view",
      "install",
      "uninstall",
      "error")


    def getStore: String = store(scala.util.Random.nextInt(store.length))

    def getAppId: String = appId(scala.util.Random.nextInt(appId.length))

    def getEventType: String = eventType(scala.util.Random.nextInt(eventType.length))
    
  }

}

object sampleApp {
  
  def appStoreStream(): Unit = {
 
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment


    val startTime: Instant =
        Instant.parse("2023-07-15T00:00:00.000Z")      

    class SampleClass extends ProcessAllWindowFunction[Event, String, TimeWindow] {
      override def process(
        context: ProcessAllWindowFunction[Event, String, TimeWindow]#Context,
        elements: lang.Iterable[Event],
        out: Collector[String]): Unit = {
          out.collect(s"${elements}")
        }
    }

    val getMetricsStream =
      env.addSource(new EventGenerator(1, startTime, 500))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(3))
          .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
              element.time.toEpochMilli
            }
          }))      
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
      .process(new SampleClass)

      getMetricsStream.print()
      env.execute()

  }

  def main(args: Array[String]): Unit = {
    appStoreStream()
  }
}



// package object generator {

//   case class Click(
//     userId: String,
//     button: String,
//     clickCount: Int,
//     time: java.time.Instant)

//   class 59nerator(
//       batchSize: Int,
//       baseTime: java.time.Instant,
//       millisBtwEvents: Int)
//     extends SourceFunction[Click] {

//     @volatile private var isRunning = true

//     private def generateClick(id: Long): Seq[Click] = {
//       val events = (1 to batchSize)
//         .map(_ =>
//           Click(
//             ClickGenerator.getUserId,
//             ClickGenerator.getButton,
//             ClickGenerator.getClickCount,
//             baseTime.plusSeconds(id)
//           )
//         )

//       events
//     }






//  private val buttons: Vector[String] = Vector(
  //    "GreenBtn", 
      // "BlueBtn", 
      // "RedBtn", 
      // "OrangeBtn")

//     private val users: Vector[String] = Vector(
//       "65b4c326",
//       "1b2e622d",
//       "24f3c8b8",
//       "9a608d9e")

//     def getButton: String = buttons(scala.util.Random.nextInt(buttons.length))

//     def getUserId: String = users(scala.util.Random.nextInt(users.length))

//     def getClickCount: Int = scala.util.Random.nextInt(10)








    // store - магазин мобильных приложений: HuaweiAppStore, AppleAppStore, GooglePlay или RuStore
    // appId - id мобильного приложения (немного упростим задачу и договоримся, что в каждом store у нас будет всего четыре приложения со следующими id: 2616001479570096825, 4091758100234781984, 9102759165103862720, 1045619379553910805, за которыми мы будем следить)
    // eventType - тип события: page_view, install, uninstall или error
    // eventTime - время наступления события



// В этот раз вы также отвечаете за генерацию событий. 
// Чтобы реализовать генерацию событий, вам предстоит поработать с RichSourceFunction и реализовать класс, 
// который расширяет эту функцию ( extends RichSourceFunction[Event]).
// Для примера можете обратиться к этому коду. Учитывайте, что отличие 
// RichSourceFunction от SourceFunction заключается лишь в том, что вдобавок к основным методам run и cancel 
// можно также переопределить методы open и close и получить доступ к RuntimeContext

// Таким образом, есть два основных метода, которые обязательно следует реализовать:
// override def run(ctx: SourceFunction.SourceContext[T]) - метод отвечает за предоставление данных 
//  во Flink (он вызывается один раз и создает бесконечный поток данных)
// override def cancel(): Unit - метод вызывается, когда приложение закрывается; ожидается, 
//   что после вызова cancel, завершается метод run
