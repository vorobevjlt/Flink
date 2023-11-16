package scala

import java.text.SimpleDateFormat
import org.apache.flink.types.PojoTestUtils.assertSerializedAsPojo
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import generator.Generator.{Event, EventGenerator}

import java.lang
import scala.math.abs
import java.time.{Instant, LocalTime}
import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap

import org.apache.flink.core.fs.Path
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.{DataStreamSource, ConnectedStreams}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.common.functions.{ReduceFunction, JoinFunction, FlatMapFunction, FilterFunction}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, ReducingStateDescriptor, ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor, StateTtlConfig}

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, GlobalWindow}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, CountTrigger, ProcessingTimeTrigger, TriggerResult,  PurgingTrigger}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingProcessingTimeWindows, EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, GlobalWindows}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.RichSourceFunction 
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction, AllWindowFunction, WindowFunction}

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}

// object Generator {

//   case class Event(
//     id: String,
//     storeName: String,
//     appID: String,
//     EventTypeName: String,
//     time: java.time.Instant)

//   class EventGenerator(
//       batchSize: Int,
//       baseTime: java.time.Instant,
//       millisBtwEvents: Int)
//     extends SourceFunction[Event] {

//     @volatile private var isRunning = true

//     private def generateEvent(id: Long): Seq[Event] = {
//       val events = (1 to batchSize)
//         .map(_ => {
//           val eventTime = abs(id - scala.util.Random.between(0L, 4L))
        
//           Event(
//             EventGenerator.getId,
//             EventGenerator.getStore,
//             EventGenerator.getAppId,
//             EventGenerator.getEventType,
//             baseTime.plusSeconds(eventTime)
//           )}
//         )

//       events
//     }

//     private def run(
//         startId: Long,
//         ctx: SourceFunction.SourceContext[Event])
//       : Unit = {

//       while (isRunning) {
//         generateEvent(startId).foreach(ctx.collect)
//         Thread.sleep(batchSize * millisBtwEvents)
//         run(startId + batchSize, ctx)
//       }
//     }


//     override def run(ctx: SourceFunction.SourceContext[Event]): Unit = run(0, ctx)

//     override def cancel(): Unit = isRunning = false
//   }

//   object EventGenerator {

//     private val id: Vector[String] = Vector("1")

//     private val store: Vector[String] = Vector(
//       "HuaweiAppStore",
//       "AppleAppStore",
//       "GooglePlay",
//       "RuStore")


//     private val appId: Vector[String] = Vector(
//       "2616001479570096825", 
//       "4091758100234781984", 
//       "9102759165103862720", 
//       "1045619379553910805")

//     private val eventType: Vector[String] = Vector(
//       "page_view",
//       "install",
//       "uninstall",
//       "error")

//     def getId: String = id(scala.util.Random.nextInt(id.length))

//     def getStore: String = store(scala.util.Random.nextInt(store.length))

//     def getAppId: String = appId(scala.util.Random.nextInt(appId.length))

//     def getEventType: String = eventType(scala.util.Random.nextInt(eventType.length))    
//   }
// }

object Main {

  def appStoreStream(): Unit = {

      val env = StreamExecutionEnvironment
        .getExecutionEnvironment

      val startTime: Instant =
            Instant.parse("2023-07-15T00:00:00.000Z")

      val outputTag = new OutputTag[Event]("ErrorOutput"){}

      class MetricByEvent extends ProcessWindowFunction[Event, String, String, GlobalWindow] {
          // определяем параметры состояния
          var storeState: MapState[String, Long] = _

          override def open(parameters: Configuration): Unit = {

              storeState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Long](
                  "storeName",classOf[String], classOf[Long])
              )
            }

          def addToState(values: Map[String, Int]): Unit = {
              val names = values.keys.toList
              names.map(n => {
                if (!storeState.contains(n)) storeState.put(n, values(n))
                else {
                  val currentCount = storeState.get(n) + values(n)
                  storeState.put(n, currentCount)
                }  
              })
          }

          override def process(
            key: String,
            context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
            elements: lang.Iterable[Event],
            out: Collector[String]): Unit = {
                // получаем название магазина, по которому будут обновлятся метрики
                val storeKey = elements.asScala.map(_.storeName).max
                // получаем данные приложении, с которыми происходили события
                val appID = elements.asScala.map(_.appID).toList
                val eventType = elements.asScala.map(_.EventTypeName).toList
                val length = eventType.size
                // получим по какому приложению было удалине либо установка 
                val metricName = Range(0, length).map(n => {appID(n) + eventType(n)})
                // считаем удаление и установку по каждому приложению
                val countMetrics = metricName.foldLeft(Map.empty[String, Int]) { (m, x) => m + ((x, m.getOrElse(x, 0) + 1)) }
                // добавляем посчитанные значения в состояние
                addToState(countMetrics)
                // получим метрики хранящиеся в состоянии
                val getKeys = (storeState.keys().toString)
                val mericNames = getKeys.filterNot(c => c  == '[' || c == ']' || c == ' ').split(",").toList
                // сортируем метрики хранящиеся в состоянии
                val getMetricFromState = mericNames.map(rec => {
                  val value = storeState.get(rec)
                  (rec, value)
                }).sortWith(_._2 > _._2)
                
                out.collect(s"${storeKey} \n" +
                  s"${getMetricFromState.take(3)}")
          }
      }

      class MetricByTime extends ProcessAllWindowFunction[Event, String, TimeWindow] {

          def getErrorsType(
            name: String,
            elements: lang.Iterable[Event]): Long = {
              elements.asScala
              .filter(rec => {
                rec.storeName == name 
              })
              .size}

          override def process(
            context: ProcessAllWindowFunction[Event, String, TimeWindow]#Context,
            elements: lang.Iterable[Event],
            out: Collector[String]): Unit = {

            // считаем события со значением "error" по каждому магазину
            val appleErrors = getErrorsType("AppleAppStore", elements)
            val googleErrors = getErrorsType("GooglePlay", elements)
            val ruErrors = getErrorsType("RuStore", elements)
            val huaweiErrors = getErrorsType("HuaweiAppStore", elements)  

            out.collect(s"\nAppleStoreErros: ${appleErrors} \n" +
            s"GoogleStoreErrors: ${googleErrors} \n" +
            s"RuStoreErrors: ${ruErrors} \n" +
            s"HuaweiStoreErrors: ${huaweiErrors}")
          }
      }

      class CustomEventTrigger extends Trigger[Event, GlobalWindow] {
        // определяем параметры состояния
        val getTypeState = new MapStateDescriptor[String, Int](
            "mapStateCounter",
            classOf[String],
            classOf[Int])

        override def onElement(
            element: Event,
            timestamp: Long,
            window: GlobalWindow,
            ctx: Trigger.TriggerContext)
            : TriggerResult = {
            // создаем экземпляр состояние
            val elementTypeState = ctx.getPartitionedState(getTypeState)
            // получаем название события
            val elementType = element.EventTypeName
            // добавляем либо инкрементируем значение состояния
            if (elementTypeState.contains(elementType)){
                val currentCount = 
                  elementTypeState.get(elementType) + 1
                elementTypeState.put(elementType, currentCount)}
            else elementTypeState.put(elementType, 1)
            // задаем условия для триггера
            val hasUninstallCount = (elementTypeState.get(elementType) == 50)          
            val hasInstallCount = (elementTypeState.get(elementType) == 100)                 
            val hasReasonToFireTrigger = (elementType == "install" && hasInstallCount || 
                  elementType == "uninstall" && hasUninstallCount)
            //определяем поведение триггера в зависимости от состояния условия
            if (hasReasonToFireTrigger) {
              elementTypeState.clear()
              TriggerResult.FIRE_AND_PURGE 
                }
            else {
              TriggerResult.CONTINUE
              }
        }

        override def onProcessingTime(
            time: Long,
            window: GlobalWindow,
            ctx: Trigger.TriggerContext)
          : TriggerResult = {
            TriggerResult.CONTINUE
        }

        override def onEventTime(
            time: Long,
            window: GlobalWindow,
            ctx: Trigger.TriggerContext)
          : TriggerResult = {
            TriggerResult.CONTINUE
        }

        override def clear(
            window: GlobalWindow,
            ctx: Trigger.TriggerContext)
          : Unit = {
        }
      }
      
      val listenEventStream =
          env
          .addSource(new EventGenerator(1, startTime, 500))
          .filter(rec => rec.EventTypeName != "page_view")
          .assignTimestampsAndWatermarks(
              WatermarkStrategy
              .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(1))
              .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
                  override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
                  element.time.toEpochMilli}}))    
          .process(new ProcessFunction[Event, Event] {
            override def processElement(
              value: Event,
              ctx: ProcessFunction[Event, Event]#Context,
              out: Collector[Event]): Unit = {
                if (value.EventTypeName == "error") {
                  ctx.output(outputTag, value)
                }
                else {
                  out.collect(value)
                }
              }
          }
      )
        
      val mainStream =
        listenEventStream
        .keyBy(new KeySelector[Event, String]{
          override def getKey(value: Event): String = value.storeName
        })
        .window(GlobalWindows.create())
        .trigger(new CustomEventTrigger)
        .process(new MetricByEvent)

      val sideOutputStream = 
        listenEventStream
        .getSideOutput(outputTag)
        .keyBy(new KeySelector[Event, String] {
          override def getKey(value: Event): String = value.id
        })
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(new MetricByTime)


      mainStream.print()
      sideOutputStream.print()
      env.execute()
  }

  def main(args: Array[String]): Unit = {
    appStoreStream()
  }

}