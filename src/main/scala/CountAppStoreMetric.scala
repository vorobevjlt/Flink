package scala

import org.apache.flink.api.java.functions.KeySelector
import generator._
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.lang
import scala.math.abs
import java.time.{Instant, LocalTime}
import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap

import org.apache.flink.core.fs.Path
import org.apache.flink.configuration.Configuration

import org.apache.flink.util.{Collector, OutputTag}

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

/*
Задача состоит в том, чтобы обрабатывать события Event,
связанные с магазинами приложений.
Каждое событие обязательно содержит следующую информацию:

  store - магазин мобильных приложений: HuaweiAppStore, AppleAppStore, GooglePlay или RuStore
  appId - id мобильного приложения
  eventType - тип события: page_view, install, uninstall или error
  eventTime - время наступления события

Эту информацию следует использовать для того,
чтобы для каждого магазина приложений store находить топ-3
наиболее часто устанавливаемых (install)
и наиболее часто удаляемых (uninstall) приложений за все время.
При этом получение обновленной статистики должно происходить каждый раз,
как только произошло 50 удалений или 100 скачиваний приложений.

При этом, каждые 10 секунд мы должны для каждого магазина получать статистику о том,
сколько за эти 10 секунд произошло ошибок (в eventType указано error).
*/

object SimpleApp {

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

        out.collect(s"${key} \n" +
          s"${getMetricFromState.take(3)}")
      }
    }

    class MetricByTime extends ProcessWindowFunction[Event, String, String, TimeWindow] {

      def getErrorsType(
                         name: String,
                         elements: lang.Iterable[Event]): Long = {
        elements.asScala
          .filter(rec => {
            rec.storeName == name
          })
          .size}

      override def process(
                            key: String,
                            context: ProcessWindowFunction[Event,String, String, TimeWindow]#Context,
                            elements: lang.Iterable[Event],
                            out: Collector[String]): Unit = {

        out.collect(s"$key ${elements.asScala.size}")
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
          override def getKey(value: Event): String = value.storeName
        })
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(new MetricByTime)


    mainStream.print()
    sideOutputStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    appStoreStream()
  }

}
