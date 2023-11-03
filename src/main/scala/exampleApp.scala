package scala

import generator.Generator.{Event, EventGenerator}

import java.lang
import scala.math.abs
import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.collection.immutable.ListMap
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.api.java.functions.KeySelector
import scala.jdk.CollectionConverters.IterableHasAsScala

import org.apache.flink.api.common.functions.{ReduceFunction, JoinFunction}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, ReducingStateDescriptor, ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor, StateTtlConfig}

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, GlobalWindow}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, CountTrigger, ProcessingTimeTrigger, TriggerResult,  PurgingTrigger}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingProcessingTimeWindows, EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows, GlobalWindows}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.windowing.{ProcessAllWindowFunction, ProcessWindowFunction, AllWindowFunction, WindowFunction}

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.TaggedUnion
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}

object exmapleApp {

  def appStoreStream(): Unit = {

      val env = StreamExecutionEnvironment
        .getExecutionEnvironment

      val startTime: Instant =
            Instant.parse("2023-07-15T00:00:00.000Z")

      val outputTag = new OutputTag[Event]("side-output"){}

      class MetricByEvent extends ProcessWindowFunction[Event, String, String, GlobalWindow] {

          var appleState: MapState[String, Long] = _

          var googleState: MapState[String, Long] = _
            
          var ruState: MapState[String, Long] = _
            
          var huaweiState: MapState[String, Long] = _

          override def open(parameters: Configuration): Unit = {

              appleState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Long](
                  "appleState",classOf[String], classOf[Long])
              )

              googleState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Long](
                  "googleState",
                  classOf[String], classOf[Long])
              )

              ruState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Long](
                  "ruState",
                  classOf[String], classOf[Long])
              )

              huaweiState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Long](
                  "huaweiState",
                  classOf[String], classOf[Long])
              )
            }

          override def process(
            key: String,
            context: ProcessWindowFunction[Event, String, String, GlobalWindow]#Context,
            elements: lang.Iterable[Event],
            out: Collector[String]): Unit = {
            
              val appId: Vector[String] = Vector(
                  "2616001479570096825", 
                  "4091758100234781984", 
                  "9102759165103862720", 
                  "1045619379553910805")

              val store: Vector[String] = Vector(
                  "HuaweiAppStore",
                  "AppleAppStore",
                  "GooglePlay",
                  "RuStore")

              val eventType: Vector[String] = Vector(
                  "install",
                  "uninstall")
              
              val metricNames = eventType.flatMap(i => appId.map(t => i + t))

              def merge(m1: Map[String, Long], m2: Map[String, Long]): Map[String, Long] = {
                  m1.map { 
                      case (k, v) => k -> m2.getOrElse(k, v) 
                  } ++ m2.filterKeys(!m1.keySet(_))
              }

              def getMetricValues(name: String, id: String, eventType: String): Long = {
                  elements.asScala.filter(rec => {
                  rec.storeName == name &&
                  rec.appID == id &&
                  rec.EventTypeName == eventType}).size
              }

              def countEventType(eventType: String): Long = {
                  elements.asScala.filter(rec => {
                  rec.EventTypeName == eventType}).size
              }

              def updateMetricValues(values: Vector[Long], state: MapState[String, Long]): Unit = {
                  Range(0,8).map(index => {
                  val currentValue = state.get(metricNames(index))
                  state.put(metricNames(index), currentValue.toLong + values(index))
                  })
              }

              def showMetricValues(state: MapState[String, Long]): Seq[(String, Long)] = {
                  val valuesInstall = appId.map(id => state.get("install" + id))
                  val valuesUninstall = appId.map(id => state.get("uninstall" + id))
                  val namesInstall = Range(0,4).map(n => metricNames(n))
                  val namesUninstall = Range(4,8).map(n => metricNames(n))
                  val metricInstall = namesInstall zip valuesInstall
                  val metricUninstall = namesUninstall zip valuesUninstall
                  val resultInstall = ListMap(metricInstall.toSeq.sortWith(_._2 > _._2):_*)
                  val resultUninstall = ListMap(metricUninstall.toSeq.sortWith(_._2 > _._2):_*)
                  merge(resultInstall, resultUninstall).toSeq
              }

              val appleUnintsall = appId.map(item => getMetricValues("AppleAppStore", item, "uninstall"))
              val appleIntsall = appId.map(item => getMetricValues("AppleAppStore", item, "install"))
              val googleUnintsall = appId.map(item => getMetricValues("GooglePlay", item, "uninstall"))
              val googleIntsall = appId.map(item => getMetricValues("GooglePlay", item, "install"))
              val ruUnintsall = appId.map(item => getMetricValues("RuStore", item, "uninstall"))
              val ruIntsall = appId.map(item => getMetricValues("RuStore", item, "install"))
              val huaweiUnintsall = appId.map(item => getMetricValues("HuaweiAppStore", item, "uninstall"))
              val huaweiIntsall = appId.map(item => getMetricValues("HuaweiAppStore", item, "install"))

              if (!appleState.contains("install2616001479570096825")){
                metricNames.map(n => appleState.put(n, 0))
                updateMetricValues(appleIntsall ++ appleUnintsall, appleState)
                metricNames.map(n => googleState.put(n, 0))
                updateMetricValues(googleIntsall ++ googleUnintsall, googleState)
                metricNames.map(n => ruState.put(n, 0))
                updateMetricValues(ruIntsall ++ ruUnintsall, ruState)
                metricNames.map(n => huaweiState.put(n, 0))
                updateMetricValues(huaweiIntsall ++ huaweiUnintsall, huaweiState)}
              else {
                updateMetricValues(appleIntsall ++ appleUnintsall, appleState)
                updateMetricValues(googleIntsall ++ googleUnintsall, googleState)
                updateMetricValues(ruIntsall ++ ruUnintsall, ruState)
                updateMetricValues(huaweiIntsall ++ huaweiUnintsall, huaweiState)
              }

              val appleChart = showMetricValues(appleState)
              val googleChart = showMetricValues(googleState)
              val ruChart = showMetricValues(ruState)
              val huaweiChart = showMetricValues(huaweiState)

              out.collect(s"""
                      AppleApp \n
                      Install:                       Uninstall: \n
                      ${appleChart(0)}  ${appleChart(4)}\n
                      ${appleChart(1)}  ${appleChart(5)}\n
                      ${appleChart(2)}  ${appleChart(6)}\n
                      GoogleApp \n
                      Install:                        Uninstall: \n
                      ${googleChart(0)}  ${googleChart(4)}\n
                      ${googleChart(1)}  ${googleChart(5)}\n
                      ${googleChart(2)}  ${googleChart(6)}\n
                      RuApp \n
                      Install:                        Uninstall: \n
                      ${ruChart(0)}  ${ruChart(4)}\n
                      ${ruChart(1)}  ${ruChart(5)}\n
                      ${ruChart(2)}  ${ruChart(6)}\n
                      HuaweiApp \n
                      Install:                        Uninstall: \n
                      ${huaweiChart(0)}  ${huaweiChart(4)}\n
                      ${huaweiChart(1)}  ${huaweiChart(5)}\n
                      ${huaweiChart(2)}  ${huaweiChart(6)}\n                                                      
                      """
            )
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

            val appleErrors = getErrorsType("AppleAppStore", elements)
            val googleErrors = getErrorsType("GooglePlay", elements)
            val ruErrors = getErrorsType("RuStore", elements)
            val huaweiErrors = getErrorsType("HuaweiAppStore", elements)  

            out.collect(s""" 
              appleErrors: ${appleErrors} \n
              googleErrors: ${googleErrors} \n
              ruErrors: ${ruErrors} \n
              huaweiErrors: ${huaweiErrors} 
              """)          
          }
      }

      class CustomEventTrigger extends Trigger[Event, GlobalWindow] {

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
            val elementTypes = ctx.getPartitionedState(getTypeState)
            
            if (element.EventTypeName != "error"){
              val elementName = element.EventTypeName match {
                case "install" => "install"
                case _ => "uninstall"
              }
              if (elementTypes.contains(elementName)){
                val currentCount = 
                  elementTypes.get(elementName) + 1
                elementTypes.put(elementName, currentCount)}
              else elementTypes.put(elementName, 1)

              val hasUninstallCount = (elementTypes.get(elementName) == 50)          
              val hasInstallCount = (elementTypes.get(elementName) == 100)                 

              if (elementName == "install" && hasInstallCount){
                  elementTypes.clear()
                  TriggerResult.FIRE_AND_PURGE}
              else if (elementName == "uninstall" && hasUninstallCount){
                  elementTypes.clear()
                  TriggerResult.FIRE_AND_PURGE}
              else TriggerResult.CONTINUE
            }
            else TriggerResult.CONTINUE          
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
          override def getKey(value: Event): String = value.id
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
