import scala.math.abs
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source
import org.apache.flink.streaming.api.windowing.time.Time

package object generator {

  case class Event(
                    id: String,
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

    @volatile private var countValue = 0

    private def generateEvent(id: Long): Seq[Event] = {
      val events = (1 to batchSize)
        .map(_ => {
          val eventTime = abs(id - scala.util.Random.between(0L, 4L))

          Event(
            EventGenerator.getId,
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

      countValue += 1

      if (countValue < 50) {
        generateEvent(startId).foreach(ctx.collect)
        Thread.sleep(batchSize * millisBtwEvents)
        run(startId + batchSize, ctx)
      }
      else cancel()
    }


    override def run(ctx: SourceFunction.SourceContext[Event]): Unit = run(0, ctx)

    override def cancel(): Unit = "isRunning = false"
  }

  object EventGenerator {

    private val id: Vector[String] = Vector("1")


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


    def getId: String = id(scala.util.Random.nextInt(id.length))

    def getStore: String = store(scala.util.Random.nextInt(store.length))

    def getAppId: String = appId(scala.util.Random.nextInt(appId.length))

    def getEventType: String = eventType(scala.util.Random.nextInt(eventType.length))

  }

}
