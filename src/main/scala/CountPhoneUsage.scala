
object PhoneUsage {

  case class Record(
    @JsonProperty("app_name") var appName: String,
    @JsonProperty("date") var date: String,
    @JsonProperty("time") var time: String,
    @JsonProperty("duration") var duration: String)


  val startTime: Instant =
    Instant.parse("2000-07-15T00:00:00.000Z")

  case class CountPhoneUsageMetric(var time: Instant, var keyName: String, var countSum: Long) {
    def this() {
      this(startTime, "", 0L)
    }
  }

  final class MetricByKey extends ProcessWindowFunction[CountPhoneUsageMetric, String, String, TimeWindow] {
    override def process(
      key: String,
      context: ProcessWindowFunction[CountPhoneUsageMetric, String, String, TimeWindow]#Context,
      elements: lang.Iterable[CountPhoneUsageMetric],
      out: Collector[String]): Unit = {
        
        out.collect(s"$key: ${elements.asScala.size}")
      }
  }

  def getDateFormat(str: String): String = {
    val dateList = str.split("-")
    dateList(2) + "-" + dateList(1) + "-" + dateList(0)
  }

  def convertToSeconds(time: String): Long = {
    val h = if (time.split(":")(0) == "00") 0
            else {3600 * time.split(":")(0).toInt}
    val m = if (time.split(":")(1) == "00") 0
            else {60 * time.split(":")(1).toInt}
    val s = time.split(":")(2).toInt
    (h+m+s).toLong
  }

  def phoneUsageStream(): Unit = {
    val filePath = new Path("src/main/resources/phone_usage.csv")
    val targetTimeOutputTag = new OutputTag[Record]("moreThanTargetTime"){}
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    val csvSchema = CsvSchema
      .builder()
      .addNumberColumn("app_name")
      .addColumn("date")
      .addColumn("time")
      .addColumn("duration")
      .build()

    val source: FileSource[Record] = FileSource
      .forRecordStreamFormat(
        CsvReaderFormat.forSchema(csvSchema, Types.GENERIC(classOf[Record])),
        filePath)
      .build()


    val separateStreamsFunction =
      new ProcessFunction[Record, CountPhoneUsageMetric] {
        override def processElement(
          value: Record,
          ctx: ProcessFunction[Record, CountPhoneUsageMetric]#Context,
          out: Collector[CountPhoneUsageMetric]): Unit = {
            
            val properFormatDate = value.date.replaceAll("[^0-9]", "-")

            val date = properFormatDate(2) match {
              case '-' => getDateFormat(properFormatDate)
              case _ => properFormatDate
            }
            
            val time: Instant =
              Instant.parse(date + "T" + value.time + "Z")

            val seconds = convertToSeconds(value.duration)
            
            if (seconds >= 1800 && seconds <= 3600) ctx.output(targetTimeOutputTag, value)

            out.collect(CountPhoneUsageMetric(
              time, value.appName, seconds))
        }
      }

    val appKey = new KeySelector[CountPhoneUsageMetric, String] {
      override def getKey(keyValue: CountPhoneUsageMetric): String =
        keyValue.keyName
    }

    val stream: DataStreamSource[Record] = env
      .fromSource(
        source,
        WatermarkStrategy.noWatermarks(),
        "csv-file"
      )

    val separateStream = stream
      .filter(rec => !(rec.appName.contains("Screen")))
      .process(separateStreamsFunction)

    val countUsagePerHour = separateStream  
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000L))
          .withTimestampAssigner(new SerializableTimestampAssigner[CountPhoneUsageMetric] {
            override def extractTimestamp(element: CountPhoneUsageMetric, recordTimestamp: Long): Long = {
              element.time.toEpochMilli
            }
          }))
      .keyBy(appKey)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .process(new MetricByKey)

    val countAppUsageTimeStream = separateStream
      .keyBy(appKey)
      .sum("countSum")

    val targetTimeUsageStream = separateStream
      .getSideOutput(targetTimeOutputTag)

    // countUsagePerHour.print()
    // countAppUsageTimeStream.print()
    // targetTimeUsageStream.print()

    env.execute()
  }  
    def main(args: Array[String]): Unit = {
      phoneUsageStream()
  }
}
