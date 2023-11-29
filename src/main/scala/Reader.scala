/*
 На основании полученных в результате обработки данных мы хотим иметь возможность 
 построить графики, показывающие:

  - статистику по использованию сокращений acronym в комментариях в течение суток
  - в какое время суток: 
    утро(с 4 до 12 часов)/ 
    день(с 12 до 17)/ 
    вечер (с 17 до 24)/ ночь(с 0 до 4) - пользователи наиболее активно оставляют комментарии в теме Hardware

*/
object Reader {

  case class Record(
    @JsonProperty("commentId") var commentId: String,
    @JsonProperty("time") var time: Long,
    @JsonProperty("user") var user: String,
    @JsonProperty("topic") var topic: String,
    @JsonProperty("acronym") var acronym: String)

  case class Acronym(var date: String, var value: String, var count: Long) {
    def this() {
      this("", "", 0L)
    }
  }


  object DayPart extends DayPart {
    implicit def dayPartToString(part: DayPart.Value): String = part.toString
  }

  trait DayPart extends Enumeration {
    val day, night,
    evening, morning = Value
  }

  case class ProcessedRecord(partPOfDay: String, count: Long)


  def readCsvStream(): Unit = {
    val filePath = new Path("src/main/resources/acronyms.csv")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val quarterDayFormat = new SimpleDateFormat("hh:mm")
    val hardwareOutputTag = new OutputTag[Record]("hardware"){}

    val targetTopic = "Hardware"
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    val csvSchema = CsvSchema
      .builder()
      .addNumberColumn("commentId")
      .addColumn("time")
      .addColumn("user")
      .addColumn("topic")
      .addColumn("acronym")
      .build()

    val source: FileSource[Record] = FileSource
      .forRecordStreamFormat(
        CsvReaderFormat.forSchema(csvSchema, Types.GENERIC(classOf[Record])),
        filePath)
      .build()


    val separateStreamsFunction =
      new ProcessFunction[Record, Acronym] {
        override def processElement(
          value: Record,
          ctx: ProcessFunction[Record, Acronym]#Context,
          out: Collector[Acronym]): Unit = {

            if (value.topic == targetTopic) ctx.output(hardwareOutputTag, value)

            out.collect(new Acronym(
              dateFormat.format(value.time), value.acronym, 1L))
        }
      }


    val acronymKey = new KeySelector[Acronym, Tuple2[String, String]] {
      override def getKey(keyValue: Acronym): Tuple2[String, String] =
        Tuple2.of(keyValue.date, keyValue.value)
    }

    def getDayPart(time: LocalTime): String = {
      if (time.compareTo(LocalTime.parse("04:00")) == 1 && time.compareTo(LocalTime.parse("12:00")) == -1)
        DayPart.morning
      else if (time.compareTo(LocalTime.parse("12:00")) == 1 && time.compareTo(LocalTime.parse("17:00")) == -1)
        DayPart.day
      else if (time.compareTo(LocalTime.parse("17:00")) == 1 && time.compareTo(LocalTime.parse("00:00")) == -1)
       DayPart.evening
      else
        DayPart.night
    }

    val processHardwareFunction = new ProcessFunction[Record, ProcessedRecord] {
      override def processElement(
          value: Record,
          ctx: ProcessFunction[Record, ProcessedRecord]#Context,
          out: Collector[ProcessedRecord]): Unit = {
        val time = LocalTime.parse(quarterDayFormat.format(value.time))
        val dayPart = getDayPart(time)

        out.collect(ProcessedRecord(dayPart, 1))
      }
    }


    val stream: DataStreamSource[Record] = env
      .fromSource(
        source,
        WatermarkStrategy.noWatermarks(),
        "csv-file"
      )


    val separatedStream = stream
      .process(separateStreamsFunction)

    val countAcronymStream = separatedStream
      .keyBy(acronymKey)
      .sum("count")

    val hardwareStream = separatedStream
      .getSideOutput(hardwareOutputTag)

    val countTargetTopicStream = hardwareStream
      .process(processHardwareFunction)

   // countTargetTopicStream.print()
   // countAcronymStream.print()

    env.execute()
  }


  def main(args: Array[String]): Unit = {
    readCsvStream()
  }
}
