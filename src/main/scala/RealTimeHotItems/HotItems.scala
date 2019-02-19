package RealTimeHotItems

import java.sql.Timestamp
import java.util
import java.util.Comparator

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.CsvFormatter
import utils.CsvFormatter.UserBehavior

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2019/2/14.
  */
object HotItems {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    var pair = CsvFormatter.format()

    // 创建数据源，得到 UserBehavior 类型的 DataStream
    val dataSource: DataStream[UserBehavior] = env.createInput(pair.getValue0)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 过滤点击事件，并抽取出事件时间和生成 watermark：原始数据单位秒，将其转成毫秒
    val timedData = dataSource.filter(_.behavior == "pv").assignAscendingTimestamps(_.timestamp * 1000)

    // 窗口统计点击量
    timedData.keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()
//      .map(x => {
//      println(s"itemId:${x.itemId},viewCount:${x.viewCount},windowEnd:${x.windowEnd}")
//    })

    env.execute("Hot Items Job")
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator() = 0L

    override def add(userBehavior: UserBehavior, acc: Long) = acc + 1L

    override def getResult(acc: Long) = acc

    override def merge(acc1: Long, acc2: Long) = acc1 + acc2

  }

  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    @throws[Exception]
    def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount.of(itemId, window.getEnd, count))
    }
  }

  /** 商品点击量(窗口操作的输出类型) */
  object ItemViewCount {
    def of(itemId: Long, windowEnd: Long, viewCount: Long): ItemViewCount = {
      val result = new ItemViewCount
      result.itemId = itemId
      result.windowEnd = windowEnd
      result.viewCount = viewCount
      result
    }
  }

  class ItemViewCount {
    var itemId = 0L // 商品ID
    var windowEnd = 0L // 窗口结束时间戳
    var viewCount = 0L // 商品的点击量
  }


  /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, HotItems.ItemViewCount, String] {

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private var itemState: ListState[HotItems.ItemViewCount] = null

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val itemsStateDesc = new ListStateDescriptor[HotItems.ItemViewCount]("itemState-state", classOf[HotItems.ItemViewCount])
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    @throws[Exception]
    override def processElement(input: HotItems.ItemViewCount, context: KeyedProcessFunction[Tuple, HotItems.ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 每条数据都保存到状态中
      itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, HotItems.ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 获取收到的所有商品点击量
      val allItems: util.List[HotItems.ItemViewCount] = new util.ArrayList[HotItems.ItemViewCount]

      for (item <- itemState.get) {
        allItems.add(item)
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      allItems.sort(new Comparator[HotItems.ItemViewCount]() {
        override def compare(o1: HotItems.ItemViewCount, o2: HotItems.ItemViewCount): Int = (o2.viewCount - o1.viewCount).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while ( {
        i < allItems.size && i < topSize
      }) {
        val currentItem: HotItems.ItemViewCount = allItems.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  商品ID=").append(currentItem.itemId).append("  浏览量=").append(currentItem.viewCount).append("\n")

        {
          i += 1; i - 1
        }
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

}
