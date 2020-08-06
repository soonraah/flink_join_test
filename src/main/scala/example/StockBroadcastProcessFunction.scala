package example

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

class StockBroadcastProcessFunction
  extends BroadcastProcessFunction[Stock, Company, (String, String, Double)] {

  private val StateDescriptor = new MapStateDescriptor(
    "CompanyState",
    classOf[String],    // ticker
    classOf[String]     // name
  )

  override def processElement(value: Stock,
                              ctx: BroadcastProcessFunction[Stock, Company, (String, String, Double)]#ReadOnlyContext,
                              out: Collector[(String, String, Double)]): Unit = {
    val companyName = ctx.getBroadcastState(StateDescriptor).get(value.ticker)
    out.collect((value.ticker, Option(companyName).getOrElse("-"), value.price))
  }

  override def processBroadcastElement(value: Company,
                                       ctx: BroadcastProcessFunction[Stock, Company, (String, String, Double)]#Context,
                                       out: Collector[(String, String, Double)]): Unit = {
    ctx.getBroadcastState(StateDescriptor).put(value.ticker, value.name)
  }
}
