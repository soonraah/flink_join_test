package example

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.test.util.MiniClusterWithClientResource

object FlinkJoinTest {
  // See https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/testing.html#testing-flink-jobs
  private val FlinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration
    .Builder()
      .setNumberSlotsPerTaskManager(2)
      .setNumberTaskManagers(1)
      .build
  )

  def main(args: Array[String]): Unit = {
    FlinkCluster.before()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val companiesMasterFilePath = "data/companies.csv"

    val companies = readCompaniesMaster(companiesMasterFilePath, env)
      .broadcast(new MapStateDescriptor(
        "CompanyState",
        classOf[String],    // ticker
        classOf[String]     // name
      ))

    // the broadcast state pattern
    // See https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/state/broadcast_state.html
    env
      .fromCollection(new UnboundedStocks)
      .connect(companies)
      .process(new StockBroadcastProcessFunction)
      .print()

    env.execute("flink join test")

    FlinkCluster.after()
  }

  private def readCompaniesMaster(companiesMasterFilePath: String,
                                  env: StreamExecutionEnvironment): DataStream[Company] = {
    env
      .readFile(
        new TextInputFormat(new Path(companiesMasterFilePath)),
        companiesMasterFilePath,
        FileProcessingMode.PROCESS_CONTINUOUSLY,
        10 * 1000
      )
      .map { line =>
        val items = line.split(",")
        Company(items(0), items(1))
      }
  }
}
