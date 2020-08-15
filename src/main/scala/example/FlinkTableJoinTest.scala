package example

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{AnyWithOperations, EnvironmentSettings, FieldExpression, call}
import org.apache.flink.test.util.MiniClusterWithClientResource

object FlinkTableJoinTest {
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

    // for batch programs use ExecutionEnvironment instead of StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(2)

    // create settings
    val setting = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    // create a TableEnvironment
    val tableEnv = StreamTableEnvironment.create(env, setting)

    // create a Table instance for Company
    val companiesMasterFilePath = "data/companies.csv"
    val companies = readCompaniesMaster(companiesMasterFilePath, env)
      .toTable(tableEnv, $"ticker".as("c_ticker"), $"name", $"c_proc_time".proctime)

    // temporal table function
    val func = companies.createTemporalTableFunction($"c_proc_time", $"c_ticker")

    // create a Table instance for Stock
    val stocks = env
      .fromCollection(new UnboundedStocks)
      .toTable(tableEnv, $"ticker".as("s_ticker"), $"price", $"s_proc_time".proctime)

    // join with a temporal table function
    val results = stocks
      .joinLateral(call(func, $"s_proc_time"), $"s_ticker" === $"c_ticker")
      .select($"s_ticker", $"name", $"price")
      .toAppendStream[(String, String, Double)]
      .print

    env.execute()

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
