package example

import scala.util.Random

/**
 * Iterator to generate unbounded stock data
 */
class UnboundedStocks extends Iterator[Stock] with Serializable {
  override def hasNext: Boolean = true  // unbounded

  override def next(): Stock = {
    Thread.sleep(1000)
    val tickers = Seq("GOOGL", "AAPL", "FB", "AMZN")
    val ticker = tickers(Random.nextInt(tickers.size))  // one of GAFA
    val price = 100 + Random.nextDouble() * 300         // random price
    Stock(ticker, price)
  }
}
