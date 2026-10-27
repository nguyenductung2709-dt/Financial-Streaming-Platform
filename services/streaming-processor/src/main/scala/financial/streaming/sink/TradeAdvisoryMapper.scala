package financial.streaming.sink

import financial.streaming.model.CrossoverResult
import market.v1.Advisory.TradeAdvisory

object TradeAdvisoryMapper {
  def toProto(result: CrossoverResult): TradeAdvisory =
    TradeAdvisory
      .newBuilder()
      .setEmaWindow(EmaWindowMapper.toProto(result.ema))
      .setAction(result.action)
      .setPreviousEma38(result.previousEma38.toPlainString)
      .setPreviousEma100(result.previousEma100.toPlainString)
      .build()
}
