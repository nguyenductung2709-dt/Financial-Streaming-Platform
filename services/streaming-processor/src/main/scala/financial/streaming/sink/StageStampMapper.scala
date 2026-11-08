package financial.streaming.sink

import financial.streaming.model.StageStamp
import market.v1.Latency

object StageStampMapper {
  def toProto(stamp: StageStamp): Latency.StageStamp =
    Latency.StageStamp
      .newBuilder()
      .setSymbol(stamp.symbol)
      .setWindowEnd(EmaWindowMapper.timestamp(stamp.windowEndMillis))
      .setStage(stamp.stage)
      .setAt(EmaWindowMapper.timestamp(stamp.atMillis))
      .setAdvisoryEmitted(stamp.advisoryEmitted)
      .build()
}
