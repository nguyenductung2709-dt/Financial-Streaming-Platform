package financial.streaming.model

import java.math.BigDecimal
import market.v1.Advisory.AdvisoryAction

final case class CrossoverResult(
    ema: EmaResult,
    action: AdvisoryAction,
    previousEma38: BigDecimal,
    previousEma100: BigDecimal
)
