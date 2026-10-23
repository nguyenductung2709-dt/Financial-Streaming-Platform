package financial.streaming.model

import java.math.BigDecimal

/** EMA values in price units for one completed five-minute window. */
final case class EmaResult(
    window: WindowClose,
    ema38: BigDecimal,
    ema100: BigDecimal
)
