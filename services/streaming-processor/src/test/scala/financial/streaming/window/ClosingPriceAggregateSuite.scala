package financial.streaming.window

import com.google.protobuf.Timestamp
import market.v1.Events.{PriceEvent, SecurityType}

class ClosingPriceAggregateSuite extends munit.FunSuite {
  private val aggregate = new ClosingPriceAggregate

  private def trade(
      seconds: Long,
      nanos: Int = 0,
      row: Long = 1L,
      priceMicros: Long = 28040000L
  ): PriceEvent = PriceEvent
    .newBuilder()
    .setSymbol("RUI.FR")
    .setSecurityType(SecurityType.SECURITY_TYPE_EQUITY)
    .setLastPriceMicros(priceMicros)
    .setSourceRow(row)
    .setTradeTime(Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos))
    .build()

  private def closingTrade(events: Seq[PriceEvent]): PriceEvent =
    aggregate.getResult(events.foldLeft(aggregate.createAccumulator()) { (accumulator, event) =>
      aggregate.add(event, accumulator)
    })

  test("a new accumulator is empty and the first trade becomes the complete result") {
    val empty = aggregate.createAccumulator()
    assert(!empty.hasTradeTime)
    val first = trade(1636700400L, nanos = 179000000, row = 728610L)
    val result = aggregate.add(first, empty)
    assertEquals(aggregate.getResult(result), first)
    assert(!empty.hasTradeTime)
  }

  test("later seconds replace the candidate even with a smaller price, row and nanoseconds") {
    val previous = trade(100L, nanos = 999999999, row = 100L, priceMicros = 99999999L)
    val later = trade(101L, nanos = 0, row = 2L, priceMicros = 1L)
    assertEquals(closingTrade(Seq(previous, later)), later)
  }

  test("an older arriving trade does not replace the closing candidate") {
    val latest = trade(101L, priceMicros = 1000000L)
    val older = trade(100L, nanos = 999999999, row = 999L, priceMicros = 99999999L)
    assertEquals(closingTrade(Seq(latest, older)), latest)
  }

  test("nanoseconds select the closing trade within the same second in either arrival order") {
    val early = trade(100L, nanos = 123000001, row = 99L)
    val late = trade(100L, nanos = 123000002, row = 1L)
    assertEquals(closingTrade(Seq(early, late)), late)
    assertEquals(closingTrade(Seq(late, early)), late)
  }

  test("equal timestamps use unsigned source-row ordering for both add and merge") {
    // Negative Long values represent uint64 rows above Long.MaxValue.
    val ascendingRows = Seq(1L, Long.MaxValue, Long.MinValue, -1L)
    ascendingRows.sliding(2).foreach { rows =>
      val lower = trade(100L, row = rows.head)
      val higher = trade(100L, row = rows.last)
      assertEquals(closingTrade(Seq(lower, higher)), higher)
      assertEquals(closingTrade(Seq(higher, lower)), higher)
      assertEquals(aggregate.merge(lower, higher), higher)
      assertEquals(aggregate.merge(higher, lower), higher)
    }
  }

  test("every arrival order produces the same closing trade without losing exact uint64 price") {
    val expected = trade(102L, nanos = 10, row = 20L, priceMicros = -1L)
    val events = Seq(
      trade(100L, row = 100L),
      trade(102L, nanos = 9, row = 999L),
      trade(102L, nanos = 10, row = 19L),
      expected
    )
    events.permutations.foreach { order =>
      val result = closingTrade(order)
      assertEquals(result, expected)
      assertEquals(
        java.lang.Long.toUnsignedString(result.getLastPriceMicros),
        "18446744073709551615"
      )
    }
  }

  test("merging with an empty accumulator preserves the selected trade") {
    val empty = aggregate.createAccumulator()
    val selected = trade(100L)
    assertEquals(aggregate.merge(empty, selected), selected)
    assertEquals(aggregate.merge(selected, empty), selected)
    assertEquals(aggregate.merge(empty, empty), empty)
  }

  test("merging partial aggregates gives the same closing trade as aggregating all events") {
    val expected = trade(103L, nanos = 5, row = 8L)
    val events = Seq(trade(101L), trade(103L, nanos = 5, row = 7L), expected, trade(102L))
    (0 to events.size).foreach { split =>
      val first = closingTrade(events.take(split))
      val second = closingTrade(events.drop(split))
      assertEquals(aggregate.getResult(aggregate.merge(first, second)), expected)
      assertEquals(aggregate.getResult(aggregate.merge(second, first)), expected)
    }
  }

  test("merge grouping and duplicate delivery do not change the closing trade") {
    val first = trade(100L, row = 999L)
    val second = trade(101L, nanos = 1, row = 1L)
    val latest = trade(101L, nanos = 2, row = 2L)
    assertEquals(aggregate.merge(aggregate.merge(first, second), latest), latest)
    assertEquals(aggregate.merge(first, aggregate.merge(second, latest)), latest)
    assertEquals(aggregate.merge(latest, latest), latest)
    assertEquals(closingTrade(Seq(first, latest, second, latest)), latest)
  }
}
