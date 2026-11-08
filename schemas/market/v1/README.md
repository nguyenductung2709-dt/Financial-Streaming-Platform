# Guidelines for schemas

The `.proto` files define the messages exchanged through Redpanda. They are the
source of truth; regenerate language bindings rather than editing generated code.

| Schema | Message | Topic |
| --- | --- | --- |
| [events.proto](events.proto) | `market.v1.PriceEvent` | `trade-events` |
| [dlq.proto](dlq.proto) | `market.v1.RejectedRow` | `trade-events-dlq` |
| [ema.proto](ema.proto) | `market.v1.EmaWindow` | `ema-windows` (planned output) |
| [advisory.proto](advisory.proto) | `market.v1.TradeAdvisory` | `trade-advisories` (planned output) |
| [latency.proto](latency.proto) | `market.v1.StageStamp` | `latency-stamps` (measurement only, not domain data) |

`EmaWindow` represents one symbol's completed window: `[window_start, window_end)`.
The closing price uses integer micros; EMA values use plain decimal strings in
price units, without exponent notation. For example, a closing price of `418.21`
is `418210000` micros, while an EMA value might be `21.44666666666666666666666666666667`.
Use `BigDecimal.toPlainString` when encoding EMA values. The current calculation
rounds each update to 34 significant digits with HALF_EVEN rounding.

`TradeAdvisory` embeds the current `EmaWindow`, a recommended BUY/SELL action, and both previous
EMA values as decimal price strings. An unspecified action is not a trading signal.
The embedded window supplies the symbol, boundaries, closing price, and current
EMA values. Previous EMAs are `"0"` when no previous state exists.

## Schema evolution

Suppose `string symbol = 2;` has already been sent to Redpanda. In those stored
records, field number `2` means a symbol.

- **Starting over from the CSV?** You can change the `.proto`, generate code
  again, send the CSV to a new empty topic, and use that topic in the processor.
- **Keeping the old Redpanda records?** Keep field `2` as a string symbol. Do not
  use `2` for a price. Keep price units the same too: `last_price_micros` must
  still mean millionths. Give any new field an unused number.
- **Removing a field but keeping old records?** If you remove
  `source_row = 6`, add `reserved 6;` inside `PriceEvent` so `6` cannot be
  assigned to a different field later.

See the [Protobuf schema evolution guide](https://protobuf.dev/programming-guides/proto3/#updating)
and [best practices](https://protobuf.dev/best-practices/dos-donts/).

## Protobuf code generation

Install `protoc` v33.4 and the Go plugin version used by CI:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
```

From the repository root, regenerate Go and Java code for **all `.proto` files** under
`schemas`, including `events.proto`, `dlq.proto`, `ema.proto`, `advisory.proto`, and `latency.proto`:

```sh
make generate-protobuf
```

The target adds Go's binary directory to `PATH` so `protoc` can find the plugin.
Commit the schema changes together with the regenerated files in `gen/`.
CI regenerates all schemas and checks that those committed files match.

Generated code is separated by language:

```text
gen/
├── go/market/v1/       # Go bindings used by the data producer
└── java/market/v1/     # Java bindings usable from Scala
```

When setting up the processor's `build.sbt`, include the shared Java sources:

```scala
Compile / unmanagedSourceDirectories +=
  (baseDirectory.value / "../../gen/java").getCanonicalFile

libraryDependencies +=
  "com.google.protobuf" % "protobuf-java" % "4.33.4"
```
