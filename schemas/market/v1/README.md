# Schemas

These `.proto` files define every message that goes through Redpanda. The code in
`gen/` is generated from them, so edit the schemas, never the generated code.

- [events.proto](events.proto): `PriceEvent`, on `trade-events`
- [dlq.proto](dlq.proto): `RejectedRow`, on `trade-events-dlq`
- [ema.proto](ema.proto): `EmaWindow`, on `ema-windows`
- [advisory.proto](advisory.proto): `TradeAdvisory`, on `trade-advisories`
- [latency.proto](latency.proto): `StageStamp`, on `latency-stamps` and
  `materialize-stamps`. These are used only for measurements.

Prices are integers in micros, so 418.21 is sent as `418210000`. EMA values are
decimal strings in price units and never use exponent notation, for example
`"21.44666666666666666666666666666667"`. On the JVM, encode them with
`BigDecimal.toPlainString`.

An `EmaWindow` covers `[window_start, window_end)` for one symbol. A `TradeAdvisory`
contains its `EmaWindow`, the action and the previous two EMA values. The previous
values are `"0"` for a symbol's first window. An unspecified action is not a signal. If one reaches `trade-advisories`, the materializer rejects it and stops.

## Regenerating code

You need `protoc` 33.4 and the Go plugin version that CI uses:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
make generate-protobuf
make generate-python-protobuf
```

The first command regenerates the Go and Java bindings, and the second the Python ones
that validation and monitoring use. Commit the files in `gen/` together with the schema
change. CI regenerates them and fails if anything differs.

## Changing a schema safely

Records already stored in a topic are decoded by field number. If you're keeping
existing topic data, never reuse or retype a field number, and don't change what a
field's units mean. Give new fields new numbers, and mark removed ones as `reserved`.
If you're going to replay the CSV into empty topics anyway, you can change whatever you
like. The
[Protobuf guide](https://protobuf.dev/programming-guides/proto3/#updating) has the
details.
