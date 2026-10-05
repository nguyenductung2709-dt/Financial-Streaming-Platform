# Guidelines for schemas

[`market/v1/events.proto`](events.proto) defines the Protobuf messages
exchanged through Redpanda. The `.proto` file is the source of truth; regenerate
language bindings from it rather than editing generated code by hand.

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

From the repository root, regenerate Go code for **all `.proto` files** under
`schemas`, including `events.proto` and `dlq.proto`:

```sh
make generate-protobuf
```

The target adds Go's binary directory to `PATH` so `protoc` can find the plugin.
Commit the schema changes together with the regenerated files in `gen/`.
CI regenerates all schemas and checks that those committed files match.
