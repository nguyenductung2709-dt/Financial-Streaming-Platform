# Redpanda with Schema Registry and Console

## Run

```
docker compose up -d        # start redpanda
docker compose ps           # wait for redpanda-0 to be healthy before running the script
bash create-topics.sh       # create topics
```

## Stop

```
docker compose down          # keeps data
docker compose down -v       # deletes data
```

## Things to note

- Use `symbol` as message key

- On Windows, run the script in Git Bash or WSL

## Topics

- `price-events.v1` - valid events read by Flink, 8 partitions
- `price-events-dlq.v1` - rejected rows, 1 partition

## Addresses

From your machine:
- Kafka: `localhost:19092`
- Schema Registry: `http://localhost:18081`

From a Docker container:
- Kafka: `redpanda-0:9092`
- Schema Registry: `http://redpanda-0:8081`

Network name: `financial-streaming-platform_redpanda_network`

Console: http://localhost:8080

## TODO

- Register schemas
- Decide on the number of partitions/brokers after tests