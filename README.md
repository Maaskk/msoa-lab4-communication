# LAB4 · Inter-service communication

This lab extends the six e-commerce services from [LAB3](https://github.com/Maaskk/msoa-lab3-api-contracts). The frontend and the existing REST contracts still use the same endpoints. A seventh service, Analytics, reads historical business events in LAB4 mode.

| Scenario | Integration | What to demonstrate |
| --- | --- | --- |
| Check stock during checkout | Orders → Inventory over gRPC | Available stock allows checkout; insufficient stock returns HTTP 409. |
| Prepare shipment after payment | Payments → RabbitMQ → Shipping | Pay while Shipping is offline, then start it and observe the queued request being processed. |
| Retain and replay business events | Orders, Payments and Shipping → Kafka → Analytics | Start a new consumer group after an order and recover its three historical events. |

## Run locally

Requires Node.js 24+, npm and Docker Compose.

```sh
npm ci
docker compose -f compose.lab4.yml up -d
INTERNAL_KEY=local-lab4-demo-key-32-characters npm run start:lab4
```

The six REST services listen on ports 8081–8086, Analytics on 8087, gRPC on 50051, RabbitMQ on 5672 and Kafka on 9092. The brokers and gRPC endpoint bind to loopback for this lab. The fixed credentials above and the Compose broker credentials are for local demonstration.

To use the inherited frontend, run `npm ci --prefix frontend` and `npm run dev --prefix frontend` in another terminal. The sample login is `demo@msoa.shop` / `demo123`. After placing and paying for an order, inspect the analytics projection.

```sh
curl -s -H 'X-Internal-Key: local-lab4-demo-key-32-characters' \
  http://127.0.0.1:8087/api/analytics/summary
```

Open a second Analytics instance with a fresh group to replay earlier events.

```sh
INTERNAL_KEY=local-lab4-demo-key-32-characters \
ANALYTICS_GROUP=msoa-demo-replay PORT=8088 \
node backend/services/analytics/index.mjs
```

Query the same summary endpoint on port 8088. Use a new group name on each fresh replay. A group with saved offsets resumes its position. Broker data persists in the Compose volumes.

## Verify

```sh
npm test
npm run test:lab4
```

The first command runs the existing contract, integration and Postman checks. The second uses real gRPC, RabbitMQ and Kafka and tests recovery and replay. See the [recorded results](evidence/lab4-verification.txt) and [analysis and communication contracts](docs/LAB4.md).
