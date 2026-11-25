# LAB4 · Analysis and implementation

## 1. High-performance communication

Orders needs an immediate stock decision before it accepts checkout. JSON REST works for this interaction, but repeated serialization and parsing and larger payloads can add overhead. A long-lived HTTP/2 connection, a typed request, a compact binary format and a short deadline fit this synchronous path. gRPC provides those properties through [inventory.proto](../contracts/inventory.proto). This lab verifies the behavior and does not claim a measured latency improvement.

Orders sends `CheckStock(items)` with internal credentials and a 1.2-second deadline. Inventory uses the same availability rule as the existing REST check. An unavailable item returns 409 through Orders. An unreachable inventory RPC returns 503. The existing SQLite reservation transaction still decides whether to decrement stock, because another checkout can occur after a successful check. This internal change leaves the public REST APIs and reservation behavior intact.

## 2. Reliable background processing

A payment can succeed without waiting for Shipping to prepare a parcel. A synchronous REST call would make the customer wait and would need its own durable retry system during a Shipping outage. RabbitMQ handles the work queue while Payments returns the existing success response.

Payments records the capture and its outgoing message in the same SQLite transaction. A publisher delivers the saved message to the durable queue `msoa.shipping.prepare.v1` as a persistent message and waits for a broker confirmation before dropping the outbox row. Shipping acknowledges the message after its SQLite transaction finishes. It uses the order ID to avoid duplicate shipments after redelivery. A cancellation recorded before delivery prevents a late message from recreating the shipment. Malformed messages go to `msoa.shipping.prepare.v1.dead`.

The shipment request is a JSON command sent by Payments and consumed by Shipping. Only Shipping needs the address, so the command stays outside the analytics stream.

```json
{
  "type": "shipment.prepare",
  "version": 1,
  "messageId": "payment-id",
  "order": {
    "id": "order-id",
    "userId": "customer-id",
    "status": "PAID",
    "customer": {"name": "Example", "address": "12 Example Street", "city": "Casablanca"}
  }
}
```

## 3. Event retention and replay

Analytics needs data from before it was deployed and from any period it was offline. A work queue typically removes a message after acknowledgement. Kafka retains an append-only log for a configured period and each consumer group tracks its own offsets. That lets Analytics resume or rebuild its view independently of other readers.

Orders, Payments and Shipping use outboxes to emit versioned JSON events to `msoa.business-events.v1`. The topic has three partitions and a seven-day retention period. Events are keyed by order ID and contain operational fields only. Kafka orders records within each partition by broker arrival. Analytics uses an event ID to deduplicate records and stores a local SQLite projection per group. A new group subscribes from the earliest retained offset.

```json
{
  "eventId": "event-uuid",
  "version": 1,
  "type": "payment.captured",
  "source": "payments",
  "orderId": "order-id",
  "occurredAt": "2026-11-25T19:00:00.000Z",
  "data": {"amount": 8499}
}
```

Other event types include `order.created`, `order.cancelled`, `payment.refunded`, `shipment.prepared`, `shipment.status_changed` and `shipment.cancelled`. Each producer owns its state changes. Analytics consumes and summarizes events and never coordinates checkout. The local setup uses a single node per broker for demonstration, with persistent volumes and no broker replication.

## Work split for the demo

| Pair of service responsibilities | LAB4 explanation |
| --- | --- |
| Orders and Inventory | Show the `.proto` contract, the Orders client, the Inventory handler and the stock decision. |
| Payments and Shipping | Show the payment outbox entry, the RabbitMQ queue and a shipment processed when Shipping returns. |
| Analytics and cross-service event publishers | Show the common Kafka event envelope, the analytics projection, and replay from a fresh group. |

## References

- [gRPC Node basics](https://grpc.io/docs/languages/node/basics/)
- [RabbitMQ acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/)
