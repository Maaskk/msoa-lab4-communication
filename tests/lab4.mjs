import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import amqp from "amqplib";

const names = ["auth", "catalog", "inventory", "orders", "payments", "shipping", "analytics"];
const ports = Object.fromEntries(names.map((name, index) => [name, 18081 + index]));
const runId = Date.now().toString(36);
const data = mkdtempSync(join(tmpdir(), "msoa-lab4-"));
const key = "lab4-integration-local-key-for-testing";
const env = {
  LAB4: "1", DATA_DIR: data, INTERNAL_KEY: key, INVENTORY_GRPC_ADDRESS: "127.0.0.1:55051",
  KAFKA_TOPIC: `msoa.lab4.test.${runId}`, RABBITMQ_QUEUE: `msoa.shipping.test.${runId}`,
  ...Object.fromEntries(names.map((name) => [name.toUpperCase() + "_URL", `http://127.0.0.1:${ports[name]}`])),
};
const children = [];
let logs = "";
let token;
let connection;

function start(name, extra = {}) {
  const child = spawn(process.execPath, [`backend/services/${name}/index.mjs`], {
    env: { ...process.env, ...env, PORT: String(ports[name]), ...extra },
    stdio: ["ignore", "pipe", "pipe"],
  });
  for (const stream of [child.stdout, child.stderr]) {
    stream.on("data", (data) => { logs += data; });
  }
  children.push(child);
  return child;
}

async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGTERM");
  await exited;
}

async function request(name, path, { body, expected = 200, internal = false, port = ports[name] } = {}) {
  const response = await fetch(`http://127.0.0.1:${port}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: "Bearer " + token } : {}),
      ...(internal ? { "X-Internal-Key": key } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(3500),
  });
  const json = await response.json();
  assert.equal(response.status, expected, JSON.stringify(json));
  return json;
}

async function until(description, check) {
  let error;
  for (let i = 0; i < 80; i++) {
    try { return await check(); } catch (e) { error = e; }
    await setTimeout(250);
  }
  throw new Error(description, { cause: error });
}

try {
  for (const name of names.slice(0, 5)) start(name);
  await until("Core services did not start", () => Promise.all(names.slice(0, 5).map((n) => request(n, "/health"))));
  const login = await request("auth", "/api/auth/login", { body: { email: "demo@msoa.shop", password: "demo123" } });
  token = login.token;

  process.env.INVENTORY_GRPC_ADDRESS = env.INVENTORY_GRPC_ADDRESS;
  process.env.INTERNAL_KEY = key;
  const { checkInventory } = await import("../backend/communication/inventory-rpc.mjs");
  assert.equal(await checkInventory([{ productId: 1, quantity: 1 }]), true);
  assert.equal(await checkInventory([{ productId: 1, quantity: 9999 }]), false);
  console.log("PASS gRPC CheckStock returns both available and out-of-stock results");

  const orderPayload = {
    userId: login.user.id,
    customer: { name: "Lab Test", address: "123 Test Street", city: "Casablanca" },
    items: [{ productId: 1, quantity: 1 }],
  };
  await request("orders", "/api/orders", { body: { ...orderPayload, items: [{ productId: 1, quantity: 9999 }] }, expected: 409 });
  const order = await request("orders", "/api/orders", { body: orderPayload });
  assert.equal(order.status, "PENDING_PAYMENT");
  const paymentPayload = { orderId: order.id, method: "CARD" };
  const payment = await request("payments", "/api/payments", { body: paymentPayload });
  assert.equal(payment.status, "SUCCESS");
  assert.equal((await request("orders", "/api/orders/" + order.id)).status, "PAID");
  console.log("PASS payment succeeds with Shipping offline");

  connection = await amqp.connect(process.env.RABBITMQ_URL || "amqp://lab4:lab4-local@127.0.0.1:5672");
  const channel = await connection.createChannel();
  await channel.assertQueue(env.RABBITMQ_QUEUE + ".dead", { durable: true });
  await channel.assertQueue(env.RABBITMQ_QUEUE, {
    durable: true,
    deadLetterExchange: "",
    deadLetterRoutingKey: env.RABBITMQ_QUEUE + ".dead",
  });
  await until("Shipment was not queued", async () => {
    assert.equal((await channel.checkQueue(env.RABBITMQ_QUEUE)).messageCount, 1);
  });
  start("shipping");
  const shipment = await until("Shipping did not process the queued message", () =>
    request("shipping", "/api/shipments/order/" + order.id));
  assert.equal(shipment.status, "PREPARING");
  await until("Shipping did not acknowledge the request", async () => {
    assert.equal((await channel.checkQueue(env.RABBITMQ_QUEUE)).messageCount, 0);
  });
  // Re-publish the same logical job to verify idempotency on delivery.
  channel.sendToQueue(env.RABBITMQ_QUEUE, Buffer.from(JSON.stringify({
    type: "shipment.prepare", version: 1, messageId: payment.id,
    order: { id: order.id, userId: login.user.id, customer: orderPayload.customer, status: "PAID" },
  })), { persistent: true });
  assert.equal((await request("payments", "/api/payments", { body: paymentPayload })).id, payment.id);
  assert.deepEqual((await request("shipping", "/api/shipments")).map((s) => s.id), [shipment.id]);
  console.log("PASS RabbitMQ retains the job and Shipping creates one shipment on recovery");

  const expected = ["order.created", "payment.captured", "shipment.prepared"];
  for (const [index, group] of ["analytics", "analytics-replay"].entries()) {
    const port = ports.analytics + index;
    start("analytics", { PORT: String(port), ANALYTICS_GROUP: group });
    const events = await until(`Kafka group ${group} did not replay the order`, async () => {
      const all = await request("analytics", "/api/analytics/events", { internal: true, port });
      const matching = all.filter((event) => event.orderId === order.id);
      assert.deepEqual(matching.map((event) => event.type).sort(), expected);
      return matching;
    });
    assert.equal(new Set(events.map((event) => event.eventId)).size, expected.length);
    console.log(`PASS Kafka group ${group} replayed 3 events produced before the group started`);
  }
  await channel.deleteQueue(env.RABBITMQ_QUEUE);
  await channel.deleteQueue(env.RABBITMQ_QUEUE + ".dead");
} catch (error) {
  console.error(logs, error);
  process.exitCode = 1;
} finally {
  if (connection) await connection.close().catch(() => {});
  await Promise.all(children.map(stop));
  rmSync(data, { recursive: true, force: true });
}
