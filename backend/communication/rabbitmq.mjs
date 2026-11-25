import amqp from "amqplib";
import { setTimeout } from "node:timers/promises";

export const shipmentQueue = process.env.RABBITMQ_QUEUE || "msoa.shipping.prepare.v1";
export const deadLetterQueue = shipmentQueue + ".dead";
const url = process.env.RABBITMQ_URL || "amqp://lab4:lab4-local@127.0.0.1:5672";
let publisher;

async function connect() {
  const connection = await amqp.connect(url, { timeout: 2500 });
  connection.on("error", () => {}); // The close handler resets the connection.
  return connection;
}

async function declare(channel) {
  await channel.assertQueue(deadLetterQueue, { durable: true });
  await channel.assertQueue(shipmentQueue, {
    durable: true,
    deadLetterExchange: "",
    deadLetterRoutingKey: deadLetterQueue,
  });
}

export async function publishShipment(request) {
  if (!publisher) {
    const connection = await connect();
    try {
      const channel = await connection.createConfirmChannel();
      const clearPublisher = () => { if (publisher?.channel === channel) publisher = undefined; };
      channel.on("error", clearPublisher);
      channel.on("close", clearPublisher);
      connection.on("close", clearPublisher);
      await declare(channel);
      publisher = { connection, channel };
    } catch (error) {
      await connection.close().catch(() => {});
      throw error;
    }
  }
  const current = publisher;
  try {
    current.channel.sendToQueue(shipmentQueue, Buffer.from(JSON.stringify(request)), {
      persistent: true,
      contentType: "application/json",
      messageId: request.messageId,
    });
    await current.channel.waitForConfirms();
  } catch (error) {
    if (publisher === current) publisher = undefined;
    await current.connection.close().catch(() => {});
    throw error;
  }
}

function decode(message) {
  const request = JSON.parse(message.content.toString());
  const order = request?.order;
  if (request?.type !== "shipment.prepare" || request.version !== 1 ||
      typeof request.messageId !== "string" || typeof order?.id !== "string" ||
      typeof order.userId !== "string" || order.status !== "PAID" ||
      !["name", "address", "city"].every((key) => typeof order.customer?.[key] === "string")) {
    throw new Error("Invalid shipment request");
  }
  return order;
}

export async function consumeShipments(prepare) {
  let reported = false;
  for (;;) {
    let connection;
    try {
      connection = await connect();
      const closed = new Promise((resolve) => connection.once("close", resolve));
      const channel = await connection.createChannel();
      channel.on("error", () => {});
      channel.on("close", () => { void connection.close().catch(() => {}); });
      await declare(channel);
      await channel.prefetch(1);
      await channel.consume(shipmentQueue, async (message) => {
        if (!message) {
          await connection.close().catch(() => {});
          return;
        }
        let order;
        try {
          order = decode(message);
        } catch {
          channel.nack(message, false, false);
          return;
        }
        try {
          prepare(order);
          channel.ack(message);
        } catch (error) {
          console.error("Shipment will be redelivered:", error.message);
          await setTimeout(1000);
          try { channel.nack(message, false, true); } catch { /* connection already closed */ }
        }
      });
      console.log("Shipping consuming", shipmentQueue);
      reported = false;
      await closed;
    } catch (error) {
      if (!reported) console.error("Shipping waiting for RabbitMQ:", error.message);
      reported = true;
      if (connection) await connection.close().catch(() => {});
    }
    await setTimeout(2000);
  }
}
