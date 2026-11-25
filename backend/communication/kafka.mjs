import { Kafka, logLevel, Partitioners } from "kafkajs";

export const topic = process.env.KAFKA_TOPIC || "msoa.business-events.v1";
export const kafka = new Kafka({
  clientId: "msoa-lab4",
  brokers: (process.env.KAFKA_BROKERS || "127.0.0.1:9092").split(","),
  connectionTimeout: 2000,
  requestTimeout: 3000,
  retry: { retries: 2 },
  logLevel: logLevel.NOTHING,
});
const producer = kafka.producer({ allowAutoTopicCreation: false, createPartitioner: Partitioners.DefaultPartitioner });
let connected = false;

export async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({ topics: [{
      topic, numPartitions: 3, replicationFactor: 1,
      configEntries: [{ name: "retention.ms", value: "604800000" }, { name: "cleanup.policy", value: "delete" }],
    }] });
  } finally {
    await admin.disconnect();
  }
}

export async function publishEvent(event) {
  try {
    if (!connected) {
      await ensureTopic();
      await producer.connect();
      connected = true;
    }
    await producer.send({ topic, acks: -1, messages: [{ key: event.orderId, value: JSON.stringify(event) }] });
  } catch (error) {
    connected = false;
    await producer.disconnect().catch(() => {});
    throw error;
  }
}
