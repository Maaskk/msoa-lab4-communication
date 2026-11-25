import { setTimeout } from "node:timers/promises";
import { assert, database, fail, requireInternal, serve } from "../../shared/runtime.mjs";
import { ensureTopic, kafka, topic } from "../../communication/kafka.mjs";

const groupId = process.env.ANALYTICS_GROUP || "msoa-analytics-v1";
assert(/^[A-Za-z0-9_.-]{1,100}$/.test(groupId), "Invalid analytics group", 500);
const db = database("analytics-" + groupId);
db.exec("CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, type TEXT NOT NULL, payload TEXT NOT NULL)");
const insert = db.prepare("INSERT OR IGNORE INTO events VALUES (?, ?, ?)");

serve("analytics", (req, url) => {
  requireInternal(req);
  if (req.method === "GET" && url.pathname === "/api/analytics/summary") {
    return { groupId, counts: db.prepare("SELECT type, COUNT(*) AS count FROM events GROUP BY type ORDER BY type").all() };
  }
  if (req.method === "GET" && url.pathname === "/api/analytics/events") {
    const limit = Number(url.searchParams.get("limit") || 100);
    assert(Number.isInteger(limit) && limit >= 1 && limit <= 500, "limit must be between 1 and 500");
    return db.prepare("SELECT payload FROM events ORDER BY rowid DESC LIMIT ?").all(limit)
      .map((row) => JSON.parse(row.payload));
  }
  fail(404, "Not found");
});

let reported = false;
for (;;) {
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  try {
    await ensureTopic();
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        let event;
        try {
          event = JSON.parse(message.value.toString());
        } catch {
          console.error("Skipping unreadable analytics record", message.offset);
          return;
        }
        if (event?.version !== 1 || typeof event.eventId !== "string" || typeof event.type !== "string") {
          console.error("Skipping unsupported analytics record", message.offset);
          return;
        }
        // Commit the projection before Kafka can commit this offset.
        insert.run(event.eventId, event.type, JSON.stringify(event));
      },
    });
    console.log("Analytics reading", topic, "as", groupId);
    break;
  } catch (error) {
    if (!reported) console.error("Analytics waiting for Kafka:", error.message);
    reported = true;
    await consumer.disconnect().catch(() => {});
    await setTimeout(2000);
  }
}
