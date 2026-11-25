import { id, now } from "../shared/runtime.mjs";

export const lab4 = process.env.LAB4 === "1";

export function initOutbox(db) {
  if (lab4) db.exec(`
    CREATE TABLE IF NOT EXISTS outbox (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      transport TEXT NOT NULL,
      payload TEXT NOT NULL
    )
  `);
}

// Call inside the same SQLite transaction as the business write.
export function enqueue(db, transport, payload) {
  if (lab4) db.prepare("INSERT INTO outbox(transport, payload) VALUES (?, ?)").run(transport, JSON.stringify(payload));
}

export function emit(db, type, source, orderId, data = {}) {
  enqueue(db, "kafka", { eventId: id(), version: 1, type, source, orderId, occurredAt: now(), data });
}

export function dispatch(db, transport, publish) {
  if (!lab4) return;
  const next = db.prepare("SELECT seq, payload FROM outbox WHERE transport = ? ORDER BY seq LIMIT 1");
  const remove = db.prepare("DELETE FROM outbox WHERE seq = ?");
  let running = false;
  let failed = false;
  const drain = async () => {
    if (running) return;
    running = true;
    try {
      let row;
      while ((row = next.get(transport))) {
        await publish(JSON.parse(row.payload));
        remove.run(row.seq);
        failed = false;
      }
    } catch (error) {
      if (!failed) console.error(`${transport} outbox retry pending:`, error.message);
      failed = true;
    } finally {
      running = false;
    }
  };
  setInterval(drain, 500).unref();
  void drain();
}
