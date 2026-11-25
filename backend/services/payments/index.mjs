import {
  serve,
  database,
  get,
  put,
  all,
  assert,
  fail,
  id,
  now,
  actor,
  own,
  call,
  requireInternal,
  txn,
} from "../../shared/runtime.mjs";
import { lab4, initOutbox, enqueue, emit, dispatch } from "../../communication/outbox.mjs";
import { publishEvent } from "../../communication/kafka.mjs";
import { publishShipment } from "../../communication/rabbitmq.mjs";

const db = database("payments");
initOutbox(db);
dispatch(db, "rabbitmq", publishShipment);
dispatch(db, "kafka", publishEvent);
serve("payments", async (req, url, b) => {
  const p = url.pathname;
  let m;
  if (
    (m = p.match(/^\/internal\/payments\/([^/]+)\/(capture|refund)$/)) &&
    req.method === "POST"
  ) {
    requireInternal(req);
    let existing = all(db).find((x) => x.orderId === m[1]);
    if (m[2] === "refund") {
      if (existing && existing.status === "SUCCESS") {
        txn(db, () => {
          put(db, { ...existing, status: "REFUNDED" });
          emit(db, "payment.refunded", "payments", existing.orderId, { amount: existing.amount });
        });
      }
      return { success: true };
    }
    assert(
      b.order?.id === m[1] && ["CARD", "CASH"].includes(b.method),
      "Invalid payment",
    );
    if (existing) {
      assert(existing.status === "SUCCESS", "Payment already refunded", 409);
      return existing;
    }
    const payment = {
      id: id(),
      orderId: m[1],
      userId: b.order.userId,
      amount: b.order.total,
      method: b.method,
      status: "SUCCESS",
      createdAt: now(),
    };
    return txn(db, () => {
      put(db, payment);
      if (lab4) {
        const { id: orderId, userId, customer } = b.order;
        enqueue(db, "rabbitmq", {
          type: "shipment.prepare", version: 1, messageId: payment.id,
          order: { id: orderId, userId, customer, status: "PAID" },
        });
        emit(db, "payment.captured", "payments", orderId, { amount: payment.amount });
      }
      return payment;
    });
  }
  const user = await actor(req);
  if (p === "/api/payments" && req.method === "GET")
    return all(db).filter((x) => x.userId === user.id);
  if (
    (m = p.match(/^\/api\/payments\/order\/([^/]+)$/)) &&
    req.method === "GET"
  ) {
    own(user, await call("orders", "/api/orders/" + m[1]));
    const r = all(db).find((x) => x.orderId === m[1]);
    assert(r, "Payment not found", 404);
    return r;
  }
  if (p === "/api/payments" && req.method === "POST") {
    assert(
      typeof b.orderId === "string" && ["CARD", "CASH"].includes(b.method),
      "Invalid order or payment method",
    );
    own(user, await call("orders", "/api/orders/" + b.orderId));
    return call("orders", `/internal/orders/${b.orderId}/action`, "POST", {
      action: "PAY",
      method: b.method,
    });
  }
  fail(404, "Not found");
});
