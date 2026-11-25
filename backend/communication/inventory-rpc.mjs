import grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { fail, secret } from "../shared/runtime.mjs";

const path = fileURLToPath(new URL("../../contracts/inventory.proto", import.meta.url));
const { Inventory } = grpc.loadPackageDefinition(loadSync(path, { longs: Number, defaults: true })).msoa.inventory.v1;
const address = process.env.INVENTORY_GRPC_ADDRESS || "127.0.0.1:50051";
let client;

export function startInventoryRpc(checkStock) {
  const server = new grpc.Server();
  server.addService(Inventory.service, {
    checkStock(call, done) {
      if (call.metadata.get("x-internal-key")[0] !== secret()) {
        return done({ code: grpc.status.PERMISSION_DENIED, message: "Internal credentials required" });
      }
      try {
        const list = call.request.items.map(({ productId, quantity }) => ({ productId, quantity }));
        done(null, { available: checkStock(list) });
      } catch (error) {
        done({ code: error.status === 400 ? grpc.status.INVALID_ARGUMENT : grpc.status.INTERNAL,
          message: error.status === 400 ? error.message : "Inventory check failed" });
      }
    },
  });
  return new Promise((resolve, reject) => {
    server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (error) => {
      if (error) reject(error);
      else {
        console.log("Inventory gRPC listening on", address);
        resolve(server);
      }
    });
  });
}

export async function checkInventory(items) {
  client ??= new Inventory(address, grpc.credentials.createInsecure());
  const metadata = new grpc.Metadata();
  metadata.set("x-internal-key", secret());
  const result = await new Promise((resolve, reject) => {
    client.checkStock({ items }, metadata, { deadline: Date.now() + 1200 }, (error, response) => {
      if (error) reject(error);
      else resolve(response);
    });
  }).catch((error) => {
    if (error.code === grpc.status.INVALID_ARGUMENT) fail(400, error.details);
    fail(503, "Inventory check temporarily unavailable");
  });
  return result.available;
}
