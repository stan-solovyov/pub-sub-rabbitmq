import { encode } from "@msgpack/msgpack";
import type { ConfirmChannel } from "amqplib";


export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const bytes = Buffer.from(JSON.stringify(value));
    ch.publish(exchange, routingKey, bytes, { contentType: "application/json" }, (err, ok) => {
      if (err) {
        reject(err);
      } else {
        console.log(`Published message to exchange "${exchange}" with routing key "${routingKey}":`, value);
        resolve();
      }
    });
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const encoded = Buffer.from(encode(value));
    ch.publish(exchange, routingKey, encoded, { contentType: "application/msgpack" }, (err, ok) => {
      if (err) {
        reject(err);
      } else {
        console.log(`Published message to exchange "${exchange}" with routing key "${routingKey}":`, value);
        resolve();
      }
    });
  });
}