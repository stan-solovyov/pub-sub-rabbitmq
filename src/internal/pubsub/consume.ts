import amqp, { type Channel } from "amqplib";
import type { AckType } from "../../client/handlers.js";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const queue = await channel.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {
      "x-dead-letter-exchange": "peril_dlx",
    },
  });
  await channel.bindQueue(queueName, exchange, key);

  return [channel, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data: Buffer) => JSON.parse(data.toString()));     
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data: Buffer) => decode(data) as T);
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
  );
  await channel.prefetch(10);
  await channel.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (!msg) {
      return;
    }

    try {
      const data: T = deserializer(msg.content);
      const ack = await handler(data);
      switch (ack) {
        case "Ack":
          channel.ack(msg);
          break;
        case "NackRequeue":
          channel.nack(msg, false, true);
          break;
        case "NackDiscard":
          channel.nack(msg, false, false);
          break;
      }
    } catch (err) {
      console.error("Failed to parse message:", err);
    }
  });
}