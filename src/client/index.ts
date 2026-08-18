import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import {
  declareAndBind,
  SimpleQueueType,
  subscribeJSON
} from "../internal/pubsub/consume.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  const rabbitMQConnection = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitMQConnection);
  console.log("Connected to RabbitMQ");
  const username = await clientWelcome();
  await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
  );
  const gameState = new GameState(username);
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gameState),
  );
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gameState, await conn.createConfirmChannel()),
  );
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gameState, await conn.createConfirmChannel()),
  );
  while (true) {
    const input = await getInput(
      "Enter command (spawn/move/status/help/spam/quit): ",
    );
    if (input.length === 0) {
      console.log("No command entered. Please try again.");
      continue;
    } else if (input[0] === "spawn") {
      try {
        commandSpawn(gameState, input);
      } catch (err) {
        console.error("Error spawning unit:", err);
      }
    } else if (input[0] === "move") {
      try {
        const result = commandMove(gameState, input);
        await publishJSON(
          await conn.createConfirmChannel(),
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          result,
        );
        console.log("Move command published successfully.");
      } catch (err) {
        console.error("Error moving unit:", err);
      }
    } else if (input[0] === "status") {
      try {
        commandStatus(gameState);
      } catch (err) {
        console.error("Error getting status:", err);
      }
    } else if (input[0] === "help") {
      printClientHelp();
    } else if (input[0] === "spam") {
      const second_parameter = input[1] ? parseInt(input[1], 10) : 10;
      const channel = await conn.createConfirmChannel();
      for (let i = 0; i < second_parameter; i++) {
        const message = getMaliciousLog();
        const log: GameLog = {
          username,
          message,
          currentTime: new Date(),
        };
        await publishMsgPack(
          channel,
          ExchangePerilTopic,
          `${GameLogSlug}.${username}`,
          log,
        );
      }
    } else if (input[0] === "quit") {
      printQuit();
      break;
    } else {
      console.log("Unknown command");
    }
  }

  process.on("SIGINT", async () => {
    console.log("Closing RabbitMQ connection...");
    await conn.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});


export function publishGameLog(channel: amqp.ConfirmChannel, username: string, message: string): Promise<void> {
  const gameLog: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };

  return publishMsgPack(channel, ExchangePerilTopic, `${GameLogSlug}.${username}`, gameLog);
}