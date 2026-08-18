import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
} from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import {
  SimpleQueueType,
  subscribeMsgPack,
} from "../internal/pubsub/consume.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import type { AckType } from "../client/handlers.js";

async function main() {
  printServerHelp();
  const rabbitMQConnection = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitMQConnection);
  await subscribeMsgPack(
    conn,
    ExchangePerilTopic,
    `${GameLogSlug}`,
    `${GameLogSlug}.#`,
    SimpleQueueType.Durable,
    async (data: any) => {
      await writeLog(data);
      process.stdout.write("Log written to file.\n> ");

      return "Ack" as AckType;
    },
  );
// Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }
  const channel = await conn.createConfirmChannel();
  console.log("Connected to RabbitMQ");
  while (true) {
    const input = await getInput("Enter command (pause/resume/quit): ");
    if (input.length === 0) {
      console.log("No command entered. Please try again.");
      continue;
    } else if (input[0] === "pause") {
      console.log("Game paused.");
      const playingState: PlayingState = { isPaused: true };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, playingState);
    } else if (input[0] === "resume") {
      console.log("Game resumed.");
      const playingState: PlayingState = { isPaused: false };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, playingState);
    } else if (input[0] === "quit") {
      console.log("Quitting the game.");
      break;
    } else {
      console.log("Unknown command. Type 'pause', 'resume', or 'quit'.");
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
