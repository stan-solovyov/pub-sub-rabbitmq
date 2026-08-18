import type {
  GameState,
  PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import {
  type ArmyMove,
  type RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import type { ConfirmChannel } from "amqplib";
import {
  ExchangePerilTopic,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import {
  handleWar,
  WarOutcome,
  type WarResolution,
} from "../internal/gamelogic/war.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { publishGameLog } from "./index.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");

    return "Ack";
  };
}

export function handlerMove(
  gs: GameState,
  channel: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const result = handleMove(gs, move);
    process.stdout.write("> ");
    if (result === MoveOutcome.Safe) {
      return "Ack";
    }

    if (result === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      try {
        await publishJSON(
          channel,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${gs.getUsername()}`,
          rw,
        );
      } catch (err) {
        return "NackRequeue";
      }

      return "Ack";
    }

    return "NackDiscard";
  };
}

export function handlerWar(
  gs: GameState,
  channel: ConfirmChannel,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    const outcome: WarResolution = handleWar(gs, rw);
    process.stdout.write("> ");
    switch (outcome.result) {
      case WarOutcome.YouWon:
      case WarOutcome.OpponentWon:
        try {
          await publishGameLog(
            channel,
            gs.getUsername(),
            `${outcome.winner} won a war against ${outcome.loser}`,
          );
        } catch (err) {
          return "NackRequeue";
        }
        return "Ack";
      case WarOutcome.Draw:
        try {
          await publishGameLog(
            channel,
            gs.getUsername(),
            `A war between ${rw.attacker.username} and ${rw.defender.username} resulted in a draw`,
          );
        } catch (err) {
          return "NackRequeue";
        }
        return "Ack";
      case WarOutcome.NoUnits:
        return "NackDiscard";
      case WarOutcome.NotInvolved:
        return "NackRequeue";
      default:
        process.stdout.write("Something went wrong with the war resolution.\n");
        return "NackDiscard";
    }
  };
}

export type AckType = "Ack" | "NackRequeue" | "NackDiscard";
