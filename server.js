import "dotenv/config";
import { Bot } from "grammy";
import express from "express";
import { TELEGRAM_BOT_TOKEN, PORT } from "./config.js";
import { LOG } from "./utils.js";
import chalk from "chalk";
import { mainKeyboard } from "./keyboards.js";

const app = express();

// parse the updates to JSON
app.use(express.json());

// Start Express Server
app.listen(PORT, () => {
    LOG(chalk.green(`Express server is listening on ${PORT}`));
});

if (!TELEGRAM_BOT_TOKEN) throw new Error("TELEGRAM_BOT_TOKEN is unset");

// Create an instance of the `Bot` class and pass your bot token to it.
const bot = new Bot(TELEGRAM_BOT_TOKEN); // <-- put your bot token between the ""

// Handle the /start command.
bot.command("start", (ctx) => {
    LOG(chalk.green("BOT INSTANCE STARTED!"));
    return ctx.reply(
        "👩‍⚕️: Welcome to Charlie's HQ Bot. Strong alone, stronger together. Send /help for list of available commands.",
        {
            reply_markup: mainKeyboard
        }
    );
});

bot.on("message:text", async (ctx) => {
    const text = ctx.msg.text;
    if (text === "/checkmc" || text === "Check MC 🗞️") {
        LOG(`ENTERING TEXT SCOPE: ${text}`);
        // change to ur logic, import logic from processor.js
        return ctx.reply(`👩‍⚕️: Selected Check MC 🗞️`, {
            reply_markup: mainKeyboard
        });
    }

    if (text === "/checkconduct" || text === "Check Conduct Tracking 🏃‍♂️") {
        LOG(`ENTERING TEXT SCOPE: ${text}`);
        // change to ur logic, import logic from processor.js
        return ctx.reply(`👩‍⚕️: Selected Check Conduct 🏃‍♂️`, {
            reply_markup: mainKeyboard
        });
    }

    if (text === "/checkall" || text === "Check Everything 👀") {
        LOG(`ENTERING TEXT SCOPE: ${text}`);
        // change to ur logic, import logic from processor.js
        return ctx.reply(`👩‍⚕️: Selected Check All 👀`, {
            reply_markup: mainKeyboard
        });
    }

    if (text === "/help") {
        LOG(`ENTERING TEXT SCOPE: ${text}`);
        // change to ur logic, import logic from processor.js
        return ctx.reply(
            `👩‍⚕️: I am a bot which manage 3GDS Charlie's MC and HA tracking with OCR technology. Built by Charlie Coy HQ.`,
            {
                reply_markup: mainKeyboard
            }
        );
    }
});

// Start the bot.
bot.start();
