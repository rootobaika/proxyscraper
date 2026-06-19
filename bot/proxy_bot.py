import asyncio
import logging
import sys

import aiohttp
import discord
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import DISCORD_TOKEN, TELEGRAM_TOKEN, SERVER_URL, PROXIES_PER_PAGE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("proxy_bot")

session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    return session


async def fetch_proxies(proto: str = None) -> list[str]:
    s = await get_session()
    url = f"{SERVER_URL}/proxies"
    if proto:
        url += f"/{proto}"
    try:
        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
            return [line.strip() for line in text.splitlines() if line.strip()]
    except Exception as e:
        log.warning(f"fetch_proxies error: {e}")
        return []


async def fetch_stats() -> str | None:
    s = await get_session()
    try:
        async with s.get(f"{SERVER_URL}/lastupdate", timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            folder = data.get("folder", "?")
            stats = data.get("stats", {})
            parts = [f"📁 {folder}"]
            for k, v in stats.items():
                parts.append(f"{k}: {v}")
            return " | ".join(parts)
    except Exception as e:
        log.warning(f"fetch_stats error: {e}")
        return None


# ─── helpers ───────────────────────────────────────────────────

def format_proxies(proxies: list[str], start: int = 0) -> str:
    lines = []
    for i, p in enumerate(proxies, start=start + 1):
        lines.append(f"{i:>3}. {p}")
    return "\n".join(lines)


def build_keyboard(page: int, total_pages: int, proto: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    if page > 0:
        row.append(InlineKeyboardButton("⬅️", callback_data=f"page|{page - 1}|{proto}"))
    row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton("➡️", callback_data=f"page|{page + 1}|{proto}"))
    buttons.append(row)
    buttons.append([
        InlineKeyboardButton("HTTP", callback_data="proto|http"),
        InlineKeyboardButton("SOCKS4", callback_data="proto|socks4"),
        InlineKeyboardButton("SOCKS5", callback_data="proto|socks5"),
        InlineKeyboardButton("All", callback_data="proto|all"),
    ])
    return InlineKeyboardMarkup(buttons)


# ─── Telegram handlers ─────────────────────────────────────────

async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Proxy Bot*\n\n"
        "`/proxies` — получить свежие прокси\n"
        "`/proxies http` — только HTTP\n"
        "`/proxies socks4` — только SOCKS4\n"
        "`/proxies socks5` — только SOCKS5\n"
        "`/stats` — статистика",
        parse_mode="Markdown",
    )


async def tg_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = await fetch_stats()
    if not s:
        await update.message.reply_text("❌ Сервер недоступен.")
        return
    await update.message.reply_text(f"📊 *Proxy Store*\n{s}", parse_mode="Markdown")


async def tg_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    proto = None
    if context.args:
        arg = context.args[0].lower()
        if arg in ("http", "socks4", "socks5"):
            proto = arg
    await tg_send_proxies(update, context, proto)


async def tg_send_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE, proto=None, page=0, edit=None):
    proxies = await fetch_proxies(proto)
    if not proxies:
        text = "❌ Нет рабочих прокси."
        if edit:
            await edit.edit_text(text)
        else:
            await update.message.reply_text(text)
        return

    total_pages = max(1, (len(proxies) + PROXIES_PER_PAGE - 1) // PROXIES_PER_PAGE)
    page = min(page, total_pages - 1)
    start = page * PROXIES_PER_PAGE
    chunk = proxies[start:start + PROXIES_PER_PAGE]

    label = proto.upper() if proto else "ALL"
    text = f"📡 *{label}* — стр. {page + 1}/{total_pages}\n```\n"
    text += format_proxies(chunk, start)
    text += "\n```"

    kb = build_keyboard(page, total_pages, proto or "all")

    if edit:
        await edit.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def tg_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("|")

    if data[0] == "page":
        page = int(data[1])
        proto = data[2] if data[2] != "all" else None
        await tg_send_proxies(update, context, proto=proto, page=page, edit=query.message)

    elif data[0] == "proto":
        proto = data[1] if data[1] != "all" else None
        await tg_send_proxies(update, context, proto=proto, page=0, edit=query.message)


def run_telegram():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", tg_start))
    app.add_handler(CommandHandler("proxies", tg_proxies))
    app.add_handler(CommandHandler("stats", tg_stats))
    app.add_handler(CallbackQueryHandler(tg_callback))
    log.info("Telegram bot started")
    app.run_polling()


# ─── Discord handlers ──────────────────────────────────────────

class ProxyBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)

    async def on_ready(self):
        log.info(f"Discord bot logged in as {self.user}")

    async def on_message(self, message):
        if message.author.bot:
            return
        if message.content.startswith("!proxies") or message.content.startswith("!proxy"):
            parts = message.content.split()
            proto = parts[1].lower() if len(parts) > 1 else None
            if proto not in ("http", "socks4", "socks5"):
                proto = None
            await self._send_proxies(message.channel, proto)

    async def _send_proxies(self, channel, proto=None):
        proxies = await fetch_proxies(proto)
        if not proxies:
            await channel.send("❌ Нет рабочих прокси.")
            return

        label = proto.upper() if proto else "ALL"
        text = f"**{label}** — свежие прокси:\n```\n"
        text += format_proxies(proxies[:PROXIES_PER_PAGE])
        text += "\n```"
        await channel.send(text)


def run_discord():
    client = ProxyBot()
    client.run(DISCORD_TOKEN)


# ─── main ──────────────────────────────────────────────────────

async def main():
    if not DISCORD_TOKEN and not TELEGRAM_TOKEN:
        print("❌ Укажите DISCORD_TOKEN и/или TELEGRAM_TOKEN в .env")
        sys.exit(1)

    tasks = []
    if TELEGRAM_TOKEN:
        tasks.append(asyncio.to_thread(run_telegram))
    if DISCORD_TOKEN:
        tasks.append(asyncio.to_thread(run_discord))

    try:
        await asyncio.gather(*tasks)
    finally:
        if session and not session.closed:
            await session.close()


if __name__ == "__main__":
    asyncio.run(main())
