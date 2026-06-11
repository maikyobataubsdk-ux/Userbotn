import re
import os
import asyncio
import logging
import random
import functools
from datetime import datetime, UTC
from typing import Union, List, Optional

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, RPCError, UserBlocked, UserDeactivated, SessionPasswordNeeded
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# --- CONSTANTS & CONFIG ---
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Automator")

# --- UTILS ---
def to_small_caps(text: str) -> str:
    mapping = {
        'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ꜰ', 'g': 'ɢ', 'h': 'ʜ',
        'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ', 'o': 'ᴏ', 'p': 'ᴘ',
        'q': 'ǫ', 'r': 'ʀ', 's': 'ꜱ', 't': 'ᴛ', 'u': 'ᴜ', 'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x',
        'y': 'ʏ', 'z': 'ᴢ',
        'A': 'ᴀ', 'B': 'ʙ', 'C': 'ᴄ', 'D': 'ᴅ', 'E': 'ᴇ', 'F': 'ꜰ', 'G': 'ɢ', 'H': 'ʜ',
        'I': 'ɪ', 'J': 'ᴊ', 'K': 'ᴋ', 'L': 'ʟ', 'M': 'ᴍ', 'N': 'ɴ', 'O': 'ᴏ', 'P': 'ᴘ',
        'Q': 'ǫ', 'R': 'ʀ', 'S': 'ꜱ', 'T': 'ᴛ', 'U': 'ᴜ', 'V': 'ᴠ', 'W': 'ᴡ', 'X': 'x',
        'Y': 'ʏ', 'Z': 'ᴢ'
    }
    return "".join(mapping.get(c, c) for c in text)

def remove_emojis(text: str) -> str:
    if not text: return ""
    return re.sub(r'[^\x00-\x7F\u00A0-\u02AF\u0370-\u1CFF\u1E00-\u20CF\u2100-\u218F\u2C00-\u2DFF\u2E00-\u2E7F\uA720-\uA7FF\uAB30-\uAB6F]+', '', text)

def best_format(text: str) -> str:
    if not text: return ""
    # Regex to find URLs so we don't mess them up with small caps
    url_pattern = r'(https?://\S+)'
    parts = re.split(url_pattern, text)
    formatted_parts = []
    for part in parts:
        if re.match(url_pattern, part):
            formatted_parts.append(part)
        else:
            formatted_parts.append(to_small_caps(remove_emojis(part)))
    return "".join(formatted_parts)

def admin_only(func):
    @functools.wraps(func)
    async def wrapper(client, message: Message):
        if message.from_user and message.from_user.id in ADMIN_IDS:
            return await func(client, message)
        # No response for non-admins as per stealth/security
    return wrapper

def cb_admin(func):
    @functools.wraps(func)
    async def wrapper(client, callback_query):
        if callback_query.from_user and callback_query.from_user.id in ADMIN_IDS:
            return await func(client, callback_query)
        await callback_query.answer(best_format("Unauthorized"), show_alert=True)
    return wrapper

# --- DATABASE ---
class DB:
    def __init__(self, url):
        self.client = AsyncIOMotorClient(url, serverSelectionTimeoutMS=5000)
        self.db = self.client.automator
        self.cfg = self.db.cfg
        self.acc = self.db.accounts
        self.tmpl = self.db.templates
        self.stats = self.db.stats
        self.bl = self.db.blacklist
        self.logs = self.db.logs
        self.amsg = self.db.account_messages

    async def ping(self):
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"DB Ping failed: {e}")
            return False

    async def get_cfg(self, key, default=None):
        doc = await self.cfg.find_one({"_id": key})
        return doc["value"] if doc else default

    async def set_cfg(self, key, value):
        await self.cfg.update_one({"_id": key}, {"$set": {"value": value}}, upsert=True)

    async def get_accounts(self, active_only=True):
        query = {"banned": False}
        if active_only:
            query["active"] = True
        return await self.acc.find(query).to_list(length=None)

    async def add_account(self, data):
        data["username"] = data["username"].lower().replace("@", "")
        data["added"] = datetime.now(UTC)
        return await self.acc.insert_one(data)

    async def update_acc_stats(self, username, sent=0, failed=0, floods=0):
        await self.acc.update_one(
            {"username": username.lower()},
            {"$inc": {"stats.sent": sent, "stats.failed": failed, "stats.floods": floods},
             "$set": {"stats.last_active": datetime.now(UTC)}}
        )

    async def get_templates(self):
        return await self.tmpl.find({"active": True}).to_list(length=None)

    async def get_amsg(self, username):
        return await self.amsg.find_one({"username": username.lower()})

    async def set_amsg(self, username, data):
        data["username"] = username.lower().replace("@", "")
        data["updated"] = datetime.now(UTC)
        await self.amsg.update_one({"username": data["username"]}, {"$set": data}, upsert=True)

    async def del_amsg(self, username):
        await self.amsg.delete_one({"username": username.lower().replace("@", "")})

    async def get_targets(self):
        return await self.get_cfg("targets", [])

    async def is_blacklisted(self, target):
        doc = await self.bl.find_one({"_id": str(target)})
        return doc is not None

    async def add_to_blacklist(self, target):
        await self.bl.update_one({"_id": str(target)}, {"$set": {"added": datetime.now(UTC)}}, upsert=True)

    async def remove_from_blacklist(self, target):
        await self.bl.delete_one({"_id": str(target)})

    async def log_event(self, event, details):
        await self.logs.insert_one({
            "event": event,
            "details": details,
            "timestamp": datetime.now(UTC)
        })


# --- ACCOUNT MANAGER ---
class AccountManager:
    def __init__(self, db: DB):
        self.db = db
        self.clients = {} # username -> Client

    async def make_client(self, acc_data):
        username = acc_data["username"]
        if username in self.clients:
            return self.clients[username]

        proxy = acc_data.get("proxy")
        client = Client(
            name=f"session_{username}",
            api_id=acc_data["api_id"],
            api_hash=acc_data["api_hash"],
            session_string=acc_data["session"],
            in_memory=True,
            proxy=proxy,
            no_updates=True
        )
        self.clients[username] = client
        return client

    async def get_active_clients(self):
        accs = await self.db.get_accounts(active_only=True)
        active_clients = []
        for acc in accs:
            try:
                client = await self.make_client(acc)
                if not client.is_connected:
                    await client.start()
                active_clients.append(client)
            except (UserDeactivated, UserBlocked):
                await self.db.acc.update_one({"username": acc["username"]}, {"$set": {"banned": True, "active": False}})
                logger.warning(f"Account {acc['username']} is banned/deactivated.")
            except Exception as e:
                logger.error(f"Failed to start client {acc['username']}: {e}")
        return active_clients

    async def stop_all(self):
        for client in self.clients.values():
            if client.is_connected:
                await client.stop()


# --- BROADCAST ENGINE ---
class Engine:
    def __init__(self, db: DB, am: AccountManager):
        self.db = db
        self.am = am
        self.is_running = False

    async def smart_send(self, client: Client, chat_id, msg_data):
        try:
            m_type = msg_data.get("type", "text")
            res = None
            if m_type == "text":
                content = best_format(msg_data["content"])
                res = await client.send_message(chat_id, content, disable_web_page_preview=msg_data.get("no_preview", False))
            elif m_type == "photo":
                caption = best_format(msg_data.get("caption")) if msg_data.get("caption") else None
                res = await client.send_photo(chat_id, msg_data["file_id"], caption=caption)
            elif m_type == "video":
                caption = best_format(msg_data.get("caption")) if msg_data.get("caption") else None
                res = await client.send_video(chat_id, msg_data["file_id"], caption=caption)
            elif m_type == "document":
                caption = best_format(msg_data.get("caption")) if msg_data.get("caption") else None
                res = await client.send_document(chat_id, msg_data["file_id"], caption=caption)
            elif m_type == "poll":
                question = best_format(msg_data["question"])
                options = [best_format(o) for o in msg_data["options"]]
                res = await client.send_poll(chat_id, question, options, is_anonymous=msg_data.get("anonymous", True))

            return {"status": "success", "floods": 0}
        except FloodWait as e:
            logger.warning(f"FloodWait on {client.me.username}: {e.value}s")
            return {"status": "flood", "floods": 1, "wait": e.value}
        except (UserBlocked, UserDeactivated):
            return {"status": "banned", "floods": 0}
        except Exception as e:
            logger.error(f"Send failed: {e}")
            return {"status": "error", "floods": 0}

    async def pick_template(self):
        tmpls = await self.db.get_templates()
        if not tmpls: return None
        mode = await self.db.get_cfg("tmpl_mode", "random")
        if mode == "random":
            return random.choice(tmpls)
        else:
            idx = await self.db.get_cfg("tmpl_idx", 0)
            tmpl = tmpls[idx % len(tmpls)]
            await self.db.set_cfg("tmpl_idx", idx + 1)
            return tmpl

    async def broadcast(self):
        if self.is_running: return
        self.is_running = True
        try:
            targets = await self.db.get_targets()
            if not targets:
                logger.info("No targets set.")
                return

            rotation = await self.db.get_cfg("rotation", "sequential")
            clients = await self.am.get_active_clients()
            if not clients:
                logger.info("No active clients.")
                return

            if rotation == "sequential":
                await self._from_account(clients, targets)
            else: # random or round_robin
                await self._one_shot(clients, targets, rotation)

        finally:
            self.is_running = False

    async def _from_account(self, clients, targets):
        for client in clients:
            me = await client.get_me()
            custom_msg = await self.db.get_amsg(me.username)

            for target in targets:
                if await self.db.is_blacklisted(target): continue

                msg_data = custom_msg if custom_msg else await self.pick_template()
                if not msg_data: break

                res = await self.smart_send(client, target, msg_data)
                if res["status"] == "success":
                    await self.db.update_acc_stats(me.username, sent=1)
                elif res["status"] == "flood":
                    await self.db.update_acc_stats(me.username, floods=1)
                    await asyncio.sleep(res["wait"] + random.randint(5, 15))
                    # Optionally skip or retry
                elif res["status"] == "banned":
                    break # move to next account

                delay = await self.db.get_cfg("idgaap_delay")
                if delay is None:
                    delay = random.randint(await self.db.get_cfg("min_delay", 3), await self.db.get_cfg("max_delay", 8))
                await asyncio.sleep(delay)

    async def _one_shot(self, clients, targets, mode):
        idx = 0
        for target in targets:
            if await self.db.is_blacklisted(target): continue

            if mode == "round_robin":
                client = clients[idx % len(clients)]
                idx += 1
            else:
                client = random.choice(clients)

            me = await client.get_me()
            custom_msg = await self.db.get_amsg(me.username)
            msg_data = custom_msg if custom_msg else await self.pick_template()
            if not msg_data: continue

            res = await self.smart_send(client, target, msg_data)
            if res["status"] == "success":
                await self.db.update_acc_stats(me.username, sent=1)
            elif res["status"] == "flood":
                await self.db.update_acc_stats(me.username, floods=1)

            delay = await self.db.get_cfg("idgaap_delay")
            if delay is None:
                delay = random.randint(await self.db.get_cfg("min_delay", 3), await self.db.get_cfg("max_delay", 8))
            await asyncio.sleep(delay)


# --- BOT SETUP ---
bot = Client("automator_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
db = DB(MONGO_URL)
am = AccountManager(db)
engine = Engine(db, am)
scheduler = AsyncIOScheduler()

# --- COMMANDS: ACCOUNT MGMT ---
@bot.on_message(filters.command("add_account") & filters.private)
@admin_only
async def cmd_add_account(client, message):
    try:
        parts = message.text.split(None, 1)[1].split("|")
        api_id = int(parts[0])
        api_hash = parts[1]
        session = parts[2]
        proxy = None
        if len(parts) > 3 and parts[3]:
            # Basic proxy parse: scheme://user:pass@host:port
            p_str = parts[3]
            # Assuming simple dict for now as per schema
            proxy = {"hostname": p_str} # Placeholder
        label = parts[4] if len(parts) > 4 else "unlabeled"

        temp_client = Client(f"temp_{api_id}", api_id=api_id, api_hash=api_hash, session_string=session, in_memory=True)
        await temp_client.start()
        me = await temp_client.get_me()
        await temp_client.stop()

        acc_data = {
            "api_id": api_id,
            "api_hash": api_hash,
            "session": session,
            "proxy": proxy,
            "label": label,
            "username": me.username.lower() if me.username else str(me.id),
            "phone": me.phone_number,
            "active": True,
            "banned": False,
            "stats": {"sent": 0, "failed": 0, "floods": 0, "last_active": None}
        }
        await db.add_account(acc_data)
        await message.reply_text(best_format(f"Account @{me.username} added successfully"))
    except Exception as e:
        await message.reply_text(best_format(f"Error: {e}"))

@bot.on_message(filters.command("list_accounts") & filters.private)
@admin_only
async def cmd_list_accounts(client, message):
    accs = await db.get_accounts(active_only=False)
    if not accs:
        return await message.reply_text(best_format("No accounts found"))

    res = "<b>ᴀᴄᴄᴏᴜɴᴛ ʟɪꜱᴛ:</b>\n\n"
    for a in accs:
        status = "✅" if a["active"] else "❌"
        if a["banned"]: status = "🚫"
        res += best_format(f"{status} @{a['username']} | ꜱ: {a['stats']['sent']} | ꜰ: {a['stats']['floods']}\n")

    await message.reply_text(res, parse_mode=enums.ParseMode.HTML)

# --- COMMANDS: CUSTOM MESSAGES ---
@bot.on_message(filters.command("setmassage") & filters.private)
@admin_only
async def cmd_set_massage(client, message):
    args = message.text.split()
    if len(args) < 2:
        return await message.reply_text(best_format("Usage: /setmassage @username [text/poll] or reply to media"))

    sub = args[1].lower()

    if sub == "list":
        amsgs = await db.amsg.find().to_list(None)
        if not amsgs: return await message.reply_text(best_format("No custom messages"))
        res = "<b>ᴄᴜꜱᴛᴏᴍ ᴍᴇꜱꜱᴀɢᴇꜱ:</b>\n"
        for m in amsgs:
            res += best_format(f"@{m['username']} -> {m['type']}\n")
        return await message.reply_text(res, parse_mode=enums.ParseMode.HTML)

    if sub == "del" and len(args) > 2:
        username = args[2].replace("@", "")
        await db.del_amsg(username)
        return await message.reply_text(best_format(f"Removed message for @{username}"))

    username = sub.replace("@", "")

    if message.reply_to_message:
        r = message.reply_to_message
        data = {"username": username}
        if r.photo:
            data.update({"type": "photo", "file_id": r.photo.file_id, "caption": r.caption})
        elif r.video:
            data.update({"type": "video", "file_id": r.video.file_id, "caption": r.caption})
        elif r.document:
            data.update({"type": "document", "file_id": r.document.file_id, "caption": r.caption})
        elif r.poll:
            data.update({"type": "poll", "question": r.poll.question, "options": [o.text for o in r.poll.options]})
        else:
            data.update({"type": "text", "content": r.text})

        await db.set_amsg(username, data)
        await message.reply_text(best_format(f"Message set for @{username}"))

    elif len(args) > 2:
        content = " ".join(args[2:])
        if content.startswith("poll|"):
            parts = content.split("|")
            data = {"type": "poll", "question": parts[1], "options": parts[2:]}
        else:
            data = {"type": "text", "content": content}

        await db.set_amsg(username, data)
        await message.reply_text(best_format(f"Message set for @{username}"))


# --- COMMANDS: TEMPLATES ---
@bot.on_message(filters.command("add_template") & filters.private)
@admin_only
async def cmd_add_template(client, message):
    data = {"active": True, "created": datetime.now(UTC), "used": 0}
    if message.reply_to_message:
        r = message.reply_to_message
        if r.photo: data.update({"type": "photo", "file_id": r.photo.file_id, "caption": r.caption})
        elif r.video: data.update({"type": "video", "file_id": r.video.file_id, "caption": r.caption})
        elif r.document: data.update({"type": "document", "file_id": r.document.file_id, "caption": r.caption})
        else: data.update({"type": "text", "content": r.text})
    else:
        parts = message.text.split(None, 1)
        if len(parts) < 2: return await message.reply_text(best_format("Provide text or reply to media"))
        content = parts[1]
        if content.startswith("poll|"):
            p = content.split("|")
            data.update({"type": "poll", "question": p[1], "options": p[2:]})
        else:
            data.update({"type": "text", "content": content})

    await db.tmpl.insert_one(data)
    await message.reply_text(best_format("Template added"))

@bot.on_message(filters.command("list_templates") & filters.private)
@admin_only
async def cmd_list_templates(client, message):
    tmpls = await db.tmpl.find().to_list(None)
    if not tmpls: return await message.reply_text(best_format("No templates"))
    res = "<b>ᴛᴇᴍᴘʟᴀᴛᴇꜱ:</b>\n"
    for i, t in enumerate(tmpls):
        res += best_format(f"{i+1}. {t['type']} | ᴜꜱᴇᴅ: {t['used']}\n")
    await message.reply_text(res, parse_mode=enums.ParseMode.HTML)

@bot.on_message(filters.command("del_template") & filters.private)
@admin_only
async def cmd_del_template(client, message):
    parts = message.text.split()
    if len(parts) < 2: return await message.reply_text(best_format("Usage: /del_template <num>"))
    idx = int(parts[1]) - 1
    tmpls = await db.tmpl.find().to_list(None)
    if 0 <= idx < len(tmpls):
        await db.tmpl.delete_one({"_id": tmpls[idx]["_id"]})
        await message.reply_text(best_format("Template deleted"))

# --- COMMANDS: TARGETS ---
@bot.on_message(filters.command("set_targets") & filters.private)
@admin_only
async def cmd_set_targets(client, message):
    parts = message.text.split(None, 1)
    targets = [t.strip() for t in parts[1].split(",")] if len(parts) > 1 else []
    await db.set_cfg("targets", targets)
    await message.reply_text(best_format(f"Targets set: {len(targets)}"))

@bot.on_message(filters.command("add_target") & filters.private)
@admin_only
async def cmd_add_target(client, message):
    parts = message.text.split(None, 1)
    new_targets = [t.strip() for t in parts[1].split(",")] if len(parts) > 1 else []
    curr = await db.get_targets()
    await db.set_cfg("targets", list(set(curr + new_targets)))
    await message.reply_text(best_format(f"Targets added: {len(new_targets)}"))

# --- COMMANDS: DELAYS & SETTINGS ---
@bot.on_message(filters.command("idgaap") & filters.private)
@admin_only
async def cmd_idgaap(client, message):
    parts = message.text.split()
    if len(parts) == 1:
        val = await db.get_cfg("idgaap_delay")
        return await message.reply_text(best_format(f"Current idgaap: {val}"))
    if parts[1].lower() == "off":
        await db.set_cfg("idgaap_delay", None)
        return await message.reply_text(best_format("idgaap turned off"))
    await db.set_cfg("idgaap_delay", int(parts[1]))
    await message.reply_text(best_format(f"idgaap set to {parts[1]}s"))

@bot.on_message(filters.command("settings") & filters.private)
@admin_only
async def cmd_settings(client, message):
    parts = message.text.split()
    if len(parts) == 1:
        m = await db.get_cfg("min_delay", 3)
        x = await db.get_cfg("max_delay", 8)
        rot = await db.get_cfg("rotation", "sequential")
        tm = await db.get_cfg("tmpl_mode", "random")
        res = best_format(f"ᴅᴇʟᴀʏ: {m}-{x} | ʀᴏᴛᴀᴛɪᴏɴ: {rot} | ᴛᴍᴘʟ_ᴍᴏᴅᴇ: {tm}")
        return await message.reply_text(res)

    key = parts[1].lower()
    if key == "delay" and len(parts) > 3:
        await db.set_cfg("min_delay", int(parts[2]))
        await db.set_cfg("max_delay", int(parts[3]))
        await message.reply_text(best_format("Delay updated"))
    elif key == "rotation" and len(parts) > 2:
        await db.set_cfg("rotation", parts[2].lower())
        await message.reply_text(best_format("Rotation updated"))
    elif key == "tmpl_mode" and len(parts) > 2:
        await db.set_cfg("tmpl_mode", parts[2].lower())
        await message.reply_text(best_format("Template mode updated"))


# --- COMMANDS: SCHEDULER & OTHER ---
@bot.on_message(filters.command("start_job") & filters.private)
@admin_only
async def cmd_start_job(client, message):
    parts = message.text.split(None, 2)
    if len(parts) < 3: return await message.reply_text(best_format("Usage: /start_job [interval/cron] [val]"))

    jtype = parts[1].lower()
    val = parts[2]

    if scheduler.get_job("broadcast_job"):
        scheduler.remove_job("broadcast_job")

    if jtype == "interval":
        scheduler.add_job(engine.broadcast, IntervalTrigger(minutes=int(val)), id="broadcast_job")
    elif jtype == "cron":
        scheduler.add_job(engine.broadcast, CronTrigger.from_crontab(val), id="broadcast_job")

    await message.reply_text(best_format(f"Job started: {jtype} {val}"))

@bot.on_message(filters.command("stop_job") & filters.private)
@admin_only
async def cmd_stop_job(client, message):
    if scheduler.get_job("broadcast_job"):
        scheduler.remove_job("broadcast_job")
        await message.reply_text(best_format("Job stopped"))
    else:
        await message.reply_text(best_format("No job running"))

@bot.on_message(filters.command("run_now") & filters.private)
@admin_only
async def cmd_run_now(client, message):
    await message.reply_text(best_format("Triggering broadcast..."))
    asyncio.create_task(engine.broadcast())

@bot.on_message(filters.command("help") & filters.private)
@admin_only
async def cmd_help(client, message):
    h = (
        "<b>ᴀᴠᴀɪʟᴀʙʟᴇ ᴄᴏᴍᴍᴀɴᴅꜱ:</b>\n\n"
        "/add_account - ᴀᴅᴅ ᴜꜱᴇʀʙᴏᴛ\n"
        "/list_accounts - ꜱʜᴏᴡ ᴀᴄᴄᴏᴜɴᴛꜱ\n"
        "/setmassage - ᴄᴜꜱᴛᴏᴍ ᴍꜱɢ\n"
        "/add_template - ᴀᴅᴅ ᴛᴇᴍᴘʟᴀᴛᴇ\n"
        "/set_targets - ꜱᴇᴛ ᴛᴀʀɢᴇᴛꜱ\n"
        "/idgaap - ꜰɪxᴇᴅ ᴅᴇʟᴀʏ\n"
        "/settings - ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ\n"
        "/start_job - ꜱᴄʜᴇᴅᴜʟᴇ\n"
        "/run_now - ʀᴜɴ ɪᴍᴍᴇᴅɪᴀᴛᴇʟʏ\n"
        "/stats - ꜱʏꜱᴛᴇᴍ ꜱᴛᴀᴛꜱ"
    )
    await message.reply_text(best_format(h), parse_mode=enums.ParseMode.HTML)

@bot.on_message(filters.command("start") & filters.private)
@admin_only
async def cmd_start(client, message):
    await message.reply_text(best_format("Welcome to Advanced Multi-Account Telegram Automator v3.1"))

# --- MAIN ---
async def main():
    if not await db.ping():
        logger.error("Could not connect to MongoDB. Exiting.")
        return

    logger.info("Starting bot...")
    await bot.start()
    scheduler.start()
    logger.info("Bot started and scheduler running.")

    # Keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
