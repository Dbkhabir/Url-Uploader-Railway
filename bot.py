import os
import sys
import time
import math
import logging
import asyncio
import sqlite3
import subprocess
from datetime import datetime

# External dependencies
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified
import aria2p

# ==========================================
# 1. CONFIGURATION & LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment Variables
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not API_ID or not API_HASH or not BOT_TOKEN:
    logger.error("Missing API_ID, API_HASH, or BOT_TOKEN. Please set them in environment variables.")
    sys.exit(1)

# ==========================================
# 2. STATE, QUEUE & RATE LIMITING
# ==========================================
download_queue = asyncio.Queue()
cancel_flags = {}      # Format: { task_id: bool }
user_cooldowns = {}    # Format: { user_id: timestamp }
COOLDOWN_SECONDS = 10

# Initialize Pyrogram Client
app = Client(
    "downloader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# ==========================================
# 3. DATABASE INITIALIZATION
# ==========================================
DB_FILE = "users.db"

def init_db():
    """Initializes SQLite database for user tracking."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            join_date TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_user_to_db(user_id):
    """Adds a new user to the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if not cursor.fetchone():
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO users (user_id, join_date) VALUES (?, ?)", (user_id, now))
        conn.commit()
    conn.close()

def get_all_users():
    """Fetches all users for broadcasting."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    conn.close()
    return [u[0] for u in users]

# ==========================================
# 4. UTILITY FUNCTIONS (Formatting & UI)
# ==========================================
def format_bytes(size: int) -> str:
    """Formats bytes into human-readable string."""
    if not size:
        return "0 B"
    power = 2 ** 10
    n = 0
    dic_powerN = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size > power:
        size /= power
        n += 1
    return f"{round(size, 2)} {dic_powerN[n]}"

def format_time(seconds: int) -> str:
    """Formats seconds into HH:MM:SS format."""
    return time.strftime('%H:%M:%S', time.gmtime(seconds))

def create_progress_bar(percentage: float) -> str:
    """Generates a visual progress bar string."""
    filled = int(percentage / 10)
    return "█" * filled + "░" * (10 - filled)

def get_cancel_button(task_id: str) -> InlineKeyboardMarkup:
    """Returns an inline keyboard with a Cancel button mapping to the task_id."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Cancel", callback_data=f"cancel_{task_id}")]
    ])

# ==========================================
# 5. WORKER LOOP (Download & Upload Logic)
# ==========================================
async def worker():
    """Background task that dequeues links and processes them."""
    # Initialize Aria2c Client locally within the worker scope
    aria2 = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
    
    while True:
        task = await download_queue.get()
        user_id, url, progress_msg, task_id = task
        filepath = None
        
        try:
            # --- PHASE 1: DOWNLOADING ---
            try:
                download = aria2.add_uris([url])
            except Exception as e:
                await progress_msg.edit_text(f"❌ **Aria2 Error:** `{str(e)}`")
                download_queue.task_done()
                continue

            last_update_time = time.time()
            download_complete = False

            while not download_complete:
                if cancel_flags.get(task_id):
                    aria2.remove([download], force=True, files=True, clean=True)
                    await progress_msg.edit_text("🚫 **Download cancelled by user.**")
                    raise Exception("TASK_CANCELLED")

                download.update()
                
                # Periodically update the progress message (every 3 seconds to avoid FloodWaits)
                if time.time() - last_update_time > 3:
                    if download.total_length > 0:
                        percentage = (download.completed_length / download.total_length) * 100
                    else:
                        percentage = 0
                    
                    bar = create_progress_bar(percentage)
                    speed = format_bytes(download.download_speed)
                    eta = format_time(download.eta.seconds) if download.eta else "Unknown"
                    downloaded = format_bytes(download.completed_length)
                    total = format_bytes(download.total_length)
                    
                    text = (
                        f"**⬇️ Downloading...**\n\n"
                        f"[{bar}] {percentage:.1f}%\n"
                        f"**Processed:** `{downloaded} / {total}`\n"
                        f"**Speed:** `{speed}/s`\n"
                        f"**ETA:** `{eta}`"
                    )
                    
                    try:
                        await progress_msg.edit_text(text, reply_markup=get_cancel_button(task_id))
                    except MessageNotModified:
                        pass
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                        
                    last_update_time = time.time()

                if download.is_complete:
                    download_complete = True
                    filepath = download.files[0].path
                elif download.has_failed:
                    await progress_msg.edit_text(f"❌ **Download Failed:** `{download.error_message}`")
                    raise Exception("DOWNLOAD_FAILED")
                
                await asyncio.sleep(1)

            # --- PHASE 2: UPLOADING ---
            await progress_msg.edit_text("⏳ **Preparing to upload to Telegram...**", reply_markup=get_cancel_button(task_id))
            last_update_time = time.time()
            start_time = time.time()

            async def upload_progress(current, total):
                """Pyrogram progress callback for dynamic upload updates."""
                nonlocal last_update_time
                if cancel_flags.get(task_id):
                    raise Exception("UPLOAD_CANCELLED") # Aborts Pyrogram native upload safely

                if time.time() - last_update_time > 3:
                    percentage = (current / total) * 100
                    bar = create_progress_bar(percentage)
                    elapsed = time.time() - start_time
                    speed_bps = current / elapsed if elapsed > 0 else 0
                    eta_seconds = int((total - current) / speed_bps) if speed_bps > 0 else 0
                    
                    text = (
                        f"**⬆️ Uploading...**\n\n"
                        f"[{bar}] {percentage:.1f}%\n"
                        f"**Processed:** `{format_bytes(current)} / {format_bytes(total)}`\n"
                        f"**Speed:** `{format_bytes(speed_bps)}/s`\n"
                        f"**ETA:** `{format_time(eta_seconds)}`"
                    )
                    
                    try:
                        await progress_msg.edit_text(text, reply_markup=get_cancel_button(task_id))
                    except MessageNotModified:
                        pass
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    
                    last_update_time = time.time()

            await app.send_document(
                chat_id=user_id,
                document=filepath,
                caption="✅ **Successfully Downloaded & Uploaded!**\n\n_Generated by your Bot_",
                progress=upload_progress
            )
            
            await progress_msg.delete()

        except Exception as e:
            if "TASK_CANCELLED" in str(e) or "UPLOAD_CANCELLED" in str(e):
                logger.info(f"Task {task_id} gracefully cancelled.")
                try:
                    await progress_msg.edit_text("🚫 **Task was cancelled.**")
                except:
                    pass
            elif "DOWNLOAD_FAILED" not in str(e):
                logger.error(f"Worker Error: {e}", exc_info=True)
                try:
                    await progress_msg.edit_text(f"❌ **An error occurred:** `{str(e)}`")
                except:
                    pass
        finally:
            # ALWAYS clean up the temporary file
            if filepath and os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except Exception as clean_up_err:
                    logger.error(f"Failed to delete file {filepath}: {clean_up_err}")
            
            # Remove flag mapping to save memory
            if task_id in cancel_flags:
                del cancel_flags[task_id]
                
            download_queue.task_done()


# ==========================================
# 6. BOT HANDLERS & ROUTES
# ==========================================
@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    add_user_to_db(message.from_user.id)
    await message.reply_text(
        f"👋 Welcome {message.from_user.first_name}!\n\n"
        "I am a lightning-fast Media Downloader bot. Just send me any direct **HTTP/HTTPS link** and I will download the file and upload it back to you here.\n\n"
        "⚠️ _Max upload size supported via Telegram is 2GB._"
    )

@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(client, message):
    total_users = len(get_all_users())
    q_size = download_queue.qsize()
    await message.reply_text(
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"⏳ **Active Queue Size:** `{q_size}`\n"
    )

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID) & filters.reply)
async def broadcast_cmd(client, message):
    users = get_all_users()
    await message.reply_text(f"🚀 Broadcasting to {len(users)} users...")
    success = 0
    for uid in users:
        try:
            await message.reply_to_message.copy(uid)
            success += 1
            await asyncio.sleep(0.1) # Prevent FloodWait
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.reply_to_message.copy(uid)
            success += 1
        except Exception:
            pass # User might have blocked the bot
    await message.reply_text(f"✅ Broadcast finished. Sent successfully to {success}/{len(users)} users.")

@app.on_callback_query(filters.regex(r"^cancel_(.+)"))
async def cancel_callback(client, callback_query):
    task_id = callback_query.data.split("_")[1]
    cancel_flags[task_id] = True
    await callback_query.answer("⚠️ Cancelling process... Please wait a moment.", show_alert=True)

@app.on_message(filters.regex(r"^https?://") & filters.private)
async def link_handler(client, message):
    user_id = message.from_user.id
    
    # 1. Anti-Spam Rate Limiting
    last_req = user_cooldowns.get(user_id, 0)
    if time.time() - last_req < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - (time.time() - last_req))
        await message.reply_text(f"🛑 Please slow down! Wait `{remaining}s` before sending another link.")
        return
    user_cooldowns[user_id] = time.time()

    # 2. Setup Task
    url = message.text.strip()
    task_id = f"{user_id}_{int(time.time())}"
    cancel_flags[task_id] = False
    
    progress_msg = await message.reply_text("📥 **Added to Queue...** Please wait.", reply_markup=get_cancel_button(task_id))
    
    # 3. Add to asyncio Queue
    await download_queue.put((user_id, url, progress_msg, task_id))


# ==========================================
# 7. MAIN ENTRY POINT (Subprocess & Runner)
# ==========================================
async def main():
    # 1. Initialize SQLite Database
    init_db()
    
    # 2. Boot up Aria2c daemon in the background via Subprocess
    logger.info("Starting Aria2c daemon...")
    subprocess.Popen([
        "aria2c", 
        "--enable-rpc", 
        "--rpc-listen-all=false", 
        "--rpc-listen-port=6800",
        "--max-connection-per-server=10", # Blazing fast splits
        "--min-split-size=10M",
        "--daemon=false" # Keeps attached to container lifecycle
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Allow a few seconds for Aria2 daemon to fully open its ports
    await asyncio.sleep(3)
    logger.info("Aria2c ready. Starting Telegram Bot...")

    # 3. Start Pyrogram Client
    await app.start()
    
    # 4. Spin up the Background Worker
    asyncio.create_task(worker())
    
    # 5. Keep bot alive
    logger.info("Bot is successfully running!")
    await idle()
    
    # 6. Graceful shutdown
    await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by User.")