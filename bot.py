"""
Telegram Media Downloader Bot
Production-ready bot with aria2c integration, queue system, and admin panel.
Single-file deployment for easy copy-paste.
"""

import os
import sys
import asyncio
import logging
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import aria2p
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified, UserIsBlocked, InputUserDeactivated

# ============================================================================
# CONFIGURATION & ENVIRONMENT VARIABLES
# ============================================================================

# Telegram API Credentials (Get from https://my.telegram.org)
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # Your Telegram User ID

# Paths
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)
DB_PATH = "users.db"

# Rate Limiting Configuration
RATE_LIMIT_SECONDS = 10
user_last_request: Dict[int, float] = {}

# Queue System
download_queue = asyncio.Queue()

# Active Downloads Tracking (for cancellation)
active_downloads: Dict[int, dict] = {}

# Aria2 Global Instance
aria2 = None

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def init_database():
    """Initialize SQLite database and create tables."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                join_date TEXT NOT NULL,
                first_name TEXT,
                username TEXT
            )
        """)
        conn.commit()
        conn.close()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}")
        sys.exit(1)


def add_user(user_id: int, first_name: str = "", username: str = ""):
    """Add or update user in database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """INSERT OR REPLACE INTO users (user_id, join_date, first_name, username) 
               VALUES (?, ?, ?, ?)""",
            (user_id, datetime.now().isoformat(), first_name, username or "")
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error adding user {user_id}: {e}")


def get_all_users():
    """Retrieve all user IDs from database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users")
        users = [row[0] for row in cursor.fetchall()]
        conn.close()
        return users
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        return []


def get_user_count():
    """Get total number of registered users."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Error getting user count: {e}")
        return 0

# ============================================================================
# ARIA2 DAEMON MANAGEMENT
# ============================================================================

def start_aria2_daemon():
    """Start aria2c RPC daemon as a background process."""
    global aria2
    try:
        logger.info("🚀 Starting aria2c daemon...")
        
        # Start aria2c with optimized settings
        subprocess.Popen([
            "aria2c",
            "--enable-rpc",
            "--rpc-listen-all=false",
            "--rpc-listen-port=6800",
            "--max-connection-per-server=10",
            "--rpc-max-request-size=1024M",
            "--seed-time=0.01",
            "--min-split-size=10M",
            "--follow-torrent=false",
            "--split=10",
            "--daemon=true",
            "--allow-overwrite=true",
            "--max-overall-download-limit=0",
            "--max-download-limit=0",
            "--max-concurrent-downloads=5",
            "--continue=true",
            "--max-overall-upload-limit=1K",
            "--max-upload-limit=1K",
            "--auto-file-renaming=false"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait for daemon to start
        time.sleep(4)
        
        # Connect to aria2 RPC
        aria2 = aria2p.API(
            aria2p.Client(
                host="http://localhost",
                port=6800,
                secret=""
            )
        )
        
        logger.info("✅ aria2c daemon started successfully")
        return aria2
        
    except FileNotFoundError:
        logger.critical("❌ aria2c not found! Please install aria2.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Failed to start aria2c: {e}")
        sys.exit(1)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def format_bytes(size: int) -> str:
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_time(seconds: int) -> str:
    """Convert seconds to HH:MM:SS format."""
    if seconds < 0:
        return "Unknown"
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def progress_bar(percentage: float, length: int = 12) -> str:
    """Generate visual progress bar."""
    filled = int(length * percentage / 100)
    bar = "█" * filled + "░" * (length - filled)
    return bar


def check_rate_limit(user_id: int) -> bool:
    """Check if user exceeded rate limit."""
    current_time = time.time()
    last_request = user_last_request.get(user_id, 0)
    
    if current_time - last_request < RATE_LIMIT_SECONDS:
        return True  # Rate limited
    
    user_last_request[user_id] = current_time
    return False

# ============================================================================
# PYROGRAM CLIENT
# ============================================================================

app = Client(
    "media_downloader_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=4
)

# ============================================================================
# DOWNLOAD FUNCTION (ARIA2C)
# ============================================================================

async def download_with_aria2(url: str, user_id: int, progress_msg: Message) -> Optional[Path]:
    """
    Download file using aria2c with real-time progress updates.
    Returns: Path to downloaded file or None if cancelled/failed
    """
    download = None
    try:
        logger.info(f"📥 Starting download for user {user_id}: {url}")
        
        # Add download to aria2
        download = aria2.add_uris([url], options={"dir": str(DOWNLOAD_DIR)})
        
        # Register in active downloads
        active_downloads[user_id] = {
            "type": "download",
            "aria2_download": download,
            "message": progress_msg
        }
        
        last_update = 0
        
        # Progress loop
        while not download.is_complete:
            await asyncio.sleep(1.5)
            download.update()
            
            # Check if cancelled
            if user_id not in active_downloads:
                logger.info(f"🚫 Download cancelled by user {user_id}")
                download.remove(force=True, files=True)
                return None
            
            # Check for errors
            if download.status == "error":
                error_msg = download.error_message or "Unknown error"
                raise Exception(f"Download failed: {error_msg}")
            
            # Update progress (throttle updates)
            current_time = time.time()
            if current_time - last_update < 2:
                continue
            
            last_update = current_time
            
            percentage = download.progress
            speed = download.download_speed
            eta = download.eta
            total = download.total_length
            completed = download.completed_length
            
            progress_text = (
                f"📥 **Downloading File...**\n\n"
                f"`{download.name[:50]}...`\n\n"
                f"{progress_bar(percentage)} **{percentage:.1f}%**\n\n"
                f"🔽 **Speed:** `{format_bytes(speed)}/s`\n"
                f"📦 **Downloaded:** `{format_bytes(completed)}` / `{format_bytes(total)}`\n"
                f"⏱ **ETA:** `{format_time(eta.seconds) if eta else 'Calculating...'}`"
            )
            
            try:
                await progress_msg.edit_text(
                    progress_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🚫 Cancel Download", callback_data=f"cancel_{user_id}")]
                    ])
                )
            except MessageNotModified:
                pass
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.warning(f"Progress update error: {e}")
        
        # Final update
        download.update()
        
        if download.status == "complete":
            file_path = Path(download.files[0].path)
            logger.info(f"✅ Download completed: {file_path.name}")
            return file_path
        else:
            raise Exception(f"Download ended with unexpected status: {download.status}")
            
    except Exception as e:
        logger.error(f"❌ Download error for user {user_id}: {e}")
        if download:
            try:
                download.remove(force=True, files=True)
            except:
                pass
        raise
    finally:
        if user_id in active_downloads and active_downloads[user_id].get("type") == "download":
            del active_downloads[user_id]

# ============================================================================
# UPLOAD FUNCTION (PYROGRAM)
# ============================================================================

async def upload_to_telegram(file_path: Path, user_id: int, progress_msg: Message, client: Client):
    """
    Upload file to Telegram with real-time progress.
    """
    try:
        logger.info(f"📤 Starting upload for user {user_id}: {file_path.name}")
        
        # Register in active uploads
        active_downloads[user_id] = {
            "type": "upload",
            "file_path": file_path,
            "message": progress_msg,
            "cancelled": False
        }
        
        file_size = file_path.stat().st_size
        start_time = time.time()
        last_update = 0
        
        async def progress_callback(current, total):
            nonlocal last_update
            
            # Check cancellation
            if user_id in active_downloads and active_downloads[user_id].get("cancelled"):
                raise Exception("Upload cancelled by user")
            
            # Throttle updates
            current_time = time.time()
            if current_time - last_update < 2:
                return
            
            last_update = current_time
            
            percentage = (current / total) * 100
            elapsed = current_time - start_time
            speed = current / elapsed if elapsed > 0 else 0
            eta = (total - current) / speed if speed > 0 else 0
            
            progress_text = (
                f"📤 **Uploading to Telegram...**\n\n"
                f"`{file_path.name[:50]}...`\n\n"
                f"{progress_bar(percentage)} **{percentage:.1f}%**\n\n"
                f"🔼 **Speed:** `{format_bytes(int(speed))}/s`\n"
                f"📦 **Uploaded:** `{format_bytes(current)}` / `{format_bytes(total)}`\n"
                f"⏱ **ETA:** `{format_time(int(eta))}`"
            )
            
            try:
                await progress_msg.edit_text(
                    progress_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🚫 Cancel Upload", callback_data=f"cancel_{user_id}")]
                    ])
                )
            except (MessageNotModified, FloodWait):
                pass
            except Exception:
                pass
        
        # Send document to user
        await client.send_document(
            chat_id=user_id,
            document=str(file_path),
            file_name=file_path.name,
            progress=progress_callback,
            caption=f"✅ **File:** `{file_path.name}`\n📦 **Size:** `{format_bytes(file_size)}`"
        )
        
        logger.info(f"✅ Upload completed: {file_path.name}")
        
    except Exception as e:
        if "Upload cancelled" in str(e):
            logger.info(f"🚫 Upload cancelled by user {user_id}")
        else:
            logger.error(f"❌ Upload error: {e}")
        raise
    finally:
        if user_id in active_downloads and active_downloads[user_id].get("type") == "upload":
            del active_downloads[user_id]

# ============================================================================
# QUEUE WORKER (BACKGROUND TASK)
# ============================================================================

async def queue_worker():
    """
    Background worker that processes download queue sequentially.
    Prevents overload by handling one job at a time.
    """
    logger.info("⚙️ Queue worker started")
    
    while True:
        try:
            # Wait for job
            job = await download_queue.get()
            user_id = job["user_id"]
            url = job["url"]
            message = job["message"]
            
            progress_msg = None
            file_path = None
            
            try:
                # Initial status
                progress_msg = await message.reply_text(
                    "⏳ **Initializing download...**\n\n"
                    "Please wait while we prepare your file.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🚫 Cancel", callback_data=f"cancel_{user_id}")]
                    ])
                )
                
                # Step 1: Download with aria2c
                file_path = await download_with_aria2(url, user_id, progress_msg)
                
                if file_path is None:
                    # Cancelled
                    await progress_msg.edit_text("❌ **Download cancelled by user.**")
                    continue
                
                # Step 2: Upload to Telegram
                await upload_to_telegram(file_path, user_id, progress_msg, app)
                
                # Success
                await progress_msg.edit_text(
                    f"✅ **File Delivered Successfully!**\n\n"
                    f"📁 **Name:** `{file_path.name}`\n"
                    f"📦 **Size:** `{format_bytes(file_path.stat().st_size)}`\n"
                    f"⚡ **Processing complete!**"
                )
                
            except Exception as e:
                error_message = str(e)[:300]
                logger.error(f"Job processing failed for user {user_id}: {e}")
                
                if progress_msg:
                    try:
                        await progress_msg.edit_text(
                            f"❌ **Error Occurred**\n\n"
                            f"**Details:** `{error_message}`\n\n"
                            f"Please check your link and try again."
                        )
                    except:
                        pass
            
            finally:
                # Cleanup: Delete temporary file
                if file_path and file_path.exists():
                    try:
                        file_path.unlink()
                        logger.info(f"🗑 Deleted temp file: {file_path.name}")
                    except Exception as e:
                        logger.error(f"Failed to delete {file_path}: {e}")
                
                download_queue.task_done()
                
        except Exception as e:
            logger.error(f"Queue worker critical error: {e}")
            await asyncio.sleep(1)

# ============================================================================
# COMMAND HANDLERS
# ============================================================================

@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Handle /start command - Welcome message"""
    user = message.from_user
    add_user(user.id, user.first_name, user.username)
    
    welcome_text = (
        f"👋 **Welcome {user.first_name}!**\n\n"
        f"🤖 **I'm a powerful Media Downloader Bot**\n\n"
        f"**📥 Features:**\n"
        f"✅ Download files up to 2GB\n"
        f"✅ Real-time progress tracking\n"
        f"✅ Fast multi-threaded downloads\n"
        f"✅ Cancel anytime with one click\n"
        f"✅ Queue system for multiple requests\n\n"
        f"**🚀 How to use:**\n"
        f"Just send me any direct download link (HTTP/HTTPS)\n\n"
        f"**Example:**\n"
        f"`https://example.com/file.zip`\n\n"
        f"**💡 Tip:** You can send multiple links, they'll be queued!"
    )
    
    await message.reply_text(welcome_text)


@app.on_message(filters.command("help") & filters.private)
async def help_command(client: Client, message: Message):
    """Handle /help command"""
    help_text = (
        f"📖 **Help & Information**\n\n"
        f"**Available Commands:**\n"
        f"/start - Start the bot\n"
        f"/help - Show this message\n"
        f"/stats - Bot statistics (Admin only)\n"
        f"/broadcast - Broadcast message (Admin only)\n\n"
        f"**How it works:**\n"
        f"1. Send a direct download link\n"
        f"2. Bot downloads it using aria2c\n"
        f"3. File is uploaded to Telegram\n"
        f"4. You receive the file!\n\n"
        f"**Supported:**\n"
        f"✅ Direct HTTP/HTTPS links\n"
        f"✅ Files up to 2GB\n"
        f"✅ Any file type\n\n"
        f"**Note:** Torrent/Magnet links are not supported."
    )
    await message.reply_text(help_text)


@app.on_message(filters.command("stats") & filters.private)
async def stats_command(client: Client, message: Message):
    """Handle /stats command - Show bot statistics (Admin only)"""
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("❌ **Access Denied**\n\nThis command is for admins only.")
        return
    
    total_users = get_user_count()
    queue_size = download_queue.qsize()
    active_tasks = len(active_downloads)
    
    stats_text = (
        f"📊 **Bot Statistics**\n\n"
        f"👥 **Total Users:** `{total_users}`\n"
        f"📋 **Queue Size:** `{queue_size}`\n"
        f"⚡ **Active Tasks:** `{active_tasks}`\n"
        f"🕐 **Uptime:** Running\n"
        f"💾 **Database:** SQLite\n"
        f"⚙️ **Downloader:** aria2c"
    )
    
    await message.reply_text(stats_text)


@app.on_message(filters.command("broadcast") & filters.private & filters.reply)
async def broadcast_command(client: Client, message: Message):
    """Handle /broadcast command - Send message to all users (Admin only)"""
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("❌ **Access Denied**\n\nThis command is for admins only.")
        return
    
    users = get_all_users()
    broadcast_msg = message.reply_to_message
    
    if not users:
        await message.reply_text("❌ No users found in database.")
        return
    
    status_msg = await message.reply_text(
        f"📢 **Broadcasting...**\n\n"
        f"👥 Total users: {len(users)}\n"
        f"⏳ Please wait..."
    )
    
    success = 0
    failed = 0
    blocked = 0
    
    for user_id in users:
        try:
            await broadcast_msg.copy(user_id)
            success += 1
            await asyncio.sleep(0.05)  # Avoid flood
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await broadcast_msg.copy(user_id)
                success += 1
            except:
                failed += 1
        except (UserIsBlocked, InputUserDeactivated):
            blocked += 1
        except Exception as e:
            failed += 1
            logger.error(f"Broadcast error for {user_id}: {e}")
    
    await status_msg.edit_text(
        f"✅ **Broadcast Complete!**\n\n"
        f"✅ **Success:** {success}\n"
        f"❌ **Failed:** {failed}\n"
        f"🚫 **Blocked:** {blocked}\n"
        f"📊 **Total:** {len(users)}"
    )


@app.on_message(filters.regex(r"^https?://") & filters.private)
async def handle_download_link(client: Client, message: Message):
    """Handle direct download links from users"""
    user_id = message.from_user.id
    url = message.text.strip()
    
    # Add user to database
    user = message.from_user
    add_user(user_id, user.first_name, user.username)
    
    # Rate limiting check
    if check_rate_limit(user_id):
        remaining = RATE_LIMIT_SECONDS - (time.time() - user_last_request.get(user_id, 0))
        await message.reply_text(
            f"⏱ **Slow Down!**\n\n"
            f"Please wait **{int(remaining)}** seconds before sending another link.\n"
            f"This prevents server overload."
        )
        return
    
    # Basic URL validation
    if not url.startswith(("http://", "https://")):
        await message.reply_text(
            "❌ **Invalid URL**\n\n"
            "Please send a valid HTTP or HTTPS direct download link."
        )
        return
    
    # Add to download queue
    await download_queue.put({
        "user_id": user_id,
        "url": url,
        "message": message
    })
    
    queue_position = download_queue.qsize()
    
    if queue_position > 1:
        await message.reply_text(
            f"✅ **Added to Queue**\n\n"
            f"📍 **Position:** #{queue_position}\n"
            f"⏳ Please wait for your turn...\n\n"
            f"You'll be notified when processing starts."
        )
    else:
        await message.reply_text(
            "🚀 **Processing Started**\n\n"
            "Your download will begin shortly..."
        )

# ============================================================================
# CALLBACK QUERY HANDLER (Cancel Button)
# ============================================================================

@app.on_callback_query(filters.regex(r"^cancel_\d+$"))
async def handle_cancel_callback(client: Client, callback: CallbackQuery):
    """Handle cancel button press"""
    try:
        user_id = int(callback.data.split("_")[1])
        
        # Authorization check
        if callback.from_user.id != user_id:
            await callback.answer("❌ This is not your download!", show_alert=True)
            return
        
        # Check if task exists
        if user_id not in active_downloads:
            await callback.answer("❌ No active task found.", show_alert=True)
            return
        
        task_info = active_downloads[user_id]
        task_type = task_info["type"]
        
        # Cancel based on type
        if task_type == "download":
            try:
                aria2_download = task_info["aria2_download"]
                aria2_download.remove(force=True, files=True)
                logger.info(f"🚫 Cancelled download for user {user_id}")
            except Exception as e:
                logger.error(f"Error cancelling aria2 download: {e}")
        
        elif task_type == "upload":
            active_downloads[user_id]["cancelled"] = True
            logger.info(f"🚫 Cancelled upload for user {user_id}")
        
        # Remove from tracking
        del active_downloads[user_id]
        
        # Update message
        await callback.message.edit_text(
            "❌ **Operation Cancelled**\n\n"
            "Your download/upload has been cancelled successfully."
        )
        await callback.answer("✅ Cancelled!", show_alert=False)
        
    except Exception as e:
        logger.error(f"Cancel callback error: {e}")
        await callback.answer("❌ Error occurred", show_alert=True)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

async def main():
    """Main function - Initialize and start bot"""
    global aria2
    
    logger.info("=" * 50)
    logger.info("🤖 TELEGRAM MEDIA DOWNLOADER BOT")
    logger.info("=" * 50)
    
    # Validation
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.critical("❌ Missing environment variables! Set API_ID, API_HASH, and BOT_TOKEN")
        sys.exit(1)
    
    # Initialize database
    init_database()
    
    # Start aria2c daemon
    aria2 = start_aria2_daemon()
    
    # Start Pyrogram client
    await app.start()
    me = await app.get_me()
    logger.info(f"✅ Bot started: @{me.username}")
    logger.info(f"👤 Admin ID: {ADMIN_ID}")
    
    # Start background queue worker
    asyncio.create_task(queue_worker())
    
    logger.info("=" * 50)
    logger.info("✅ All systems operational!")
    logger.info("=" * 50)
    
    # Keep bot running
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user (Ctrl+C)")
    except Exception as e:
        logger.critical(f"💥 Critical error: {e}", exc_info=True)
        sys.exit(1)
