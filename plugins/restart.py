import os
import sys
import logging
from pyrogram import Client, filters, enums
from info import ADMINS

logger = logging.getLogger(__name__)

@Client.on_message(filters.command('restart') & filters.user(ADMINS))
async def restart_bot(client, message):
    """Manually restart the bot process."""
    msg = await message.reply("🔄 <b>Restarting bot...</b> Please wait.", quote=True, parse_mode=enums.ParseMode.HTML)
    
    logger.info("🔄 Manual restart initiated via /restart command.")
    
    try:
        # Replaces the current process with a new one
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        logger.error(f"os.execv failed during manual restart: {e}. Falling back to sys.exit(0).")
        # Fallback to sys.exit, relying on the process manager (Docker, Heroku, systemd, etc.) to restart it
        sys.exit(0)
