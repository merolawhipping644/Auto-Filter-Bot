#(c) Integration by Bruce
import os
import asyncio
import base64
from pyrogram import filters, Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from info import ADMINS, LOG_CHANNEL
from database.ia_filterdb import get_file_details
from database.link_cache_db import link_cache_db
from utils import get_size
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configuration for bin channel streaming
BIN_CHANNEL = int(os.environ.get('BIN_CHANNEL', LOG_CHANNEL))  # Uses LOG_CHANNEL as default
STREAM_URL = os.environ.get('STREAM_URL', '')  # Your streaming server URL (e.g., https://yourapp.herokuapp.com)

def get_hash(media_msg):
    """Generate hash for stream URL"""
    media = media_msg.document or media_msg.video or media_msg.audio or media_msg.photo
    if media:
        return media.file_unique_id[:6]
    return ""

def format_stream_url(base_url, msg_id, hash_val):
    """Format streaming URL properly"""
    if not base_url:
        return None
    
    # Ensure URL ends with /
    if not base_url.endswith('/'):
        base_url += '/'
    
    # Create the full URL
    return f"{base_url}{msg_id}?hash={hash_val}"

@Client.on_callback_query(filters.regex(r"^fastdl"))
async def fast_download_handler(bot: Client, query: CallbackQuery):
    """Handle fast download link generation"""
    try:
        _, file_id = query.data.split("#")
        
        # Get file details from database
        files_ = await get_file_details(file_id)
        if not files_:
            return await query.answer('No such file exist.', show_alert=True)
        
        files = files_[0]
        
        # Check if STREAM_URL is configured and valid
        if not STREAM_URL or not STREAM_URL.startswith('http'):
            await query.answer(
                "⚠️ Streaming server not configured!\n\n"
                "Please set STREAM_URL environment variable.",
                show_alert=True
            )
            return
        
        # Check cache first
        cached_link = await link_cache_db.get_cached_link(file_id, "fastdownload")
        if cached_link:
            # Cache hit - return immediately
            msg_text = f"""**✨ Fast Download Link (Cached)!**

**📁 FileName:** `{files.file_name}`
**📦 FileSize:** `{get_size(files.file_size)}`

**🔗 Download Link:**
{cached_link}

**⚡ This link supports:**
• Direct Download
• Use ADM/1DM For For Fast Downloads And Resume Support

**💾 Cached:** This link was retrieved instantly from cache!

Powered By @TG_MOVIES4U"""
            
            await query.answer("✨ Retrieved from cache!", show_alert=False)
            await bot.send_message(
                chat_id=query.from_user.id,
                text=msg_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('📥 Download Now', url=cached_link)],
                    [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                ])
            )
            return
        
        # Cache miss - generate new link
        await query.answer("⏳ Generating link...", show_alert=False)
        sts = await bot.send_message(
            chat_id=query.from_user.id,
            text="⏳ Generating fast download link..."
        )
        
        try:
            # Forward the file to bin channel using copy_message
            # First, we need to send the file to bin channel
            file_msg = await bot.copy_message(
                chat_id=BIN_CHANNEL,
                from_chat_id=query.message.chat.id,
                message_id=query.message.id
            )
            
            # Generate streaming link
            # Get file unique id for hash
            msg_info = await bot.get_messages(BIN_CHANNEL, file_msg.id)
            if msg_info.document:
                file_unique_id = msg_info.document.file_unique_id
            elif msg_info.video:
                file_unique_id = msg_info.video.file_unique_id
            elif msg_info.audio:
                file_unique_id = msg_info.audio.file_unique_id
            else:
                file_unique_id = "default"
                
            stream_hash = file_unique_id[:6] if file_unique_id else "hash00"
            online_link = format_stream_url(STREAM_URL, file_msg.id, stream_hash)
            
            if not online_link:
                raise ValueError("Failed to generate valid streaming URL")
            
            # Save to cache
            await link_cache_db.save_cached_link(
                file_id=file_id,
                link_type="fastdownload",
                url=online_link,
                file_name=files.file_name
            )
            
            # Send the link back to user
            msg_text = f"""**🎬 Fast Download Link Generated!**

**📁 FileName:** `{files.file_name}`
**📦 FileSize:** `{get_size(files.file_size)}`

**🔗 Download Link:**
{online_link}

**⚡ This link supports:**
• Direct Download
• Use ADM/1DM For For Fast Downloads And Resume Support

**⏰ Note:** Link Will Expires At Anytime!

Powered By @TG_MOVIES4U"""
            
            try:
                await sts.edit_text(
                    text=msg_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('📥 Download Now', url=online_link)],
                        [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                    ])
                )
            except Exception as edit_error:
                # If edit fails, send new message
                logger.warning(f"Failed to edit message, sending new one: {edit_error}")
                await sts.delete()
                await bot.send_message(
                    chat_id=query.from_user.id,
                    text=msg_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('📥 Download Now', url=online_link)],
                        [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                    ])
                )
            
            # Log to bin channel
            await bot.send_message(
                BIN_CHANNEL,
                text=f"**Requested by:** [{query.from_user.first_name}](tg://user?id={query.from_user.id})\n"
                     f"**User ID:** `{query.from_user.id}`\n"
                     f"**File:** `{files.file_name}`\n"
                     f"**Link:** {online_link}",
                reply_to_message_id=file_msg.id,
                disable_web_page_preview=True
            )
            
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await sts.edit_text("⚠️ Please try again in a moment.")
        except Exception as e:
            logger.exception(f"Error generating fast download link: {e}")
            try:
                await sts.edit_text(
                    "❌ **Error generating link!**\n\n"
                    f"**Error:** `{str(e)}`\n\n"
                    "Please make sure:\n"
                    "• Bot is admin in BIN_CHANNEL\n"
                    "• STREAM_URL is configured correctly\n"
                    "• Try again or contact support."
                )
            except:
                await bot.send_message(
                    chat_id=query.from_user.id,
                    text="❌ **Error generating link!**\n\n"
                         f"**Error:** `{str(e)}`"
                )
            
            
    except Exception as e:
        logger.exception(f"Fast download handler error: {e}")
        await query.answer(f"An error occurred: {str(e)}", show_alert=True)
