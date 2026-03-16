#(c) GoFile Upload Integration by Bruce
import os
import asyncio
import logging
from pyrogram import filters, Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait
from info import ADMINS, LOG_CHANNEL, BIN_CHANNEL, STREAM_URL, GOFILE_TOKEN
from database.ia_filterdb import get_file_details
from database.link_cache_db import link_cache_db
from utils import get_size

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import GoFile upload functions
try:
    import gofile
    GOFILE_AVAILABLE = True
except ImportError:
    GOFILE_AVAILABLE = False
    logger.warning("GoFile module not found. GoFile upload will not work.")

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

@Client.on_callback_query(filters.regex(r"^gofileup"))
async def gofile_upload_handler(bot: Client, query: CallbackQuery):
    """Handle GoFile upload button click"""
    try:
        _, file_id = query.data.split("#")
        
        # Get file details from database
        files_ = await get_file_details(file_id)
        if not files_:
            return await query.answer('No such file exist.', show_alert=True)
        
        files = files_[0]
        
        # Check if GoFile module is available
        if not GOFILE_AVAILABLE:
            await query.answer(
                "⚠️ GoFile module not available!\\n\\n"
                "Please contact the bot administrator.",
                show_alert=True
            )
            return
        
        # Check if GOFILE_TOKEN is configured
        if not GOFILE_TOKEN:
            await query.answer(
                "⚠️ GoFile not configured!\\n\\n"
                "Please set GOFILE_TOKEN environment variable.",
                show_alert=True
            )
            return
        
        # Check if STREAM_URL is configured
        if not STREAM_URL or not STREAM_URL.startswith('http'):
            await query.answer(
                "⚠️ Streaming server not configured!\\n\\n"
                "Please set STREAM_URL environment variable.",
                show_alert=True
            )
            return
        
        # Check cache first
        cached_link = await link_cache_db.get_cached_link(file_id, "gofile")
        if cached_link:
            # Cache hit - return immediately
            msg_text = f"""**✨ GoFile Upload (Cached)!**

**📁 File:** `{files.file_name}`
**📦 Size:** `{get_size(files.file_size)}`

**🔗 GoFile Link:**
{cached_link}

**💎 Powered by @BruceDoesNotExist**"""
            
            await query.answer("✨ Retrieved from cache!", show_alert=False)
            await bot.send_message(
                chat_id=query.from_user.id,
                text=msg_text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔗 Open GoFile Link', url=cached_link)],
                    [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                ]),
                disable_web_page_preview=True
            )
            return
        
        # Cache miss - upload to GoFile
        await query.answer("⏳ Uploading to GoFile...", show_alert=False)
        sts = await bot.send_message(
            chat_id=query.from_user.id,
            text="⏳ **Uploading to GoFile...**\\n\\nPlease wait while we upload your file."
        )
        
        try:
            # Step 1: Forward the file to bin channel
            file_msg = await bot.copy_message(
                chat_id=BIN_CHANNEL,
                from_chat_id=query.message.chat.id,
                message_id=query.message.id
            )
            
            # Step 2: Generate streaming link from bin channel
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
            download_link = format_stream_url(STREAM_URL, file_msg.id, stream_hash)
            
            if not download_link:
                raise ValueError("Failed to generate valid streaming URL")
            
            # Update status message
            await sts.edit_text(
                "⏳ **Uploading to GoFile...It may takes Few Minutes,PleaseWait...**"
            )
            
            # Step 3: Upload the URL to GoFile
            gofile_link, file_name, file_size = await gofile.upload_url_to_gofile_streaming(
                url=download_link,
                suggested_name=files.file_name,
                download_progress_cb=None,
                upload_progress_cb=None,
                cancel_check=None
            )
            
            if not gofile_link:
                raise ValueError("Failed to upload to GoFile. Please try again later.")
            
            # Save to cache
            await link_cache_db.save_cached_link(
                file_id=file_id,
                link_type="gofile",
                url=gofile_link,
                file_name=files.file_name
            )
            
            # Step 4: Send the GoFile link back to user
            msg_text = f"""**🎉 GoFile Upload Successful!**

**📁 FileNName:** `{files.file_name}`
**📦 FileSize:** `{get_size(files.file_size)}`

**🔗 GoFile Link:**
{gofile_link}

**💎 Powered by @BruceDoesNotExist**"""
            
            try:
                await sts.edit_text(
                    text=msg_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('🔗 Open GoFile Link', url=gofile_link)],
                        [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                    ]),
                    disable_web_page_preview=True
                )
            except Exception as edit_error:
                # If edit fails, send new message
                logger.warning(f"Failed to edit message, sending new one: {edit_error}")
                await sts.delete()
                await bot.send_message(
                    chat_id=query.from_user.id,
                    text=msg_text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('🔗 Open GoFile Link', url=gofile_link)],
                        [InlineKeyboardButton('🔙 Back', callback_data=f'file#{file_id}')]
                    ]),
                    disable_web_page_preview=True
                )
            
            # Log to bin channel
            await bot.send_message(
                BIN_CHANNEL,
                text=f"**GoFile Upload Requested by:** [{query.from_user.first_name}](tg://user?id={query.from_user.id})\\n"
                     f"**User ID:** `{query.from_user.id}`\\n"
                     f"**File:** `{files.file_name}`\\n"
                     f"**Download Link:** {download_link}\\n"
                     f"**GoFile Link:** {gofile_link}",
                reply_to_message_id=file_msg.id,
                disable_web_page_preview=True
            )
            
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await sts.edit_text("⚠️ Rate limit hit. Please try again in a moment.")
        except Exception as e:
            logger.exception(f"Error uploading to GoFile: {e}")
            try:
                await sts.edit_text(
                    "❌ **Error uploading to GoFile!**\\n\\n"
                    f"**Error:** `{str(e)}`\\n\\n"
                    "Please try again later or contact support."
                )
            except:
                await bot.send_message(
                    chat_id=query.from_user.id,
                    text="❌ **Error uploading to GoFile!**\\n\\n"
                         f"**Error:** `{str(e)}`"
                )
            
    except Exception as e:
        logger.exception(f"GoFile upload handler error: {e}")
        await query.answer(f"An error occurred: {str(e)}", show_alert=True)
