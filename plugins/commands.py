import os
import logging
import random
import asyncio
from urllib.parse import unquote_plus
from Script import script
from pyrogram import Client, filters, enums
from pyrogram.errors import ChatAdminRequired, FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from database.ia_filterdb import Media, Media2, get_file_details, get_search_results, unpack_new_file_id
from database.users_chats_db import db
from info import CHANNELS, ADMINS, AUTH_CHANNEL, AUTH_CHANNEL_2, MULTI_FORCESUB, LOG_CHANNEL, PICS, BATCH_FILE_CAPTION, CUSTOM_FILE_CAPTION, PROTECT_CONTENT, ENABLE_STREAM_LINK, ENABLE_GOFILE_LINK
from utils import (
    get_settings,
    get_size,
    is_subscribed,
    save_group_settings,
    temp,
    send_document_with_anonymous_filename,
)
from sanitizers import clean_file_name, clean_caption, normalize_for_dedup
from database.connections_mdb import active_connection
import re
import json
import base64
logger = logging.getLogger(__name__)

BATCH_FILES = {}


def _build_media_caption(media_doc: Media) -> str:
    title = media_doc.file_name
    size = get_size(media_doc.file_size)
    caption = media_doc.caption

    if CUSTOM_FILE_CAPTION:
        try:
            caption = CUSTOM_FILE_CAPTION.format(
                file_name="" if title is None else title,
                file_size="" if size is None else size,
                file_caption="" if caption is None else caption,
            )
        except Exception as exc:  # noqa: BLE001 - guard formatting issues
            logger.warning("Failed to format custom caption: %s", exc)
            caption = media_doc.caption

    if caption is None:
        caption = f"{title}"

    return clean_caption(caption)


async def _send_movie_results(client: Client, message, slug: str) -> None:
    query_text = unquote_plus(slug).strip()
    if not query_text:
        await message.reply("Movie name is missing in this link. Please try again.")
        return

    files, _, total_results = await get_search_results(query_text, offset=0, max_results=20, filter=True)

    if not files:
        await message.reply(f"No files found for <b>{query_text}</b>.", parse_mode=enums.ParseMode.HTML)
        return

    if total_results > len(files):
        notice = (
            f"Found <b>{total_results}</b> files for <b>{query_text}</b>. "
            "Sending the 20 most recent entries. Use inline search for more."
        )
    else:
        notice = (
            f"Found <b>{total_results}</b> file{'s' if total_results != 1 else ''} "
            f"for <b>{query_text}</b>."
        )

    await message.reply(notice, parse_mode=enums.ParseMode.HTML)

    target_chat = message.from_user.id if message.from_user else message.chat.id

    for media_doc in files:
        caption = _build_media_caption(media_doc)
        
        # Add fast download link button and GoFile upload button
        btn = []
        if ENABLE_STREAM_LINK:
            btn.append([InlineKeyboardButton("⚡ Fast Download Link", callback_data=f"fastdl#{media_doc.file_id}")])
        if ENABLE_GOFILE_LINK:
            btn.append([InlineKeyboardButton("📤 GoFile Upload", callback_data=f"gofileup#{media_doc.file_id}")])
        reply_markup = InlineKeyboardMarkup(btn) if btn else None
        
        await send_document_with_anonymous_filename(
            client,
            chat_id=target_chat,
            media=media_doc,
            caption=caption,
            protect_content=PROTECT_CONTENT,
            reply_markup=reply_markup,
        )
        await asyncio.sleep(1)

@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        buttons = [
            [
                InlineKeyboardButton('🤖 𝚄𝚙𝚍𝚊𝚝𝚎𝚜', url='https://t.me/BrUceDoesNotExiSt')
            ],
            [
                InlineKeyboardButton('ℹ️ 𝙷𝚎𝚕𝚙', url=f"https://t.me/{temp.U_NAME}?start=help"),
            ]
            ]
        reply_markup = InlineKeyboardMarkup(buttons)
        await message.reply(script.START_TXT.format(message.from_user.mention if message.from_user else message.chat.title, temp.U_NAME, temp.B_NAME), reply_markup=reply_markup)
        await asyncio.sleep(2) # 😢 https://github.com/GreyMattersbot/EvaMaria/blob/master/plugins/p_ttishow.py#L17 😬 wait a bit, before checking.
        if not await db.get_chat(message.chat.id):
            total=await client.get_chat_members_count(message.chat.id)
            try:
                await client.send_message(LOG_CHANNEL, script.LOG_TEXT_G.format(message.chat.title, message.chat.id, total, "Unknown"))       
                await db.add_chat(message.chat.id, message.chat.title)
            except Exception as e:
                logger.error(f"Failed to add new group to DB: {e}")
        return  
    if not await db.is_user_exist(message.from_user.id):
        try:
            await db.add_user(message.from_user.id, message.from_user.first_name)
            await client.send_message(LOG_CHANNEL, script.LOG_TEXT_P.format(message.from_user.id, message.from_user.mention))
        except Exception as e:
            logger.error(f"Failed to add new user {message.from_user.id} to DB (quota full?): {e}")
    if len(message.command) != 2:
        buttons = [[
            InlineKeyboardButton('➕ 𝙰𝚍𝚍 𝙼𝚎 𝚃𝚘 𝚈𝚘𝚞𝚛 𝙶𝚛𝚘𝚞𝚙𝚜 ➕', url=f'http://t.me/{temp.U_NAME}?startgroup=true')
            ],[
            InlineKeyboardButton('🔍 𝚂𝚎𝚊𝚛𝚌𝚑', switch_inline_query_current_chat=''),
            InlineKeyboardButton('🤖 𝚄𝚙𝚍𝚊𝚝𝚎𝚜', url='https://t.me/BrUceDoesNotExiSt')
            ],[
            InlineKeyboardButton('ℹ️ 𝙷𝚎𝚕𝚙', callback_data='help'),
            InlineKeyboardButton('😊 𝙰𝚋𝚘𝚞𝚝', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        try:
            await message.reply_photo(
                photo=random.choice(PICS),
                caption=script.START_TXT.format(message.from_user.mention, temp.U_NAME, temp.B_NAME),
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        except Exception:
            await message.reply_text(
                text=script.START_TXT.format(message.from_user.mention, temp.U_NAME, temp.B_NAME),
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        return
    if AUTH_CHANNEL and not await is_subscribed(client, message):
        try:
            invite_link = await client.create_chat_invite_link(int(AUTH_CHANNEL))
        except ChatAdminRequired:
            logger.error("Make sure Bot is admin in Forcesub channel")
            return
        btn = [
            [
                InlineKeyboardButton(
                    "🤖 Join Channel 1 🤖", url=invite_link.invite_link
                )
            ]
        ]
        
        # Add second channel button if MULTI_FORCESUB is enabled
        if MULTI_FORCESUB and AUTH_CHANNEL_2:
            try:
                invite_link_2 = await client.create_chat_invite_link(int(AUTH_CHANNEL_2))
                btn.append([
                    InlineKeyboardButton(
                        "🎬 Join Channel 2 🎬", url=invite_link_2.invite_link
                    )
                ])
            except ChatAdminRequired:
                logger.error("Make sure Bot is admin in Second Forcesub channel")
                
        if message.command[1] != "subscribe":
            try:
                kk, file_id = message.command[1].split("_", 1)
                pre = 'checksubp' if kk == 'filep' else 'checksub' 
                btn.append([InlineKeyboardButton(" 🔄 Try Again", callback_data=f"{pre}#{file_id}")])
            except (IndexError, ValueError):
                btn.append([InlineKeyboardButton(" 🔄 Try Again", url=f"https://t.me/{temp.U_NAME}?start={message.command[1]}")])
        await client.send_message(
            chat_id=message.from_user.id,
            text="**Please Join Below Channels to use this Bot! Note : It's A Promotional Channels,You Can Left Later After Certain Time!**",
            reply_markup=InlineKeyboardMarkup(btn),
            parse_mode=enums.ParseMode.MARKDOWN
            )
        return
    if len(message.command) == 2 and message.command[1] in ["subscribe", "error", "okay", "help"]:
        buttons = [[
            InlineKeyboardButton('➕ 𝙰𝚍𝚍 𝙼𝚎 𝚃𝚘 𝚈𝚘𝚞𝚛 𝙶𝚛𝚘𝚞𝚙𝚜 ➕', url=f'http://t.me/{temp.U_NAME}?startgroup=true')
            ],[
            InlineKeyboardButton('🔍 𝚂𝚎𝚊𝚛𝚌𝚑', switch_inline_query_current_chat=''),
            InlineKeyboardButton('🤖 𝚄𝚙𝚍𝚊𝚝𝚎𝚜', url='https://t.me/BrUceDoesNotExiSt')
            ],[
            InlineKeyboardButton('ℹ️ 𝙷𝚎𝚕𝚙', callback_data='help'),
            InlineKeyboardButton('😊 𝙰𝚋𝚘𝚞𝚝', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        try:
            await message.reply_photo(
                photo=random.choice(PICS),
                caption=script.START_TXT.format(message.from_user.mention, temp.U_NAME, temp.B_NAME),
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        except Exception:
            await message.reply_text(
                text=script.START_TXT.format(message.from_user.mention, temp.U_NAME, temp.B_NAME),
                reply_markup=reply_markup,
                parse_mode=enums.ParseMode.HTML
            )
        return
    data = message.command[1]
    try:
        pre, file_id = data.split('_', 1)
    except:
        file_id = data
        pre = ""
    if data.split("-", 1)[0] == "BATCH":
        sts = await message.reply("Please wait")
        file_id = data.split("-", 1)[1]
        msgs = BATCH_FILES.get(file_id)
        if not msgs:
            file = await client.download_media(file_id)
            try: 
                with open(file) as file_data:
                    msgs=json.loads(file_data.read())
            except:
                await sts.edit("FAILED")
                return await client.send_message(LOG_CHANNEL, "UNABLE TO OPEN FILE.")
            os.remove(file)
            BATCH_FILES[file_id] = msgs
        for msg in msgs:
            title = msg.get("title")
            size=get_size(int(msg.get("size", 0)))
            f_caption=msg.get("caption", "")
            if BATCH_FILE_CAPTION:
                try:
                    f_caption=BATCH_FILE_CAPTION.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='' if f_caption is None else f_caption)
                except Exception as e:
                    logger.exception(e)
                    f_caption=f_caption
            if f_caption is None:
                f_caption = f"{title}"
            f_caption = clean_caption(f_caption)
            try:
                await client.send_cached_media(
                    chat_id=message.from_user.id,
                    file_id=msg.get("file_id"),
                    caption=f_caption,
                    protect_content=msg.get('protect', False),
                    )
            except FloodWait as e:
                await asyncio.sleep(e.x)
                logger.warning(f"Floodwait of {e.x} sec.")
                await client.send_cached_media(
                    chat_id=message.from_user.id,
                    file_id=msg.get("file_id"),
                    caption=f_caption,
                    protect_content=msg.get('protect', False),
                    )
            except Exception as e:
                logger.warning(e, exc_info=True)
                continue
            await asyncio.sleep(1) 
        await sts.delete()
        return
    elif data.split("-", 1)[0] == "DSTORE":
        sts = await message.reply("Please wait")
        b_string = data.split("-", 1)[1]
        decoded = (base64.urlsafe_b64decode(b_string + "=" * (-len(b_string) % 4))).decode("ascii")
        try:
            f_msg_id, l_msg_id, f_chat_id, protect = decoded.split("_", 3)
        except:
            f_msg_id, l_msg_id, f_chat_id = decoded.split("_", 2)
            protect = "/pbatch" if PROTECT_CONTENT else "batch"
        diff = int(l_msg_id) - int(f_msg_id)
        async for msg in client.iter_messages(int(f_chat_id), int(l_msg_id), int(f_msg_id)):
            if msg.media:
                media = getattr(msg, msg.media.value)
                if BATCH_FILE_CAPTION:
                    try:
                        f_caption=BATCH_FILE_CAPTION.format(file_name=getattr(media, 'file_name', ''), file_size=getattr(media, 'file_size', ''), file_caption=getattr(msg, 'caption', ''))
                    except Exception as e:
                        logger.exception(e)
                        f_caption = getattr(msg, 'caption', '')
                else:
                    media = getattr(msg, msg.media.value)
                    file_name = getattr(media, 'file_name', '')
                    f_caption = getattr(msg, 'caption', file_name)
                f_caption = clean_caption(f_caption)
                try:
                    await msg.copy(message.chat.id, caption=f_caption, protect_content=True if protect == "/pbatch" else False)
                except FloodWait as e:
                    await asyncio.sleep(e.x)
                    await msg.copy(message.chat.id, caption=f_caption, protect_content=True if protect == "/pbatch" else False)
                except Exception as e:
                    logger.exception(e)
                    continue
            elif msg.empty:
                continue
            else:
                try:
                    await msg.copy(message.chat.id, protect_content=True if protect == "/pbatch" else False)
                except FloodWait as e:
                    await asyncio.sleep(e.x)
                    await msg.copy(message.chat.id, protect_content=True if protect == "/pbatch" else False)
                except Exception as e:
                    logger.exception(e)
                    continue
            await asyncio.sleep(1) 
        return await sts.delete()


    if pre == "movie":
        await _send_movie_results(client, message, file_id)
        return

    files_ = await get_file_details(file_id)
    if not files_:
        pre, file_id = ((base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))).decode("ascii")).split("_", 1)
        try:
            msg = await client.send_cached_media(
                chat_id=message.from_user.id,
                file_id=file_id,
                protect_content=True if pre == 'filep' else False,
                )
            filetype = msg.media
            file = getattr(msg, filetype.value)
            title = file.file_name
            size=get_size(file.file_size)
            f_caption = f"<code>{title}</code>"
            if CUSTOM_FILE_CAPTION:
                try:
                    f_caption=CUSTOM_FILE_CAPTION.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='')
                except:
                    return
            f_caption = clean_caption(f_caption)
            await msg.edit_caption(f_caption)
            return
        except:
            pass
        return await message.reply('No such file exist.')
    files = files_[0]
    title = files.file_name
    size=get_size(files.file_size)
    f_caption=files.caption
    if CUSTOM_FILE_CAPTION:
        try:
            f_caption=CUSTOM_FILE_CAPTION.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='' if f_caption is None else f_caption)
        except Exception as e:
            logger.exception(e)
            f_caption=f_caption
    if f_caption is None:
        f_caption = f"{files.file_name}"
    f_caption = clean_caption(f_caption)
    
    # Add fast download link button and GoFile upload button
    btn = []
    if ENABLE_STREAM_LINK:
        btn.append([InlineKeyboardButton("⚡ Fast Download Link", callback_data=f"fastdl#{file_id}")])
    if ENABLE_GOFILE_LINK:
        btn.append([InlineKeyboardButton("📤 GoFile Upload", callback_data=f"gofileup#{file_id}")])
    reply_markup = InlineKeyboardMarkup(btn) if btn else None
    
    await send_document_with_anonymous_filename(
        client,
        chat_id=message.from_user.id,
        media=files,
        caption=f_caption,
        protect_content=True if pre == 'filep' else False,
        reply_markup=reply_markup,
    )
                    

@Client.on_message(filters.command('channel') & filters.user(ADMINS))
async def channel_info(bot, message):
           
    """Send basic information of channel"""
    if isinstance(CHANNELS, (int, str)):
        channels = [CHANNELS]
    elif isinstance(CHANNELS, list):
        channels = CHANNELS
    else:
        raise ValueError("Unexpected type of CHANNELS")

    text = '📑 **Indexed channels/groups**\n'
    for channel in channels:
        chat = await bot.get_chat(channel)
        if chat.username:
            text += '\n@' + chat.username
        else:
            text += '\n' + chat.title or chat.first_name

    text += f'\n\n**Total:** {len(CHANNELS)}'

    if len(text) < 4096:
        await message.reply(text)
    else:
        file = 'Indexed channels.txt'
        with open(file, 'w') as f:
            f.write(text)
        await message.reply_document(file)
        os.remove(file)


@Client.on_message(filters.command('logs') & filters.user(ADMINS))
async def log_file(bot, message):
    """Send log file"""
    try:
        await message.reply_document('TelegramBot.log')
    except Exception as e:
        await message.reply(str(e))

@Client.on_message(filters.command('delete') & filters.user(ADMINS))
async def delete(bot, message):
    """Delete file from database"""
    reply = message.reply_to_message
    if reply and reply.media:
        msg = await message.reply("Processing...⏳", quote=True)
    else:
        await message.reply('Reply to file with /delete which you want to delete', quote=True)
        return

    for file_type in ("document", "video", "audio"):
        media = getattr(reply, file_type, None)
        if media is not None:
            break
    else:
        await msg.edit('This is not supported file format')
        return
    
    file_id, file_ref = unpack_new_file_id(media.file_id)

    result = await Media.collection.delete_one({
        '_id': file_id,
    })
    if result.deleted_count:
        await msg.edit('File is successfully deleted from database')
    else:
        file_name = clean_file_name(media.file_name)
        result = await Media.collection.delete_many({
            'file_name': file_name,
            'file_size': media.file_size,
            'mime_type': media.mime_type
            })
        if result.deleted_count:
            await msg.edit('File is successfully deleted from database')
        else:
            # files indexed before https://github.com/GreyMattersbot/EvaMaria/commit/f3d2a1bcb155faf44178e5d7a685a1b533e714bf#diff-86b613edf1748372103e94cacff3b578b36b698ef9c16817bb98fe9ef22fb669R39 
            # have original file name.
            result = await Media.collection.delete_many({
                'file_name': media.file_name,
                'file_size': media.file_size,
                'mime_type': media.mime_type
            })
            if result.deleted_count:
                await msg.edit('File is successfully deleted from database')
            else:
                await msg.edit('File not found in database')


@Client.on_message(filters.command('deleteall') & filters.user(ADMINS))
async def delete_all_index(bot, message):
    await message.reply_text(
        'This will delete all indexed files.\nDo you want to continue??',
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        text="YES", callback_data="autofilter_delete"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="CANCEL", callback_data="close_data"
                    )
                ],
            ]
        ),
        quote=True,
    )


@Client.on_callback_query(filters.regex(r'^autofilter_delete'))
async def delete_all_index_confirm(bot, message):
    await Media.collection.drop()
    await message.answer('Piracy Is Crime')
    await message.message.edit('Succesfully Deleted All The Indexed Files.')


@Client.on_message(filters.command('delkeyword') & filters.user(ADMINS))
async def delete_keyword(bot, message):
    """Delete all files matching a keyword from the database (admin only).

    Usage: /delkeyword <keyword>
    """
    if len(message.command) < 2:
        await message.reply(
            "<b>Usage:</b> <code>/delkeyword &lt;keyword&gt;</code>\n"
            "Deletes all indexed files whose name or caption matches the keyword.",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    keyword = message.text.split(None, 1)[1].strip()
    if not keyword:
        await message.reply("Please provide a valid keyword.", quote=True)
        return

    msg = await message.reply(f"🔍 Searching for files matching <b>{keyword}</b>...", parse_mode=enums.ParseMode.HTML, quote=True)

    try:
        regex = re.compile(keyword, re.IGNORECASE)
    except re.error:
        await msg.edit("❌ Invalid regex pattern. Please use a plain keyword or valid regex.")
        return

    mongo_filter = {'$or': [{'file_name': regex}, {'caption': regex}]}

    total_deleted = 0
    try:
        result = await Media.collection.delete_many(mongo_filter)
        total_deleted += result.deleted_count
    except Exception as e:
        logger.error(f"Error deleting from primary DB by keyword: {e}")

    if Media2 is not None:
        try:
            result2 = await Media2.collection.delete_many(mongo_filter)
            total_deleted += result2.deleted_count
        except Exception as e:
            logger.warning(f"Error deleting from secondary DB by keyword: {e}")

    await msg.edit(
        f"✅ Deleted <b>{total_deleted}</b> file(s) matching <code>{keyword}</code> from the database.",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command('detectduplicates') & filters.user(ADMINS))
async def detect_and_remove_duplicates(bot, message):
    """Scan the database, detect content-equivalent duplicate files, and remove them.
    Keeps the most recently indexed copy of each duplicate group.
    """
    import time as _time
    start = _time.monotonic()
    msg = await message.reply(
        "🔍 Scanning database for duplicates... This may take a while.",
        quote=True, parse_mode=enums.ParseMode.HTML,
    )

    async def _clean_collection(collection, label: str):
        groups_found = 0
        docs_removed = 0
        
        try:
            total_docs = await collection.estimated_document_count()
        except Exception:
            total_docs = "Unknown"

        # Fetch in massive batches to minimize network latency
        cursor = collection.find({}, {'file_name': 1, 'file_size': 1}).batch_size(10000)
        dedup: dict = {}
        processed = 0
        async for doc in cursor:
            processed += 1
            if processed % 50000 == 0:
                try:
                    await msg.edit(f"🔍 Scanning {label} DB...\nProcessed: <code>{processed}</code> / <code>{total_docs}</code>", parse_mode=enums.ParseMode.HTML)
                except Exception:
                    pass

            raw_name = doc.get('file_name') or ''
            size = doc.get('file_size') or 0
            key = (normalize_for_dedup(raw_name), size)
            if not key[0]:
                continue
            dedup.setdefault(key, []).append(doc['_id'])

        try:
            await msg.edit(f"🗑 Consolidating duplicates in {label} DB...", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass

        to_delete_ids = []
        for key, ids in dedup.items():
            if len(ids) < 2:
                continue
            groups_found += 1
            ids_sorted = sorted(ids)
            to_delete_ids.extend(ids_sorted[:-1])  # keep newest

        # Delete in chunks to avoid slamming DB or hitting BSON size limits
        chunk_size = 5000
        for i in range(0, len(to_delete_ids), chunk_size):
            chunk = to_delete_ids[i:i + chunk_size]
            try:
                result = await collection.delete_many({'_id': {'$in': chunk}})
                docs_removed += result.deleted_count
            except Exception as e:
                logger.warning(f"Error removing duplicates chunk from {label}: {e}")

        return groups_found, docs_removed

    total_groups = 0
    total_removed = 0

    try:
        g, r = await _clean_collection(Media.collection, 'primary')
        total_groups += g
        total_removed += r
    except Exception as e:
        logger.error(f"detectduplicates: primary DB error: {e}")
        await msg.edit(f"❌ Error scanning primary database: <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    if Media2 is not None:
        try:
            g2, r2 = await _clean_collection(Media2.collection, 'secondary')
            total_groups += g2
            total_removed += r2
        except Exception as e:
            logger.warning(f"detectduplicates: secondary DB error: {e}")

    elapsed = _time.monotonic() - start
    await msg.edit(
        f"✅ <b>Duplicate scan complete</b> in <code>{elapsed:.1f}s</code>\n\n"
        f"📂 Duplicate groups found: <b>{total_groups}</b>\n"
        f"🗑 Files removed: <b>{total_removed}</b>\n"
        f"(Kept the most recently indexed copy of each group)",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.command('compact') & filters.user(ADMINS))
async def compact_cmd(bot, message):
    """Run the compact command on the database to reclaim free space."""
    msg = await message.reply("⏳ Running database compaction to reclaim disk space...\n<i>This might take a moment.</i>", quote=True)
    from database.ia_filterdb import compact_database, _db2
    results = await compact_database()
    
    response = "<b>Compaction Results:</b>\n"
    response += f"Primary DB: {'✅ Success' if results['primary'] else '❌ Failed'}\n"
    if results["secondary"] or _db2 is not None:
         response += f"Secondary DB: {'✅ Success' if results['secondary'] else '❌ Failed'}\n"
         
    if results["error"]:
        response += f"\n<b>Note/Error:</b> <code>{results['error']}</code>\n"
        response += "\n<i>MongoDB Atlas M0 (Free Tier) sometimes restricts the <code>compact</code> command. If this failed with an authorization error, the space will be reclaimed automatically over time, or you can drop and recreate the collection.</i>"
    else:
        response += "\n✅ Compaction complete. Check /stats to see updated disk usage."
        
    await msg.edit(response)


@Client.on_message(filters.command('settings'))
async def settings(client, message):
    userid = message.from_user.id if message.from_user else None
    if not userid:
        return await message.reply(f"You are anonymous admin. Use /connect {message.chat.id} in PM")
    chat_type = message.chat.type

    if chat_type == enums.ChatType.PRIVATE:
        grpid = await active_connection(str(userid))
        if grpid is not None:
            grp_id = grpid
            try:
                chat = await client.get_chat(grpid)
                title = chat.title
            except:
                await message.reply_text("Make sure I'm present in your group!!", quote=True)
                return
        else:
            await message.reply_text("I'm not connected to any groups!", quote=True)
            return

    elif chat_type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        grp_id = message.chat.id
        title = message.chat.title

    else:
        return

    st = await client.get_chat_member(grp_id, userid)
    if (
            st.status != enums.ChatMemberStatus.ADMINISTRATOR
            and st.status != enums.ChatMemberStatus.OWNER
            and str(userid) not in ADMINS
    ):
        return

    settings = await get_settings(grp_id)

    if settings is not None:
        buttons = [
            [
                InlineKeyboardButton(
                    'Filter Button',
                    callback_data=f'setgs#button#{settings["button"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    'Single' if settings["button"] else 'Double',
                    callback_data=f'setgs#button#{settings["button"]}#{grp_id}',
                ),
            ],
            [
                InlineKeyboardButton(
                    'Bot PM',
                    callback_data=f'setgs#botpm#{settings["botpm"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    '✅ Yes' if settings["botpm"] else '❌ No',
                    callback_data=f'setgs#botpm#{settings["botpm"]}#{grp_id}',
                ),
            ],
            [
                InlineKeyboardButton(
                    'File Secure',
                    callback_data=f'setgs#file_secure#{settings["file_secure"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    '✅ Yes' if settings["file_secure"] else '❌ No',
                    callback_data=f'setgs#file_secure#{settings["file_secure"]}#{grp_id}',
                ),
            ],
            [
                InlineKeyboardButton(
                    'IMDB',
                    callback_data=f'setgs#imdb#{settings["imdb"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    '✅ Yes' if settings["imdb"] else '❌ No',
                    callback_data=f'setgs#imdb#{settings["imdb"]}#{grp_id}',
                ),
            ],
            [
                InlineKeyboardButton(
                    'Spell Check',
                    callback_data=f'setgs#spell_check#{settings["spell_check"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    '✅ Yes' if settings["spell_check"] else '❌ No',
                    callback_data=f'setgs#spell_check#{settings["spell_check"]}#{grp_id}',
                ),
            ],
            [
                InlineKeyboardButton(
                    'Welcome',
                    callback_data=f'setgs#welcome#{settings["welcome"]}#{grp_id}',
                ),
                InlineKeyboardButton(
                    '✅ Yes' if settings["welcome"] else '❌ No',
                    callback_data=f'setgs#welcome#{settings["welcome"]}#{grp_id}',
                ),
            ],
        ]

        reply_markup = InlineKeyboardMarkup(buttons)

        await message.reply_text(
            text=f"<b>Change Your Settings for {title} As Your Wish ⚙</b>",
            reply_markup=reply_markup,
            disable_web_page_preview=True,
            parse_mode=enums.ParseMode.HTML,
            reply_to_message_id=message.id
        )



@Client.on_message(filters.command('set_template'))
async def save_template(client, message):
    sts = await message.reply("Checking template")
    userid = message.from_user.id if message.from_user else None
    if not userid:
        return await message.reply(f"You are anonymous admin. Use /connect {message.chat.id} in PM")
    chat_type = message.chat.type

    if chat_type == enums.ChatType.PRIVATE:
        grpid = await active_connection(str(userid))
        if grpid is not None:
            grp_id = grpid
            try:
                chat = await client.get_chat(grpid)
                title = chat.title
            except:
                await message.reply_text("Make sure I'm present in your group!!", quote=True)
                return
        else:
            await message.reply_text("I'm not connected to any groups!", quote=True)
            return

    elif chat_type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        grp_id = message.chat.id
        title = message.chat.title

    else:
        return

    st = await client.get_chat_member(grp_id, userid)
    if (
            st.status != enums.ChatMemberStatus.ADMINISTRATOR
            and st.status != enums.ChatMemberStatus.OWNER
            and str(userid) not in ADMINS
    ):
        return

    if len(message.command) < 2:
        return await sts.edit("No Input!!")
    template = message.text.split(" ", 1)[1]
    await save_group_settings(grp_id, 'template', template)
    await sts.edit(f"Successfully changed template for {title} to\n\n{template}")
