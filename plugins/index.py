import logging
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.errors.exceptions.bad_request_400 import ChannelInvalid, ChatAdminRequired, UsernameInvalid, UsernameNotModified
from info import ADMINS
from info import INDEX_REQ_CHANNEL as LOG_CHANNEL
from database.ia_filterdb import save_file, unpack_new_file_id
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from utils import temp
from movie_updates import publish_movie_update
import re
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
lock = asyncio.Lock()


@Client.on_callback_query(filters.regex(r'^index'))
async def index_files(bot, query):
    if query.data.startswith('index_cancel'):
        temp.CANCEL = True
        return await query.answer("ᴄᴀɴᴄᴇʟʟɪɴɢ ɪɴᴅᴇxɪɴɢ")
    _, raju, chat, lst_msg_id, from_user = query.data.split("#")
    if raju == 'reject':
        await query.message.delete()
        await bot.send_message(int(from_user),
                               f'Yᴏᴜʀ Sᴜʙᴍɪꜱꜱɪᴏɴ ғᴏʀ ɪɴᴅᴇxɪɴɢ {chat} ʜᴀꜱ ʙᴇᴇɴ ᴅᴇᴄʟɪᴇɴᴇᴅ ʙʏ ᴏᴜʀ ᴍᴏᴅᴇʀᴀᴛᴏʀꜱ.',
                               reply_to_message_id=int(lst_msg_id))
        return

    if lock.locked():
        return await query.answer('Wᴀɪᴛ ᴜɴᴛɪʟ ᴘʀᴇᴠɪᴏᴜꜱ ᴘʀᴏᴄᴇꜱꜱ ᴄᴏᴍᴘʟᴇᴛᴇ.', show_alert=True)
    msg = query.message

    await query.answer('Pʀᴏᴄᴇssɪɴɢ....⏳', show_alert=True)
    if int(from_user) not in ADMINS:
        await bot.send_message(int(from_user),
                               f'ʏᴏᴜʀ sᴜʙᴍɪssɪᴏɴ ғᴏʀ ɪɴᴅᴇxɪɴɢ {chat} ʜᴀs ʙᴇᴇɴ ᴀᴄᴄᴇᴘᴛᴇᴅ ʙʏ ᴏᴜʀ ᴀᴅᴍɪɴs ᴀɴᴅ ғɪʟᴇs ᴀᴅᴅᴇᴅ sᴏᴏɴ',
                               reply_to_message_id=int(lst_msg_id))
    await msg.edit(
        "Starting Indexing",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
        )
    )
    try:
        chat = int(chat)
    except:
        chat = chat
    await index_files_to_db(int(lst_msg_id), chat, msg, bot)


@Client.on_message((filters.forwarded | (filters.regex("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")) & filters.text ) & filters.private & filters.incoming)
async def send_for_index(bot, message):
    if message.text:
        regex = re.compile("(https://)?(t\.me/|telegram\.me/|telegram\.dog/)(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)$")
        match = regex.match(message.text)
        if not match:
            return await message.reply('ɪɴᴠᴀʟɪᴅ ʟɪɴᴋ')
        chat_id = match.group(4)
        last_msg_id = int(match.group(5))
        if chat_id.isnumeric():
            chat_id  = int(("-100" + chat_id))
    elif message.forward_from_chat.type == enums.ChatType.CHANNEL:
        last_msg_id = message.forward_from_message_id
        chat_id = message.forward_from_chat.username or message.forward_from_chat.id
    else:
        return
    try:
        await bot.get_chat(chat_id)
    except ChannelInvalid:
        return await message.reply('Tʜɪꜱ ᴍᴀʏ ʙᴇ ᴀ ᴘʀɪᴠᴀᴛᴇ ᴄʜᴀɴɴᴇʟ / ɢʀᴏᴜᴘ. Mᴀᴋᴇ ᴍᴇ ᴀɴ ᴀᴅᴍɪɴ ᴏᴠᴇʀ ᴛʜᴇʀᴇ ᴛᴏ ɪɴᴅᴇx ᴛʜᴇ ғɪʟᴇꜱ.')
    except (UsernameInvalid, UsernameNotModified):
        return await message.reply('ɪɴᴠᴀʟɪᴅ ʟɪɴᴋ sᴘᴇᴄɪғɪᴇᴅ.')
    except Exception as e:
        logger.exception(e)
        return await message.reply(f'Errors - {e}')
    try:
        k = await bot.get_messages(chat_id, last_msg_id)
    except:
        return await message.reply('ᴍᴀᴋᴇ sᴜʀᴇ ɪᴀᴍ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ ɪғ ᴛʜᴇ ᴄʜᴀɴɴᴇʟ ɪs ᴘʀɪᴠᴀᴛᴇ')
    if k.empty:
        return await message.reply('ɪᴀᴍ ɴᴏᴛ ᴀᴅᴍɪɴ ɪɴ ᴛʜɪs ɢʀɪᴜᴘ')

    if message.from_user.id in ADMINS:
        buttons = [
            [
                InlineKeyboardButton('Yes',
                                     callback_data=f'index#accept#{chat_id}#{last_msg_id}#{message.from_user.id}')
            ],
            [
                InlineKeyboardButton('close', callback_data='close_data'),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(buttons)
        return await message.reply(
            f'ᴅᴏ ʏᴏᴜ ᴡᴀɴᴛ ɪɴᴅᴇx ᴄʜᴀɴɴᴇʟ/ɢʀᴏᴜᴘ ?\n\nChat ID/ Username: <code>{chat_id}</code>\nLast Message ID: <code>{last_msg_id}</code>',
            reply_markup=reply_markup)

    if type(chat_id) is int:
        try:
            link = (await bot.create_chat_invite_link(chat_id)).invite_link
        except ChatAdminRequired:
            return await message.reply('ᴍᴀᴋᴇ sᴜʀᴇ ᴀᴅᴍɪɴ ɪɴ ᴛʜᴇ ᴡɪᴛʜ ᴡɪᴛʜ ɪɴᴠɪᴛᴇ ʟɪɴᴋ ᴘᴇʀᴍɪssɪᴏɴ')
    else:
        link = f"@{message.forward_from_chat.username}"
    buttons = [
        [
            InlineKeyboardButton('Accept Index',
                                 callback_data=f'index#accept#{chat_id}#{last_msg_id}#{message.from_user.id}')
        ],
        [
            InlineKeyboardButton('Reject Index',
                                 callback_data=f'index#reject#{chat_id}#{message.id}#{message.from_user.id}'),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await bot.send_message(LOG_CHANNEL,
                           f'#IndexRequest\n\nBy : {message.from_user.mention} (<code>{message.from_user.id}</code>)\nChat ID/ Username - <code> {chat_id}</code>\nLast Message ID - <code>{last_msg_id}</code>\nInviteLink - {link}',
                           reply_markup=reply_markup)
    await message.reply('ThankYou For the Contribution, Wait For My Moderators to verify the files.')


@Client.on_message(filters.command('setskip') & filters.user(ADMINS))
async def set_skip_number(bot, message):
    if ' ' in message.text:
        _, skip = message.text.split(" ")
        try:
            skip = int(skip)
        except:
            return await message.reply("sᴋɪᴘ ɴᴜᴍʙᴇʀ sʜᴏᴜʟᴅ ʙᴇ ᴀʜ ɪɴᴛᴇɢᴇʀ")
        await message.reply(f"sᴜᴄᴄᴇssғᴜʟʟʏ sᴇᴛ sᴋɪᴘ ɴᴜᴍʙᴇʀ ᴀs {skip}")
        temp.CURRENT = int(skip)
    else:
        await message.reply("ɢɪᴠᴇ ᴍᴇ ᴀ sᴋɪᴘ ɴᴜᴍʙᴇʀ")


async def index_files_to_db(lst_msg_id, chat, msg, bot):
    total_files = 0
    duplicate = 0
    smart_dup = 0
    errors = 0
    deleted = 0
    no_media = 0
    unsupported = 0
    async with lock:
        try:
            current = temp.CURRENT
            temp.CANCEL = False
            async for message in bot.iter_messages(chat, lst_msg_id, temp.CURRENT):
                if temp.CANCEL:
                    await msg.edit(
                        f"Successfully Cancelled!!\n\n"
                        f"Saved <code>{total_files}</code> files to dataBase!\n"
                        f"Duplicate Files Skipped: <code>{duplicate}</code>\n"
                        f"Smart Duplicates Skipped: <code>{smart_dup}</code>\n"
                        f"Deleted Messages Skipped: <code>{deleted}</code>\n"
                        f"Non-Media messages skipped: <code>{no_media + unsupported}</code>"
                        f"(Unsupported Media - `{unsupported}` )\n"
                        f"Errors Occurred: <code>{errors}</code>"
                    )
                    break
                current += 1
                if current % 20 == 0:
                    can = [[InlineKeyboardButton('Cancel', callback_data='index_cancel')]]
                    reply = InlineKeyboardMarkup(can)
                    await msg.edit_text(
                        text=(
                            f"Total messages fetched: <code>{current}</code>\n"
                            f"Total messages saved: <code>{total_files}</code>\n"
                            f"Duplicate Files Skipped: <code>{duplicate}</code>\n"
                            f"Smart Duplicates Skipped: <code>{smart_dup}</code>\n"
                            f"Deleted Messages Skipped: <code>{deleted}</code>\n"
                            f"Non-Media messages skipped: <code>{no_media + unsupported}</code>"
                            f"(Unsupported Media - `{unsupported}` )\n"
                            f"Errors Occurred: <code>{errors}</code>"
                        ),
                        reply_markup=reply,
                    )
                if message.empty:
                    deleted += 1
                    continue
                elif not message.media:
                    no_media += 1
                    continue
                elif message.media not in [enums.MessageMediaType.VIDEO, enums.MessageMediaType.AUDIO, enums.MessageMediaType.DOCUMENT]:
                    unsupported += 1
                    continue
                media = getattr(message, message.media.value, None)
                if not media:
                    unsupported += 1
                    continue
                media.file_type = message.media.value
                media.caption = message.caption
                aynav, vnay = await save_file(media)
                if aynav:
                    total_files += 1
                    file_id, _ = unpack_new_file_id(media.file_id)
                    await publish_movie_update(bot, media=media, file_id=file_id)
                elif vnay == 0:
                    duplicate += 1
                elif vnay == 2:
                    errors += 1
                elif vnay == 3:
                    smart_dup += 1
        except Exception as e:
            logger.exception(e)
            await msg.edit(f'Error: {e}')
        else:
            await msg.edit(
                f'Succesfully saved <code>{total_files}</code> to dataBase!\n'
                f'Duplicate Files Skipped: <code>{duplicate}</code>\n'
                f'Smart Duplicates Skipped: <code>{smart_dup}</code>\n'
                f'Deleted Messages Skipped: <code>{deleted}</code>\n'
                f'Non-Media messages skipped: <code>{no_media + unsupported}</code>'
                f'(Unsupported Media - `{unsupported}` )\n'
                f'Errors Occurred: <code>{errors}</code>'
            )
