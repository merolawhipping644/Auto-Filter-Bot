import asyncio
import re
import ast
import math
from html import escape
from typing import Dict, List, Optional, Tuple

from pyrogram.errors.exceptions.bad_request_400 import MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty
from Script import script
import pyrogram
from database.connections_mdb import active_connection, all_connections, delete_connection, if_active, make_active, \
    make_inactive
from info import *
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserIsBlocked, MessageNotModified, PeerIdInvalid, ChannelPrivate, QueryIdInvalid
from utils import (
    get_size,
    is_subscribed,
    get_poster,
    search_gagala,
    temp,
    get_settings,
    save_group_settings,
    send_document_with_anonymous_filename,
)
from sanitizers import clean_caption
from database.users_chats_db import db
from database.ia_filterdb import Media, get_file_details, get_search_results
from database.filters_mdb import (
    del_all,
    find_filter,
    get_filters,
)
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

BUTTONS: Dict[str, str] = {}
BUTTON_LANG_SELECTION: Dict[str, Optional[str]] = {}
SPELL_CHECK: Dict[int, list] = {}
FILTER_MODE: Dict[str, str] = {}
SEND_ALL_PAGE_CACHE: Dict[str, Dict[int, List[Media]]] = {}


def _reset_page_cache(key: str) -> None:
    SEND_ALL_PAGE_CACHE[key] = {}


def _store_page_results(key: str, offset: int, files: List[Media]) -> None:
    page_cache = SEND_ALL_PAGE_CACHE.setdefault(key, {})
    page_cache[offset] = list(files)

LANGUAGE_FILTER_OPTIONS: Tuple[Tuple[str, Optional[str]], ...] = (
    ("All", None),
    ("English", "english"),
    ("Hindi", "hindi"),
    ("Tamil", "tamil"),
    ("Telugu", "telugu"),
    ("Malayalam", "malayalam"),
    ("Kannada", "kannada"),
)

MOVIE_UI_EMOJIS = (
    "🎬",
    "🍿",
    "🎞️",
    "📽️",
    "🎟️",
    "✨",
    "🌟",
    "💫",
    "🔥",
    "🪄",
)

def _compose_language_query(search: str, language: Optional[str]) -> str:
    """Return a query string constrained by the selected language."""

    search = (search or "").strip()
    if language:
        return f"{search} {language}".strip()
    return search


def _build_language_buttons(
    req: int,
    key: str,
    selected: Optional[str],
) -> list[list[InlineKeyboardButton]]:
    """Create inline buttons that allow users to filter by language."""

    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []

    for label, language in LANGUAGE_FILTER_OPTIONS:
        callback_token = language or "all"
        is_selected = (language is None and not selected) or (language is not None and selected == language)
        button_label = f"✅ {label}" if is_selected else label
        current_row.append(
            InlineKeyboardButton(
                button_label,
                callback_data=f"lang_{req}_{key}_{callback_token}",
            )
        )
        if len(current_row) == 3:
            rows.append(current_row)
            current_row = []

    if current_row:
        rows.append(current_row)

    return rows


def _reaction_emojis(seed: str) -> tuple[str, str]:
    """Return two visually distinct emojis based on a text seed."""

    if not seed:
        return MOVIE_UI_EMOJIS[0], MOVIE_UI_EMOJIS[5]

    hashed = sum(ord(char) for char in seed)
    first = MOVIE_UI_EMOJIS[hashed % len(MOVIE_UI_EMOJIS)]
    second = MOVIE_UI_EMOJIS[(hashed + 3) % len(MOVIE_UI_EMOJIS)]
    if second == first:
        second = MOVIE_UI_EMOJIS[(hashed + 6) % len(MOVIE_UI_EMOJIS)]
    return first, second


def _build_reaction_banner(title: str) -> str:
    """Create a styled banner that reacts to a movie request."""

    first, second = _reaction_emojis(title)
    clean_title = escape(title.strip()) if title else "Unknown"
    return f"{first}✨ <b>Movie Request:</b> <code>{clean_title}</code> {second}"


def _decorate_caption(title: str, body: str) -> str:
    """Attach a decorative banner to the top of a caption body."""

    banner = _build_reaction_banner(title)
    body = body.strip()
    return f"{banner}\n\n{body}" if body else banner


def _format_not_found_message(query: str) -> str:
    """Return a friendly styled message when a movie search fails."""

    first, second = _reaction_emojis(query)
    clean_query = escape(query.strip()) if query else "that title"
    return (
        f"{first} <b>Oops! I couldn't find “{clean_query}”.</b> {second}\n"
        "🔎 Try a different spelling or share more details so I can hunt it down! 🍿"
    )


def _format_spellcheck_prompt() -> str:
    """Generate the prompt shown alongside spell-check suggestions."""

    return (
        "🧐✨ <b>I couldn't find a perfect match.</b>\n"
        "💡 Did you mean any of these cinematic gems?"
    )


def _format_unavailable_message(title: str) -> str:
    """Return a styled notice when a movie is absent from the database."""

    body = (
        "🚧 I haven't added this one to my vault just yet.\n"
        "🗂️ Check back soon or try another title in the meantime!"
    )
    return _decorate_caption(title, body)


def _format_fallback_caption(search: str) -> str:
    """Create a friendly caption when IMDb details are unavailable."""

    body = (
        "🍿 <b>Here is your files!</b>\n"
        "🎟️ Join @BrUceDoesNotExiSt for more cinematic adventures!"
    )
    return _decorate_caption(search, body)


def _prepare_file_caption(file, settings) -> str:
    title = file.file_name
    size = get_size(file.file_size)
    caption = file.caption

    if CUSTOM_FILE_CAPTION:
        try:
            caption = CUSTOM_FILE_CAPTION.format(
                file_name="" if title is None else title,
                file_size="" if size is None else size,
                file_caption="" if caption is None else caption,
            )
        except Exception as exc:  # noqa: BLE001 - log but fallback to original caption
            logger.exception("Failed to format custom caption: %%s", exc)

    if not caption:
        caption = title or ""

    return clean_caption(caption)

@Client.on_message(filters.command('autofilter'))
async def fil_mod(client, message): 
      mode_on = ["yes", "on", "true"]
      mode_of = ["no", "off", "false"]

      try: 
         args = message.text.split(None, 1)[1].lower() 
      except: 
         return await message.reply("**𝙸𝙽𝙲𝙾𝙼𝙿𝙻𝙴𝚃𝙴 𝙲𝙾𝙼𝙼𝙰𝙽𝙳...**")
      
      m = await message.reply("**𝚂𝙴𝚃𝚃𝙸𝙽𝙶.../**")

      if args in mode_on:
          FILTER_MODE[str(message.chat.id)] = "True" 
          await m.edit("**𝙰𝚄𝚃𝙾𝙵𝙸𝙻𝚃𝙴𝚁 𝙴𝙽𝙰𝙱𝙻𝙴𝙳**")
      
      elif args in mode_of:
          FILTER_MODE[str(message.chat.id)] = "False"
          await m.edit("**𝙰𝚄𝚃𝙾𝙵𝙸𝙻𝚃𝙴𝚁 𝙳𝙸𝚂𝙰𝙱𝙻𝙴𝙳**")
      else:
          await m.edit("USE :- /autofilter on 𝙾𝚁 /autofilter off")

@Client.on_message((filters.group | filters.private) & filters.text & filters.incoming)
async def give_filter(client, message):
    k = await manual_filters(client, message)
    if k == False:
        await auto_filter(client, message)


@Client.on_callback_query(filters.regex(r"^next"))
async def next_page(bot, query):
    ident, req, key, offset_token = query.data.split("_", 3)
    if int(req) not in [query.from_user.id, 0]:
        return await query.answer(
            "🍿 Please tap your own buttons to enjoy the show!",
            show_alert=True,
        )
    try:
        current_offset = int(offset_token)
    except (TypeError, ValueError):
        current_offset = 0
    search = BUTTONS.get(key)
    if not search:
        await query.answer(
            "🕰️ That menu expired. Send a fresh request for the latest reels!",
            show_alert=True,
        )
        return

    selected_language = BUTTON_LANG_SELECTION.get(key)
    search_query = _compose_language_query(search, selected_language).lower()

    files, n_offset, total = await get_search_results(search_query, offset=current_offset, filter=True)
    try:
        n_offset = int(n_offset)
    except:
        n_offset = 0

    if not files:
        return
    _store_page_results(key, current_offset, files)
    settings = await get_settings(query.message.chat.id)
    pre = 'filep' if settings.get('file_secure') else 'file'
    if settings['button']:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"[{get_size(file.file_size)}]-✨-{file.file_name}", callback_data=f'{pre}#{file.file_id}'
                ),
            ]
            for file in files
        ]
    else:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"{file.file_name}", callback_data=f'{pre}#{file.file_id}'
                ),
                InlineKeyboardButton(
                    text=f"{get_size(file.file_size)}",
                    callback_data=f'{pre}#{file.file_id}',
                ),
            ]
            for file in files
        ]

    if 0 < current_offset <= 10:
        off_set = 0
    elif current_offset == 0:
        off_set = None
    else:
        off_set = current_offset - 10

    try:
        total_results = int(total)
    except (TypeError, ValueError):
        total_results = current_offset + len(files)

    current_page = (current_offset // 10) + 1
    total_pages = max(((total_results + 9) // 10), current_page, 1)
    page_label = f"🗓 {current_page} / {total_pages}"
    summary_label = f"📃 𝗣𝗮𝗴𝗲s {current_page} / {total_pages}"

    language_rows = _build_language_buttons(int(req), key, selected_language)
    if language_rows:
        btn.extend(language_rows)

    # Discovery buttons
    discover_row = [
        InlineKeyboardButton("⭐ Add Watchlist", callback_data=f"add_watchlist#{search}"),
    ]
    # We don't easily have the IMDb URL here unless we store it, but we can still show the Trailer button
    yt_query = search.replace(" ", "+")
    discover_row.append(InlineKeyboardButton("📹 Trailer", url=f"https://www.youtube.com/results?search_query={yt_query}+trailer"))
    btn.append(discover_row)

    btn.append([
        InlineKeyboardButton(
            "📨 Send All",
            callback_data=f"sendall_{req}_{key}_{current_offset}",
        )
    ])
    if n_offset == 0:
        row = []
        if off_set is not None:
            row.append(InlineKeyboardButton("⏪ 𝗕𝗮𝗰𝗸", callback_data=f"next_{req}_{key}_{off_set}"))
        row.append(InlineKeyboardButton(summary_label, callback_data="pages"))
        btn.append(row)
    elif off_set is None:
        btn.append(
            [InlineKeyboardButton(page_label, callback_data="pages"),
             InlineKeyboardButton("𝗡𝗲𝘅𝘁 ➡️", callback_data=f"next_{req}_{key}_{n_offset}")])
    else:
        btn.append(
            [
                InlineKeyboardButton("⏪ 𝗕𝗮𝗰𝗸", callback_data=f"next_{req}_{key}_{off_set}"),
                InlineKeyboardButton(page_label, callback_data="pages"),
                InlineKeyboardButton("𝗡𝗲𝘅𝘁 ➡️", callback_data=f"next_{req}_{key}_{n_offset}")
            ],
        )
    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(btn)
        )
    except MessageNotModified:
        pass
    await query.answer()


@Client.on_callback_query(filters.regex(r"^lang_"))
async def change_language(bot, query: CallbackQuery):
    _, req, key, language_code = query.data.split("_", 3)

    try:
        req_user = int(req)
    except ValueError:
        req_user = 0

    if req_user not in [query.from_user.id, 0]:
        return await query.answer(
            "🍿 Please tap your own buttons to enjoy the show!",
            show_alert=True,
        )

    req_token = str(req_user)

    search = BUTTONS.get(key)
    if not search:
        await query.answer(
            "🕰️ That menu expired. Send a fresh request for the latest reels!",
            show_alert=True,
        )
        return

    previous_language = BUTTON_LANG_SELECTION.get(key)
    new_language = None if language_code == "all" else language_code
    BUTTON_LANG_SELECTION[key] = new_language

    search_query = _compose_language_query(search, new_language).lower()
    files, offset, total_results = await get_search_results(search_query, offset=0, filter=True)

    if not files:
        BUTTON_LANG_SELECTION[key] = previous_language
        await query.answer("No results found for this language.", show_alert=True)
        return

    _reset_page_cache(key)
    _store_page_results(key, 0, files)

    settings = await get_settings(query.message.chat.id)
    pre = 'filep' if settings['file_secure'] else 'file'

    if settings["button"]:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"[{get_size(file.file_size)}]-✨-{file.file_name}",
                    callback_data=f'{pre}#{file.file_id}'
                ),
            ]
            for file in files
        ]
    else:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"{file.file_name}",
                    callback_data=f'{pre}#{file.file_id}',
                ),
                InlineKeyboardButton(
                    text=f"{get_size(file.file_size)}",
                    callback_data=f'{pre}#{file.file_id}',
                ),
            ]
            for file in files
        ]

    language_rows = _build_language_buttons(req_user, key, new_language)
    if language_rows:
        btn.extend(language_rows)

    # Discovery buttons
    discover_row = [
        InlineKeyboardButton("⭐ Add Watchlist", callback_data=f"add_watchlist#{search}"),
    ]
    yt_query = search.replace(" ", "+")
    discover_row.append(InlineKeyboardButton("📹 Trailer", url=f"https://www.youtube.com/results?search_query={yt_query}+trailer"))
    btn.append(discover_row)

    btn.append([
        InlineKeyboardButton(
            "📨 Send All",
            callback_data=f"sendall_{req_token}_{key}_0",
        )
    ])

    if offset != "":
        btn.append(
            [
                InlineKeyboardButton(
                    text=f"🗓 1/{math.ceil(int(total_results) / 10)}",
                    callback_data="pages",
                ),
                InlineKeyboardButton(
                    text="𝗡𝗲𝘅𝘁 ⏩",
                    callback_data=f"next_{req_token}_{key}_{offset}",
                ),
            ]
        )
    else:
        btn.append([
            InlineKeyboardButton(text="🗓 1/1", callback_data="pages")
        ])

    try:
        await query.edit_message_reply_markup(
            reply_markup=InlineKeyboardMarkup(btn)
        )
    except MessageNotModified:
        pass

    await query.answer("Language filter updated.")


@Client.on_callback_query(filters.regex(r"^spolling"))
async def advantage_spoll_choker(bot, query):
    _, user, movie_ = query.data.split('#')
    if int(user) != 0 and query.from_user.id != int(user):
        return await query.answer("😁 𝗛𝗲𝘆 𝗙𝗿𝗶𝗲𝗻𝗱,𝗣𝗹𝗲𝗮𝘀𝗲 𝗦𝗲𝗮𝗿𝗰𝗵 𝗬𝗼𝘂𝗿𝘀𝗲𝗹𝗳.", show_alert=True)
    if movie_ == "close_spellcheck":
        return await query.message.delete()
    movies = SPELL_CHECK.get(query.message.reply_to_message.id)
    if not movies:
        return await query.answer("𝐋𝐢𝐧𝐤 𝐄𝐱𝐩𝐢𝐫𝐞𝐝 𝐊𝐢𝐧𝐝𝐥𝐲 𝐏𝐥𝐞𝐚𝐬𝐞 𝐒𝐞𝐚𝐫𝐜𝐡 𝐀𝐠𝐚𝐢𝐧 🙂.", show_alert=True)
    movie = movies[(int(movie_))]
    await query.answer("🎬✨ Rolling through the reels for you...")
    k = await manual_filters(bot, query.message, text=movie)
    if k == False:
        files, offset, total_results = await get_search_results(movie, offset=0, filter=True)
        if files:
            k = (movie, files, offset, total_results)
            await auto_filter(bot, query, k)
        else:
            k = await query.message.edit(_format_unavailable_message(movie))
            await asyncio.sleep(10)
            await k.delete()


@Client.on_callback_query()
async def cb_handler(client: Client, query: CallbackQuery):
    if query.data.startswith(("browse_", "add_watchlist#", "rem_watchlist#", "spolling#")):
        return
    if query.data == "close_data":
        await query.message.delete()
    elif query.data == "delallconfirm":
        userid = query.from_user.id
        chat_type = query.message.chat.type

        if chat_type == enums.ChatType.PRIVATE:
            grpid = await active_connection(str(userid))
            if grpid is not None:
                grp_id = grpid
                try:
                    chat = await client.get_chat(grpid)
                    title = chat.title
                except:
                    await query.message.edit_text("Make sure I'm present in your group!!", quote=True)
                    return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
            else:
                await query.message.edit_text(
                    "I'm not connected to any groups!\nCheck /connections or connect to any groups",
                    quote=True
                )
                return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')

        elif chat_type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
            grp_id = query.message.chat.id
            title = query.message.chat.title

        else:
            return await query.answer('Piracy Is Crime')

        st = await client.get_chat_member(grp_id, userid)
        if (st.status == enums.ChatMemberStatus.OWNER) or (str(userid) in ADMINS):
            await del_all(query.message, grp_id, title)
        else:
            await query.answer("You need to be Group Owner or an Auth User to do that!", show_alert=True)
    elif query.data == "delallcancel":
        userid = query.from_user.id
        chat_type = query.message.chat.type

        if chat_type == enums.ChatType.PRIVATE:
            await query.message.reply_to_message.delete()
            await query.message.delete()

        elif chat_type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
            grp_id = query.message.chat.id
            st = await client.get_chat_member(grp_id, userid)
            if (st.status == enums.ChatMemberStatus.OWNER) or (str(userid) in ADMINS):
                await query.message.delete()
                try:
                    await query.message.reply_to_message.delete()
                except:
                    pass
            else:
                await query.answer("Buddy Don't Touch Others Property 😁", show_alert=True)
    elif "groupcb" in query.data:
        await query.answer()

        group_id = query.data.split(":")[1]
        act = query.data.split(":")[2]
        user_id = query.from_user.id

        try:
            hr = await client.get_chat(int(group_id))
            title = hr.title
        except ChannelPrivate:
            await query.message.edit_text(
                "⚠️ This group is no longer accessible (private or bot removed). "
                "Please delete this connection.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Delete", callback_data=f"deletecb:{group_id}")],
                    [InlineKeyboardButton("𝙱𝙰𝙲𝙺", callback_data="backcb")]
                ])
            )
            return
        except Exception:
            await query.message.edit_text("❌ Failed to fetch group info. Please try again.")
            return

        if act == "":
            stat = "𝙲𝙾𝙽𝙽𝙴𝙲𝚃"
            cb = "connectcb"
        else:
            stat = "𝙳𝙸𝚂𝙲𝙾𝙽𝙽𝙴𝙲𝚃"
            cb = "disconnect"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{stat}", callback_data=f"{cb}:{group_id}"),
             InlineKeyboardButton("𝙳𝙴𝙻𝙴𝚃𝙴", callback_data=f"deletecb:{group_id}")],
            [InlineKeyboardButton("𝙱𝙰𝙲𝙺", callback_data="backcb")]
        ])

        await query.message.edit_text(
            f"𝙶𝚁𝙾𝚄𝙿 𝙽𝙰𝙼𝙴 :- **{title}**\n𝙶𝚁𝙾𝚄𝙿 𝙸𝙳 :- `{group_id}`",
            reply_markup=keyboard,
            parse_mode=enums.ParseMode.MARKDOWN
        )
        return await query.answer('Piracy Is Crime')
    elif "connectcb" in query.data:
        await query.answer()

        group_id = query.data.split(":")[1]
        user_id = query.from_user.id

        try:
            hr = await client.get_chat(int(group_id))
            title = hr.title
        except ChannelPrivate:
            await query.message.edit_text(
                "⚠️ Cannot connect — this group is private or the bot was removed.",
                parse_mode=enums.ParseMode.MARKDOWN
            )
            return
        except Exception:
            await query.message.edit_text("❌ Failed to fetch group info.", parse_mode=enums.ParseMode.MARKDOWN)
            return

        mkact = await make_active(str(user_id), str(group_id))

        if mkact:
            await query.message.edit_text(
                f"𝙲𝙾𝙽𝙽𝙴𝙲𝚃𝙴𝙳 𝚃𝙾 **{title}**",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await query.message.edit_text('Some error occurred!!', parse_mode=enums.ParseMode.MARKDOWN)
        return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
    elif "disconnect" in query.data:
        await query.answer()

        group_id = query.data.split(":")[1]
        user_id = query.from_user.id

        try:
            hr = await client.get_chat(int(group_id))
            title = hr.title
        except (ChannelPrivate, Exception):
            title = f"group `{group_id}`"

        mkinact = await make_inactive(str(user_id))

        if mkinact:
            await query.message.edit_text(
                f"𝙳𝙸𝚂𝙲𝙾𝙽𝙽𝙴𝙲𝚃 FROM **{title}**",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await query.message.edit_text(
                f"Some error occurred!!",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
    elif "deletecb" in query.data:
        await query.answer()

        user_id = query.from_user.id
        group_id = query.data.split(":")[1]

        delcon = await delete_connection(str(user_id), str(group_id))

        if delcon:
            await query.message.edit_text(
                "Successfully deleted connection"
            )
        else:
            await query.message.edit_text(
                f"Some error occurred!!",
                parse_mode=enums.ParseMode.MARKDOWN
            )
        return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
    elif query.data == "backcb":
        await query.answer()

        userid = query.from_user.id

        groupids = await all_connections(str(userid))
        if groupids is None:
            await query.message.edit_text(
                "There are no active connections!! Connect to some groups first.",
            )
            return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
        buttons = []
        for groupid in groupids:
            try:
                ttl = await client.get_chat(int(groupid))
                title = ttl.title
                active = await if_active(str(userid), str(groupid))
                act = " - ACTIVE" if active else ""
                buttons.append(
                    [
                        InlineKeyboardButton(
                            text=f"{title}{act}", callback_data=f"groupcb:{groupid}:{act}"
                        )
                    ]
                )
            except:
                pass
        if buttons:
            await query.message.edit_text(
                "Your connected group details ;\n\n",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
    elif "alertmessage" in query.data:
        grp_id = query.message.chat.id
        i = query.data.split(":")[1]
        keyword = query.data.split(":")[2]
        reply_text, btn, alerts, fileid = await find_filter(grp_id, keyword)
        if alerts is not None:
            alerts = ast.literal_eval(alerts)
            alert = alerts[int(i)]
            alert = alert.replace("\\n", "\n").replace("\\t", "\t")
            await query.answer(alert, show_alert=True)
    if query.data.startswith("file"):
        ident, file_id = query.data.split("#")
        files_ = await get_file_details(file_id)
        if not files_:
            try:
                await query.answer('No such file exist.', show_alert=True)
            except Exception:
                pass
            return
        files = files_[0]
        settings = await get_settings(query.message.chat.id)
        f_caption = _prepare_file_caption(files, settings)

        # Determine whether the user needs to subscribe
        needs_sub = AUTH_CHANNEL and not await is_subscribed(client, query)

        try:
            if needs_sub:
                # Show join-channel prompt; if query expired just send a PM message
                try:
                    await query.answer(url=f"https://t.me/{temp.U_NAME}?start={ident}_{file_id}")
                except QueryIdInvalid:
                    await client.send_message(
                        query.from_user.id,
                        "⚠️ Please subscribe to our channel first, then tap the file button again."
                    )
                return

            elif settings['botpm']:
                # Redirect to /start; if query expired fall through and send directly
                try:
                    await query.answer(url=f"https://t.me/{temp.U_NAME}?start={ident}_{file_id}")
                    return
                except QueryIdInvalid:
                    pass  # query too old — fall through to send directly below

            # Send file directly (botpm=False OR query expired)
            btn = []
            if ENABLE_STREAM_LINK:
                btn.append([InlineKeyboardButton("⚡ Fast Download Link", callback_data=f"fastdl#{file_id}")])
            if ENABLE_GOFILE_LINK:
                btn.append([InlineKeyboardButton("📤 GoFile Upload", callback_data=f"gofileup#{file_id}")])
            reply_markup = InlineKeyboardMarkup(btn) if btn else None
            await send_document_with_anonymous_filename(
                client,
                chat_id=query.from_user.id,
                media=files,
                caption=f_caption,
                protect_content=ident == "filep",
                reply_markup=reply_markup,
            )
            try:
                await query.answer('Check PM, I have sent files in pm', show_alert=True)
            except Exception:
                pass

        except UserIsBlocked:
            try:
                await query.answer('You Are Blocked to use me !', show_alert=True)
            except Exception:
                pass
        except PeerIdInvalid:
            bot_link = f"https://t.me/{temp.U_NAME}?start={ident}_{file_id}"
            answered = False
            try:
                await query.answer(url=bot_link)
                answered = True
            except Exception:
                pass
            if not answered:
                # Query expired before we could redirect — send a PM link directly
                try:
                    await client.send_message(
                        query.from_user.id,
                        f"👋 Please start the bot in PM first, then use this link to get your file:\n{bot_link}",
                    )
                except Exception:
                    pass
        except Exception as e:
            logger.exception("Failed to send file to user %s: %s", query.from_user.id, e)
            try:
                await query.answer('❌ Failed to send file. Please try again or start the bot in PM.', show_alert=True)
            except Exception:
                pass
    elif query.data.startswith("sendall_"):
        _, req, key, offset_token = query.data.split("_", 3)
        try:
            req_user = int(req)
        except ValueError:
            req_user = 0
        if req_user not in [query.from_user.id, 0]:
            return await query.answer("oKda", show_alert=True)

        try:
            offset_value = int(offset_token)
        except ValueError:
            offset_value = 0

        search = BUTTONS.get(key)
        if not search:
            await query.answer("Search expired. Please search again.", show_alert=True)
            return

        settings = await get_settings(query.message.chat.id)
        selected_language = BUTTON_LANG_SELECTION.get(key)
        search_query = _compose_language_query(search, selected_language).lower()
        if AUTH_CHANNEL and not await is_subscribed(client, query):
            await query.answer("Join the updates channel and try again.", show_alert=True)
            return

        await query.answer("Preparing files…")

        cached_pages = SEND_ALL_PAGE_CACHE.get(key, {})
        page_files = cached_pages.get(offset_value)

        if page_files is None:
            page_files, _, _ = await get_search_results(
                search_query,
                offset=offset_value,
                filter=True,
            )
            if not page_files:
                await query.message.reply_text("No files found to send.")
                return
            _store_page_results(key, offset_value, page_files)

        files_to_process = list(page_files)

        sent = 0
        failed = 0

        for media in files_to_process:
            caption_text = _prepare_file_caption(media, settings)
            try:
                await send_document_with_anonymous_filename(
                    client,
                    chat_id=query.from_user.id,
                    media=media,
                    caption=caption_text,
                    protect_content=settings['file_secure'],
                )
                sent += 1
                await asyncio.sleep(0.2)
            except FloodWait as fw:
                await asyncio.sleep(fw.value)
                try:
                    await send_document_with_anonymous_filename(
                        client,
                        chat_id=query.from_user.id,
                        media=media,
                        caption=caption_text,
                        protect_content=settings['file_secure'],
                    )
                    sent += 1
                except Exception as exc:  # noqa: BLE001 - log and continue
                    logger.exception("Failed to send media after FloodWait: %s", exc)
                    failed += 1
            except UserIsBlocked:
                await query.message.reply_text("You have blocked the bot. Please unblock me to receive files.")
                return
            except PeerIdInvalid:
                link = f"https://t.me/{temp.U_NAME}" if temp.U_NAME else None
                prompt = "Please start the bot in PM and try again."
                if link:
                    prompt += f"\n👉 {link}"
                await query.message.reply_text(prompt)
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to send media in send-all flow: %s", exc)
                failed += 1

        summary_lines = [f"✅ Sent {sent} file(s) for '{search}' from the current page."]
        if failed:
            summary_lines.append(f"⚠️ Failed to deliver {failed} file(s).")

        summary_text = "\n".join(summary_lines)
        try:
            await client.send_message(query.from_user.id, summary_text)
        except Exception:
            await query.message.reply_text(summary_text)

    elif query.data.startswith("checksub"):
        if AUTH_CHANNEL and not await is_subscribed(client, query):
            try:
                await query.answer("I Like Your Smartness, But Don't Be Oversmart Okay 😒", show_alert=True)
            except Exception:
                pass
            return
        ident, file_id = query.data.split("#")
        files_ = await get_file_details(file_id)
        if not files_:
            try:
                await query.answer('No such file exist.', show_alert=True)
            except Exception:
                pass
            return
        files = files_[0]
        f_caption = _prepare_file_caption(files, await get_settings(query.message.chat.id))
        try:
            await send_document_with_anonymous_filename(
                client,
                chat_id=query.from_user.id,
                media=files,
                caption=f_caption,
                protect_content=ident == 'checksubp',
            )
            try:
                await query.answer('Check PM, I have sent the file!', show_alert=True)
            except Exception:
                pass
        except UserIsBlocked:
            try:
                await query.answer('You have blocked me. Please unblock the bot first!', show_alert=True)
            except Exception:
                pass
        except PeerIdInvalid:
            try:
                await query.answer(url=f"https://t.me/{temp.U_NAME}?start={ident}_{file_id}")
            except Exception:
                pass
        except Exception as e:
            logger.exception("checksub: failed to send file to user %s: %s", query.from_user.id, e)
            try:
                await query.answer('❌ Failed to send file. Please try again.', show_alert=True)
            except Exception:
                pass
    elif query.data == "pages":
        await query.answer()
    elif query.data == "start":
        buttons = [[
            InlineKeyboardButton('⚚ ΛᎠᎠ MΞ ϮԾ YԾUᏒ GᏒԾUᎮ ⚚', url=f'http://t.me/{temp.U_NAME}?startgroup=true')
        ], [
            InlineKeyboardButton('⚡ SUBSCᏒIBΞ ⚡', url='https://t.me/BrUceDoesNotExiSt'),
            InlineKeyboardButton('🤖 UᎮDΛTΞS 🤖', url=f'{script.HOME_BUTTONURL_UPDATES}')
        ], [
            InlineKeyboardButton('♻️ HΞLᎮ ♻️', callback_data='help'),
            InlineKeyboardButton('♻️ ΛBOUT ♻️', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.START_TXT.format(query.from_user.mention, temp.U_NAME, temp.B_NAME),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
        await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')
    elif query.data == "help":
        buttons = [[
            InlineKeyboardButton('𝙼𝙰𝙽𝚄𝙴𝙻 𝙵𝙸𝙻𝚃𝙴𝚁', callback_data='manuelfilter'),
            InlineKeyboardButton('𝙰𝚄𝚃𝙾 𝙵𝙸𝙻𝚃𝙴𝚁', callback_data='autofilter')
        ], [
            InlineKeyboardButton('𝙲𝙾𝙽𝙽𝙴𝙲𝚃𝙸𝙾𝙽𝚂', callback_data='coct'),
            InlineKeyboardButton('𝙴𝚇𝚃𝚁𝙰 𝙼𝙾D𝚂', callback_data='extra')
        ], [
            InlineKeyboardButton('🏠 H𝙾𝙼𝙴 🏠', callback_data='start'),
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.HELP_TXT.format(query.from_user.mention),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "about":
        buttons = [[
            InlineKeyboardButton('🏠 H𝙾𝙼𝙴 🏠', callback_data='start'),
         ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.ABOUT_TXT.format(temp.B_NAME),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "source":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='about')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.SOURCE_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "manuelfilter":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help'),
            InlineKeyboardButton('⏹️ 𝙱𝚄𝚃𝚃𝙾𝙽𝚂', callback_data='button')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.MANUELFILTER_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "button":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='manuelfilter')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.BUTTON_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "autofilter":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.AUTOFILTER_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "coct":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.CONNECTION_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "extra":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help'),
            InlineKeyboardButton('👮‍♂️ 𝙰𝙳𝙼𝙸𝙽', callback_data='admin')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.EXTRAMOD_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "admin":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='extra')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.message.edit_text(
            text=script.ADMIN_TXT,
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "stats":
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help'),
            InlineKeyboardButton('♻️ 𝚁𝙴𝙵𝚁𝙴𝚂𝙷', callback_data='rfrsh')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        total = await Media.count_documents()
        users = await db.total_users_count()
        chats = await db.total_chat_count()
        monsize = await db.get_db_size()
        free = 536870912 - monsize
        monsize = get_size(monsize)
        free = get_size(free)
        await query.message.edit_text(
            text=script.STATUS_TXT.format(total, users, chats, monsize, free),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data == "rfrsh":
        await query.answer("Fetching MongoDb DataBase")
        buttons = [[
            InlineKeyboardButton('👩‍🦯 𝙱𝙰𝙲𝙺', callback_data='help'),
            InlineKeyboardButton('♻️ 𝚁𝙴𝙵𝚁𝙴𝚂𝙷', callback_data='rfrsh')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        total = await Media.count_documents()
        users = await db.total_users_count()
        chats = await db.total_chat_count()
        monsize = await db.get_db_size()
        free = 536870912 - monsize
        monsize = get_size(monsize)
        free = get_size(free)
        await query.message.edit_text(
            text=script.STATUS_TXT.format(total, users, chats, monsize, free),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
    elif query.data.startswith("setgs"):
        ident, set_type, status, grp_id = query.data.split("#")
        grpid = await active_connection(str(query.from_user.id))

        if str(grp_id) != str(grpid):
            await query.message.edit("Your Active Connection Has Been Changed. Go To /settings.")
            return await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')

        if status == "True":
            await save_group_settings(grpid, set_type, False)
        else:
            await save_group_settings(grpid, set_type, True)

        settings = await get_settings(grpid)

        if settings is not None:
            buttons = [
                [
                    InlineKeyboardButton('𝐅𝐈𝐋𝐓𝐄𝐑 𝐁𝐔𝐓𝐓𝐎𝐍',
                                         callback_data=f'setgs#button#{settings["button"]}#{str(grp_id)}'),
                    InlineKeyboardButton('𝐒𝐈𝐍𝐆𝐋𝐄' if settings["button"] else '𝐃𝐎𝐔𝐁𝐋𝐄',
                                         callback_data=f'setgs#button#{settings["button"]}#{str(grp_id)}')
                ],
                [
                    InlineKeyboardButton('𝐁𝐎𝐓 𝐏𝐌', callback_data=f'setgs#botpm#{settings["botpm"]}#{str(grp_id)}'),
                    InlineKeyboardButton('✅ 𝐘𝐄𝐒' if settings["botpm"] else '❌ 𝐍𝐎',
                                         callback_data=f'setgs#botpm#{settings["botpm"]}#{str(grp_id)}')
                ],
                [
                    InlineKeyboardButton('𝐅𝐈𝐋𝐄 𝐒𝐄𝐂𝐔𝐑𝐄',
                                         callback_data=f'setgs#file_secure#{settings["file_secure"]}#{str(grp_id)}'),
                    InlineKeyboardButton('✅ 𝐘𝐄𝐒' if settings["file_secure"] else '❌ 𝐍𝐎',
                                         callback_data=f'setgs#file_secure#{settings["file_secure"]}#{str(grp_id)}')
                ],
                [
                    InlineKeyboardButton('𝐈𝐌𝐃𝐁', callback_data=f'setgs#imdb#{settings["imdb"]}#{str(grp_id)}'),
                    InlineKeyboardButton('✅ 𝐘𝐄𝐒' if settings["imdb"] else '❌ 𝐍𝐎',
                                         callback_data=f'setgs#imdb#{settings["imdb"]}#{str(grp_id)}')
                ],
                [
                    InlineKeyboardButton('𝐒𝐏𝐄𝐋𝐋 𝐂𝐇𝐄𝐂𝐊',
                                         callback_data=f'setgs#spell_check#{settings["spell_check"]}#{str(grp_id)}'),
                    InlineKeyboardButton('✅ 𝐘𝐄𝐒' if settings["spell_check"] else '❌ 𝐍𝐎',
                                         callback_data=f'setgs#spell_check#{settings["spell_check"]}#{str(grp_id)}')
                ],
                [
                    InlineKeyboardButton('𝐖𝐄𝐋𝐂𝐎𝐌𝐄', callback_data=f'setgs#welcome#{settings["welcome"]}#{str(grp_id)}'),
                    InlineKeyboardButton('✅ 𝐘𝐄𝐒' if settings["welcome"] else '❌ 𝐍𝐎',
                                         callback_data=f'setgs#welcome#{settings["welcome"]}#{str(grp_id)}')
                ]
            ]
            reply_markup = InlineKeyboardMarkup(buttons)
            await query.message.edit_reply_markup(reply_markup)
        await query.answer('𝙿𝙻𝙴𝙰𝚂𝙴 𝚂𝙷𝙰𝚁𝙴 𝙰𝙽𝙳 𝚂𝚄𝙿𝙿𝙾𝚁𝚃')

    # Ensure the callback is always acknowledged for any branch that did not
    # explicitly call query.answer().  The most common cases are the menu
    # navigation callbacks (start, help, about, etc.) that only call
    # edit_message_text without answering the query.  All file-delivery
    # branches above always call query.answer() themselves, so this safety
    # net call raises QueryIdInvalid which we silently discard.
    try:
        await query.answer()
    except Exception:
        pass


async def auto_filter(client, msg, spoll=False):
    if not spoll:
        message = msg
        settings = await get_settings(message.chat.id)
        if message.text.startswith("/"): return  # ignore commands
        if re.findall("((^\/|^,|^!|^\.|^[\U0001F600-\U000E007F]).*)", message.text):
            return
        if 2 < len(message.text) < 100:
            search = message.text
            files, offset, total_results = await get_search_results(search.lower(), offset=0, filter=True)
            if not files:
                if settings["spell_check"]:
                    return await advantage_spell_chok(msg)
                else:
                    return
        else:
            return
    else:
        settings = await get_settings(msg.message.chat.id)
        message = msg.message.reply_to_message or msg.message # msg will be callback query
        search, files, offset, total_results = spoll
    pre = 'filep' if settings['file_secure'] else 'file'
    if settings["button"]:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"[{get_size(file.file_size)}]-✨-{file.file_name}", callback_data=f'{pre}#{file.file_id}'
                ),
            ]
            for file in files
        ]
    else:
        btn = [
            [
                InlineKeyboardButton(
                    text=f"{file.file_name}",
                    callback_data=f'{pre}#{file.file_id}',
                ),
                InlineKeyboardButton(
                    text=f"{get_size(file.file_size)}",
                    callback_data=f'{pre}#{file.file_id}',
                ),
            ]
            for file in files
        ]

    key = f"{message.chat.id}-{message.id}"
    BUTTONS[key] = search
    BUTTON_LANG_SELECTION[key] = None
    _reset_page_cache(key)
    _store_page_results(key, 0, files)
    req = message.from_user.id if message.from_user else 0

    language_rows = _build_language_buttons(req, key, BUTTON_LANG_SELECTION[key])
    if language_rows:
        btn.extend(language_rows)

    btn.append([InlineKeyboardButton("📨 Send All", callback_data=f"sendall_{req}_{key}_0")])
    
    # Discovery buttons
    discover_row = [
        InlineKeyboardButton("⭐ Add Watchlist", callback_data=f"add_watchlist#{search}"),
    ]
    if imdb and imdb.get('url'):
        # Using YouTube search for the trailer as it's more reliable than scraping IMDb's player
        yt_query = search.replace(" ", "+")
        discover_row.append(InlineKeyboardButton("📹 Trailer", url=f"https://www.youtube.com/results?search_query={yt_query}+trailer"))
    
    btn.append(discover_row)

    if offset != "":
        btn.append(
            [InlineKeyboardButton(text=f"🗓 1/{math.ceil(int(total_results) / 10)}", callback_data="pages"),
             InlineKeyboardButton(text="𝗡𝗲𝘅𝘁 ⏩", callback_data=f"next_{req}_{key}_{offset}")]
        )
    else:
        btn.append(
            [InlineKeyboardButton(text="🗓 1/1", callback_data="pages")]
        )
    imdb = await get_poster(search, file=(files[0]).file_name) if settings["imdb"] else None
    TEMPLATE = settings['template']
    if imdb:
        cap = TEMPLATE.format(
            query=search,
            title=imdb['title'],
            votes=imdb['votes'],
            aka=imdb["aka"],
            seasons=imdb["seasons"],
            box_office=imdb['box_office'],
            localized_title=imdb['localized_title'],
            kind=imdb['kind'],
            imdb_id=imdb["imdb_id"],
            cast=imdb["cast"],
            runtime=imdb["runtime"],
            countries=imdb["countries"],
            certificates=imdb["certificates"],
            languages=imdb["languages"],
            director=imdb["director"],
            writer=imdb["writer"],
            producer=imdb["producer"],
            composer=imdb["composer"],
            cinematographer=imdb["cinematographer"],
            music_team=imdb["music_team"],
            distributors=imdb["distributors"],
            release_date=imdb['release_date'],
            year=imdb['year'],
            genres=imdb['genres'],
            poster=imdb['poster'],
            plot=imdb['plot'],
            rating=imdb['rating'],
            url=imdb['url'],
            **locals()
        )
        cap = _decorate_caption(search, cap)
    else:
        cap = _format_fallback_caption(search)
    if imdb and imdb.get('poster'):
        try:
            hehe =  await message.reply_photo(photo=imdb.get('poster'), caption=cap[:1024],
                                      reply_markup=InlineKeyboardMarkup(btn))
            if SELF_DELETE:
                await asyncio.sleep(SELF_DELETE_SECONDS)
                await hehe.delete()

        except (MediaEmpty, PhotoInvalidDimensions, WebpageMediaEmpty):
            pic = imdb.get('poster')
            poster = pic.replace('.jpg', "._V1_UX360.jpg")
            hmm = await message.reply_photo(photo=poster, caption=cap[:1024], reply_markup=InlineKeyboardMarkup(btn))
            if SELF_DELETE:
                await asyncio.sleep(SELF_DELETE_SECONDS)
                await hmm.delete()
        except Exception as e:
            logger.exception(e)
            fek = await message.reply_text(cap, reply_markup=InlineKeyboardMarkup(btn))
            if SELF_DELETE:
                await asyncio.sleep(SELF_DELETE_SECONDS)
                await fek.delete()
    else:
        fuk = await message.reply_text(cap, reply_markup=InlineKeyboardMarkup(btn))
        if SELF_DELETE:
            await asyncio.sleep(SELF_DELETE_SECONDS)
            await fuk.delete()

async def advantage_spell_chok(msg):
    query = re.sub(
        r"\b(pl(i|e)*?(s|z+|ease|se|ese|(e+)s(e)?)|((send|snd|giv(e)?|gib)(\sme)?)|movie(s)?|new|latest|br((o|u)h?)*|^h(e|a)?(l)*(o)*|mal(ayalam)?|t(h)?amil|file|that|find|und(o)*|kit(t(i|y)?)?o(w)?|thar(u)?(o)*w?|kittum(o)*|aya(k)*(um(o)*)?|full\smovie|any(one)|with\ssubtitle(s)?)",
        "", msg.text, flags=re.IGNORECASE)  # plis contribute some common words
    query = query.strip() + " movie"
    g_s = await search_gagala(query)
    g_s += await search_gagala(msg.text)
    gs_parsed = []
    if not g_s:
        k = await msg.reply(_format_not_found_message(msg.text))
        await asyncio.sleep(8)
        await k.delete()
        return
    regex = re.compile(r".*(imdb|wikipedia).*", re.IGNORECASE)  # look for imdb / wiki results
    gs = list(filter(regex.match, g_s))
    gs_parsed = [re.sub(
        r'\b(\-([a-zA-Z-\s])\-\simdb|(\-\s)?imdb|(\-\s)?wikipedia|\(|\)|\-|reviews|full|all|episode(s)?|film|movie|series)',
        '', i, flags=re.IGNORECASE) for i in gs]
    if not gs_parsed:
        reg = re.compile(r"watch(\s[a-zA-Z0-9_\s\-\(\)]*)*\|.*",
                         re.IGNORECASE)  # match something like Watch Niram | Amazon Prime
        for mv in g_s:
            match = reg.match(mv)
            if match:
                gs_parsed.append(match.group(1))
    user = msg.from_user.id if msg.from_user else 0
    movielist = []
    gs_parsed = list(dict.fromkeys(gs_parsed))  # removing duplicates https://stackoverflow.com/a/7961425
    if len(gs_parsed) > 3:
        gs_parsed = gs_parsed[:3]
    if gs_parsed:
        for mov in gs_parsed:
            imdb_s = await get_poster(mov.strip(), bulk=True)  # searching each keyword in imdb
            if imdb_s:
                movielist += [movie.get('title') for movie in imdb_s]
    movielist += [(re.sub(r'(\-|\(|\)|_)', '', i, flags=re.IGNORECASE)).strip() for i in gs_parsed]
    movielist = list(dict.fromkeys(movielist))  # removing duplicates
    if not movielist:
        k = await msg.reply(_format_not_found_message(msg.text))
        await asyncio.sleep(8)
        await k.delete()
        return
    SPELL_CHECK[msg.id] = movielist
    btn = [[
        InlineKeyboardButton(
            text=movie.strip(),
            callback_data=f"spolling#{user}#{k}",
        )
    ] for k, movie in enumerate(movielist)]
    btn.append([InlineKeyboardButton(text="Close", callback_data=f'spolling#{user}#close_spellcheck')])
    await msg.reply(
        _format_spellcheck_prompt(),
        reply_markup=InlineKeyboardMarkup(btn),
    )


async def manual_filters(client, message, text=False):
    group_id = message.chat.id
    name = text or message.text
    reply_id = message.reply_to_message.id if message.reply_to_message else message.id
    keywords = await get_filters(group_id)
    for keyword in reversed(sorted(keywords, key=len)):
        pattern = r"( |^|[^\w])" + re.escape(keyword) + r"( |$|[^\w])"
        if re.search(pattern, name, flags=re.IGNORECASE):
            reply_text, btn, alert, fileid = await find_filter(group_id, keyword)

            if reply_text:
                reply_text = reply_text.replace("\\n", "\n").replace("\\t", "\t")
                reply_text = clean_caption(reply_text)

            if btn is not None:
                try:
                    if fileid == "None":
                        if btn == "[]":
                            await client.send_message(
                                group_id, 
                                reply_text, 
                                disable_web_page_preview=True,
                                reply_to_message_id=reply_id)
                        else:
                            button = eval(btn)
                            await client.send_message(
                                group_id,
                                reply_text,
                                disable_web_page_preview=True,
                                reply_markup=InlineKeyboardMarkup(button),
                                reply_to_message_id=reply_id
                            )
                    elif btn == "[]":
                        await client.send_cached_media(
                            group_id,
                            fileid,
                            caption=clean_caption(reply_text) or "",
                            reply_to_message_id=reply_id
                        )
                    else:
                        button = eval(btn)
                        await message.reply_cached_media(
                            fileid,
                            caption=clean_caption(reply_text) or "",
                            reply_markup=InlineKeyboardMarkup(button),
                            reply_to_message_id=reply_id
                        )
                except Exception as e:
                    logger.exception(e)
                break
    else:
        return False
   
