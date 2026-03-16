import re
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from database.ia_filterdb import get_search_results
from database.users_chats_db import db
from plugins.pm_filter import auto_filter
from info import ADMINS

GENRES = [
    "Action", "Comedy", "Drama", "Horror", "Sci-Fi", 
    "Thriller", "Romantic", "Animation", "Fantasy", "Mystery"
]

YEARS = [
    "2025", "2024", "2023", "2022", "2021", "2020",
    "2019", "2018", "2017", "2016", "2015"
]

@Client.on_message(filters.command("browse") & filters.private)
async def browse_menu(client, message):
    buttons = [
        [
            InlineKeyboardButton("🎭 Genres", callback_data="browse_genres"),
            InlineKeyboardButton("📅 Years", callback_data="browse_years")
        ],
        [
            InlineKeyboardButton("⭐ My Watchlist", callback_data="browse_watchlist"),
            InlineKeyboardButton("🔥 Trending", callback_data="browse_search:trending")
        ],
        [
            InlineKeyboardButton("✨ Latest", callback_data="browse_search:latest")
        ]
    ]
    await message.reply_text(
        "<b>🦇 Welcome to the Bat-Vault Discovery!</b>\n\n"
        "How would you like to hunt for your next cinematic adventure?",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@Client.on_callback_query(filters.regex(r"^browse_"), group=-1)
async def browse_callback(client: Client, query: CallbackQuery):
    data = query.data
    
    if data == "browse_main":
        buttons = [
            [
                InlineKeyboardButton("🎭 Genres", callback_data="browse_genres"),
                InlineKeyboardButton("📅 Years", callback_data="browse_years")
            ],
            [
                InlineKeyboardButton("⭐ My Watchlist", callback_data="browse_watchlist"),
                InlineKeyboardButton("🔥 Trending", callback_data="browse_search:trending")
            ],
            [
                InlineKeyboardButton("✨ Latest", callback_data="browse_search:latest")
            ]
        ]
        await query.message.edit_text(
            "<b>🦇 Welcome to the Bat-Vault Discovery!</b>\n\n"
            "How would you like to hunt for your next cinematic adventure?",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
        
    elif data == "browse_watchlist":
        await watchlist_view(client, query, query.from_user.id, is_callback=True)
        
    elif data == "browse_genres":
        buttons = []
        for i in range(0, len(GENRES), 2):
            row = [InlineKeyboardButton(GENRES[i], callback_data=f"browse_search:{GENRES[i].lower()}")]
            if i + 1 < len(GENRES):
                row.append(InlineKeyboardButton(GENRES[i+1], callback_data=f"browse_search:{GENRES[i+1].lower()}"))
            buttons.append(row)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="browse_main")])
        await query.message.edit_text(
            "<b>🎭 Select a Genre to Explore:</b>",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
        
    elif data == "browse_years":
        buttons = []
        for i in range(0, len(YEARS), 3):
            row = [InlineKeyboardButton(YEARS[i], callback_data=f"browse_search:{YEARS[i]}")]
            if i + 1 < len(YEARS):
                row.append(InlineKeyboardButton(YEARS[i+1], callback_data=f"browse_search:{YEARS[i+1]}"))
            if i + 2 < len(YEARS):
                row.append(InlineKeyboardButton(YEARS[i+2], callback_data=f"browse_search:{YEARS[i+2]}"))
            buttons.append(row)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="browse_main")])
        await query.message.edit_text(
            "<b>📅 Select a Year to Explore:</b>",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await query.answer()
        
    elif data.startswith("browse_search:"):
        search_query = data.split(":")[1]
        
        # Mapping specialized searches
        if search_query == "trending":
            # For now, trending just searches for popular current year/quality
            search_query = "2024" 
        elif search_query == "latest":
            search_query = "2025"

        await query.answer(f"🔎 Searching for {search_query}...")
        
        # We need to simulate a message for auto_filter if it's coming from a callback
        # But auto_filter expects msg.message.reply_to_message if spoll is provided.
        # Let's adjust auto_filter in pm_filter.py if needed, or follow its pattern.
        
        files, offset, total_results = await get_search_results(search_query, offset=0, filter=True)
        
        if not files:
            await query.answer("No files found in this category yet! 🦇", show_alert=True)
            return

        # Prepare spoll for auto_filter
        spoll = (search_query, files, offset, total_results)
        
        # We need to make sure the callback query has a reply_to_message or we handle it
        # In pm_filter.py: message = msg.message.reply_to_message
        # Since this is a direct command /browse, there might not be a reply_to_message.
        # Let's check how to handle this.
        
        await auto_filter(client, query, spoll=spoll)
    
@Client.on_callback_query(filters.regex(r"^add_watchlist#"), group=-1)
async def add_watchlist_handler(client, query: CallbackQuery):
    movie_name = query.data.split("#")[1]
    user_id = query.from_user.id
    
    await db.add_to_watchlist(user_id, movie_name)
    await query.answer(f"⭐ Added \"{movie_name}\" to your watchlist!", show_alert=True)

@Client.on_callback_query(filters.regex(r"^rem_watchlist#"), group=-1)
async def remove_watchlist_handler(client, query: CallbackQuery):
    movie_name = query.data.split("#")[1]
    user_id = query.from_user.id
    
    await db.remove_from_watchlist(user_id, movie_name)
    # Refresh the watchlist view
    await watchlist_view(client, query, user_id, is_callback=True)
    await query.answer(f"🗑 Removed \"{movie_name}\" from watchlist.")

@Client.on_message(filters.command("watchlist") & filters.private)
async def watchlist_cmd(client, message):
    await watchlist_view(client, message, message.from_user.id)

async def watchlist_view(client, message_or_query, user_id, is_callback=False):
    if is_callback:
        message = message_or_query.message
        query = message_or_query
    else:
        message = message_or_query
        query = None

    movies = await db.get_watchlist(user_id)
    
    if not movies:
        text = "<b>Your Watchlist is currently empty. 📂</b>\n\nSearch for a movie and tap <b>⭐ Add Watchlist</b> to save it here!"
        if is_callback:
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Discovery", callback_data="browse_main")]]))
            await query.answer()
        else:
            await message.reply_text(text)
        return

    text = f"<b>🦇 Batman's Watchlist for {message.from_user.first_name}:</b>\n\n"
    buttons = []
    
    for movie in movies:
        text += f"• <code>{movie}</code>\n"
        buttons.append([
            InlineKeyboardButton(f"🔎 Search: {movie}", callback_data=f"browse_search:{movie.lower()}"),
            InlineKeyboardButton("🗑 Remove", callback_data=f"rem_watchlist#{movie}")
        ])
    
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="browse_main")])
    
    if is_callback:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        await query.answer()
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))
