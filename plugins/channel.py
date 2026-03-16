from pyrogram import Client, filters
from info import CHANNELS
from database.ia_filterdb import save_file, unpack_new_file_id
from movie_updates import publish_movie_update

media_filter = filters.document | filters.video | filters.audio


@Client.on_message(filters.chat(CHANNELS) & media_filter)
async def media(bot, message):
    """Media Handler"""
    for file_type in ("document", "video", "audio"):
        media = getattr(message, file_type, None)
        if media is not None:
            break
    else:
        return

    media.file_type = file_type
    media.caption = message.caption
    saved, _ = await save_file(media)
    if saved:
        file_id, _ = unpack_new_file_id(media.file_id)
        await publish_movie_update(bot, media=media, file_id=file_id)
