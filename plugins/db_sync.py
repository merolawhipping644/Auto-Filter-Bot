import logging
from pyrogram import Client, filters, enums
from info import ADMINS, DATABASE_URI, DATABASE_URI_2, DATABASE_NAME, DATABASE_NAME_2
import motor.motor_asyncio
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)

@Client.on_message(filters.command('copydb') & filters.user(ADMINS))
async def copy_db_command(client, message):
    if not DATABASE_URI or not DATABASE_URI_2:
        await message.reply("❌ Error: Both DATABASE_URI and DATABASE_URI_2 must be set in info.py to use this command.")
        return

    msg = await message.reply("⏳ Connecting to databases and starting copy process...\n<i>This might take some time depending on your database size.</i>", parse_mode=enums.ParseMode.HTML)

    try:
        source_client = motor.motor_asyncio.AsyncIOMotorClient(DATABASE_URI)
        source_db = source_client[DATABASE_NAME]
        
        dest_client = motor.motor_asyncio.AsyncIOMotorClient(DATABASE_URI_2)
        dest_db = dest_client[DATABASE_NAME_2]
        
        collections = await source_db.list_collection_names()
    except Exception as e:
        logger.error(f"Error connecting to databases during /copydb: {e}")
        await msg.edit(f"❌ Error connecting to databases:\n<code>{e}</code>", parse_mode=enums.ParseMode.HTML)
        return

    out_text = f"🔄 <b>Starting DB Copy</b>\n\nFound {len(collections)} collections to copy:\n"
    await msg.edit(out_text, parse_mode=enums.ParseMode.HTML)

    for coll_name in collections:
        out_text += f"\n• <code>{coll_name}</code> - Copying... "
        await msg.edit(out_text, parse_mode=enums.ParseMode.HTML)
        
        source_coll = source_db[coll_name]
        dest_coll = dest_db[coll_name]
        
        try:
            total_docs = await source_coll.count_documents({})
            if total_docs == 0:
                out_text += "⚠️ Empty."
                await msg.edit(out_text, parse_mode=enums.ParseMode.HTML)
                continue

            batch_size = 5000
            inserted_total = 0
            batch = []

            docs_cursor = source_coll.find({})
            async for doc in docs_cursor:
                batch.append(doc)
                if len(batch) >= batch_size:
                    try:
                        await dest_coll.insert_many(batch, ordered=False)
                        inserted_total += len(batch)
                    except BulkWriteError as bwe:
                        inserted_total += bwe.details.get('nInserted', 0)
                    batch = []

            if batch:
                try:
                    await dest_coll.insert_many(batch, ordered=False)
                    inserted_total += len(batch)
                except BulkWriteError as bwe:
                    inserted_total += bwe.details.get('nInserted', 0)
            
            out_text += f"✅ Inserted {inserted_total} new docs."
                
        except Exception as e:
            logger.error(f"Failed to copy collection {coll_name}: {e}")
            out_text += f"❌ Error: {e}"
            
        await msg.edit(out_text, parse_mode=enums.ParseMode.HTML)

    out_text += "\n\n✅ <b>Database data successfully copied!</b>"
    await msg.edit(out_text, parse_mode=enums.ParseMode.HTML)
