import motor.motor_asyncio
from pyrogram import enums
from info import DATABASE_URI, DATABASE_NAME, DATABASE_URI_2, DATABASE_NAME_2
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

myclient = motor.motor_asyncio.AsyncIOMotorClient(DATABASE_URI)
mydb = myclient[DATABASE_NAME]

# Secondary DB (optional)
_myclient2 = None
_mydb2 = None
if DATABASE_URI_2:
    try:
        _myclient2 = motor.motor_asyncio.AsyncIOMotorClient(DATABASE_URI_2)
        _mydb2 = _myclient2[DATABASE_NAME_2]
        logger.info("✅ Secondary filters DB connected")
    except Exception as _e:
        logger.warning(f"⚠️ Secondary filters DB connection failed: {_e}")
        _myclient2 = _mydb2 = None


async def add_filter(grp_id, text, reply_text, btn, file, alert):
    mycol = mydb[str(grp_id)]
    data = {
        'text': str(text),
        'reply': str(reply_text),
        'btn': str(btn),
        'file': str(file),
        'alert': str(alert)
    }
    try:
        await mycol.update_one({'text': str(text)}, {"$set": data}, upsert=True)
    except Exception:
        logger.exception('Some error occurred on primary filters DB!', exc_info=True)

    if _mydb2 is not None:
        try:
            await _mydb2[str(grp_id)].update_one({'text': str(text)}, {"$set": data}, upsert=True)
        except Exception:
            logger.warning('Secondary filters DB add_filter failed')


async def find_filter(group_id, name):
    mycol = mydb[str(group_id)]
    query = mycol.find({"text": name})
    try:
        reply_text = btn = fileid = alert = None
        async for file in query:
            reply_text = file['reply']
            btn = file['btn']
            fileid = file['file']
            alert = file.get('alert')
        if reply_text is not None:
            return reply_text, btn, alert, fileid
    except Exception:
        pass  # try secondary

    if _mydb2 is not None:
        try:
            reply_text = btn = fileid = alert = None
            query2 = _mydb2[str(group_id)].find({"text": name})
            async for file in query2:
                reply_text = file['reply']
                btn = file['btn']
                fileid = file['file']
                alert = file.get('alert')
            if reply_text is not None:
                return reply_text, btn, alert, fileid
        except Exception:
            pass

    return None, None, None, None


async def get_filters(group_id):
    mycol = mydb[str(group_id)]
    texts = []
    query = mycol.find()
    try:
        async for file in query:
            text = file['text']
            texts.append(text)
    except Exception:
        pass
    return texts


async def delete_filter(message, text, group_id):
    mycol = mydb[str(group_id)]
    myquery = {'text': text}
    query = await mycol.count_documents(myquery)
    if query == 1:
        await mycol.delete_one(myquery)
        if _mydb2 is not None:
            try:
                await _mydb2[str(group_id)].delete_one(myquery)
            except Exception:
                pass
        await message.reply_text(
            f"'`{text}`'  deleted. I'll not respond to that filter anymore.",
            quote=True,
            parse_mode=enums.ParseMode.MARKDOWN
        )
    else:
        await message.reply_text("Couldn't find that filter!", quote=True)


async def del_all(message, group_id, title):
    collections = await mydb.list_collection_names()
    if str(group_id) not in collections:
        await message.edit_text(f"Nothing to remove in {title}!")
        return

    mycol = mydb[str(group_id)]
    try:
        await mycol.drop()
        if _mydb2 is not None:
            try:
                await _mydb2[str(group_id)].drop()
            except Exception:
                pass
        await message.edit_text(f"All filters from {title} has been removed")
    except Exception:
        await message.edit_text("Couldn't remove all filters from group!")
        return


async def count_filters(group_id):
    mycol = mydb[str(group_id)]
    count = await mycol.count_documents({})
    return False if count == 0 else count


async def filter_stats():
    collections = await mydb.list_collection_names()

    if "CONNECTION" in collections:
        collections.remove("CONNECTION")

    totalcount = 0
    for collection in collections:
        mycol = mydb[collection]
        count = await mycol.count_documents({})
        totalcount += count

    totalcollections = len(collections)

    return totalcollections, totalcount
