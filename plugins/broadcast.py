import asyncio
import datetime
import time
from typing import Iterable, List

from pyrogram import Client, filters
from pyrogram.errors import MessageNotModified

from database.users_chats_db import db
from info import ADMINS
from utils import broadcast_messages


BROADCAST_CONCURRENCY = 20
BROADCAST_BATCH_SIZE = BROADCAST_CONCURRENCY * 5
PROGRESS_UPDATE_INTERVAL = 50


async def _broadcast_to_user(user_id: int, message, semaphore: asyncio.Semaphore):
    async with semaphore:
        return await broadcast_messages(user_id, message)


async def _drain_tasks(
    tasks: Iterable[asyncio.Task],
    status_message,
    counters: dict,
    total_users: int,
):
    """Await tasks as they complete and update progress counters."""

    done = counters["done"]
    success = counters["success"]
    blocked = counters["blocked"]
    deleted = counters["deleted"]
    failed = counters["failed"]

    for finished in asyncio.as_completed(list(tasks)):
        pti, sh = await finished
        done += 1
        if pti:
            success += 1
        else:
            if sh == "Blocked":
                blocked += 1
            elif sh == "Deleted":
                deleted += 1
            else:
                failed += 1

        if done % PROGRESS_UPDATE_INTERVAL == 0 or done == total_users:
            try:
                await status_message.edit(
                    f"ʙʀᴏᴀᴅᴄᴀsᴛ ɪɴ ᴘʀᴏɢʀᴇss:\n\n"
                    f"ᴛᴏᴛᴀʟ ᴜsᴇʀs {total_users}\n"
                    f"ᴄᴏᴍᴘʟᴇᴛᴇᴅ: {done} / {total_users}\n"
                    f"sᴜᴄᴄᴇss: {success}\n"
                    f"ʙʟᴏᴄᴋᴇᴅ: {blocked}\n"
                    f"ᴅᴇʟᴇᴛᴇᴅ: {deleted}\n"
                    f"ғᴀɪʟᴇᴅ: {failed}"
                )
            except MessageNotModified:
                pass

    counters.update(
        {"done": done, "success": success, "blocked": blocked, "deleted": deleted, "failed": failed}
    )

@Client.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.reply)
# https://t.me/GetTGLink/4178
async def verupikkals(bot, message):
    users = await db.get_all_users()
    b_msg = message.reply_to_message
    sts = await message.reply_text(
        text='ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ᴜʀ ᴍᴇssᴀɢᴇ ᴛᴏ ᴛʜɪs ʙᴏᴛ ᴜsᴇʀs...'
    )
    start_time = time.time()
    total_users = await db.total_users_count()

    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    pending: List[asyncio.Task] = []

    counters = {
        "done": 0,
        "success": 0,
        "blocked": 0,
        "deleted": 0,
        "failed": 0,
    }

    async def flush_batch(tasks: List[asyncio.Task]):
        if not tasks:
            return
        await _drain_tasks(tasks, sts, counters, total_users)
        tasks.clear()

    async for user in users:
        user_id = int(user.get('id', 0))
        if not user_id:
            continue

        task = asyncio.create_task(_broadcast_to_user(user_id, b_msg, semaphore))
        pending.append(task)

        if len(pending) >= BROADCAST_BATCH_SIZE:
            await flush_batch(pending)

    await flush_batch(pending)

    time_taken = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        "ʙʀᴏᴀᴅᴄᴀsᴛ sᴜᴄᴄᴇssғᴜʟʟʏ ᴄᴏᴍᴘʟᴇᴛᴇᴅ:\n"
        f"ʙʀᴏᴀᴅᴄᴀsᴛ ᴄᴏᴍᴘʟᴇᴛᴇᴅ ɪɴ {time_taken} sᴇᴄᴏɴᴅs.\n\n"
        f"ᴛᴏᴛᴀʟ ᴜsᴇʀs {total_users}\n"
        f"ᴄᴏᴍᴘʟᴇᴛᴇᴅ: {counters['done']} / {total_users}\n"
        f"sᴜᴄᴄᴇss: {counters['success']}\n"
        f"ʙʟᴏᴄᴋᴇᴅ: {counters['blocked']}\n"
        f"ᴅᴇʟᴇᴛᴇᴅ: {counters['deleted']}\n"
        f"ғᴀɪʟᴇᴅ: {counters['failed']}"
    )
