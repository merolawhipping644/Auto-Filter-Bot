import motor.motor_asyncio
import logging
from info import DATABASE_NAME, DATABASE_URI, DATABASE_URI_2, DATABASE_NAME_2, IMDB, IMDB_TEMPLATE, MELCOW_NEW_USERS, P_TTI_SHOW_OFF, SINGLE_BUTTON, SPELL_CHECK_REPLY, PROTECT_CONTENT

logger = logging.getLogger(__name__)

class Database:
    
    def __init__(self, uri, database_name, uri2=None, db_name2=None):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users
        self.grp = self.db.groups
        self.watchlist = self.db.watchlist

        # Secondary DB
        self._client2 = None
        self.db2 = None
        self.col2 = None
        self.grp2 = None
        self.watchlist2 = None
        if uri2:
            try:
                self._client2 = motor.motor_asyncio.AsyncIOMotorClient(uri2)
                self.db2 = self._client2[db_name2 or database_name]
                self.col2 = self.db2.users
                self.grp2 = self.db2.groups
                self.watchlist2 = self.db2.watchlist
                logger.info("✅ Secondary users/chats DB connected")
            except Exception as e:
                logger.warning(f"⚠️ Secondary users/chats DB connection failed: {e}")

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )


    def new_group(self, id, title):
        return dict(
            id = id,
            title = title,
            chat_status=dict(
                is_disabled=False,
                reason="",
            ),
        )
    
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        try:
            await self.col.insert_one(user)
        except Exception as e:
            logger.error(f"Primary DB insert_one failed: {e}")
            
        if self.col2 is not None:
            try:
                await self.col2.update_one({'id': int(id)}, {'$set': user}, upsert=True)
            except Exception as e:
                pass
    
    async def is_user_exist(self, id):
        user = await self.col.find_one({'id':int(id)})
        if not user and self.col2 is not None:
            try:
                user = await self.col2.find_one({'id':int(id)})
            except:
                pass
        return bool(user)
    
    async def total_users_count(self):
        count = await self.col.count_documents({})
        if self.col2 is not None:
            try:
                 count += await self.col2.count_documents({})
            except:
                 pass
        return count
    
    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        try:
            await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
        except:
            pass
        if self.col2 is not None:
            try:
                await self.col2.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
            except:
                pass
    
    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        try:
            await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})
        except:
            pass
        if self.col2 is not None:
            try:
                await self.col2.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})
            except:
                pass

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self.col.find_one({'id':int(id)})
        if not user and self.col2 is not None:
            try:
                user = await self.col2.find_one({'id':int(id)})
            except:
                pass
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return self.col.find({})
    

    async def delete_user(self, user_id):
        try:
            await self.col.delete_many({'id': int(user_id)})
        except:
            pass
        if self.col2 is not None:
             try:
                 await self.col2.delete_many({'id': int(user_id)})
             except:
                 pass


    async def get_banned(self):
        try:
            users = self.col.find({'ban_status.is_banned': True})
            chats = self.grp.find({'chat_status.is_disabled': True})
            b_chats = [chat['id'] async for chat in chats]
            b_users = [user['id'] async for user in users]
            return b_users, b_chats
        except Exception as e:
            logger.error(f"Failed to get_banned: {e}")
            return [], []

    async def add_chat(self, chat, title):
        chat_obj = self.new_group(chat, title)
        try:
            await self.grp.insert_one(chat_obj)
        except:
            pass
        if self.grp2 is not None:
             try:
                 await self.grp2.update_one({'id': int(chat)}, {'$set': chat_obj}, upsert=True)
             except:
                 pass
    

    async def get_chat(self, chat):
        chat_data = await self.grp.find_one({'id':int(chat)})
        if not chat_data and self.grp2 is not None:
            try:
                chat_data = await self.grp2.find_one({'id':int(chat)})
            except:
                pass
        return False if not chat_data else chat_data.get('chat_status')
    

    async def re_enable_chat(self, id):
        chat_status=dict(
            is_disabled=False,
            reason="",
            )
        try:
            await self.grp.update_one({'id': int(id)}, {'$set': {'chat_status': chat_status}})
        except:
            pass
        if self.grp2 is not None:
             try:
                 await self.grp2.update_one({'id': int(id)}, {'$set': {'chat_status': chat_status}})
             except:
                 pass
        
    async def update_settings(self, id, settings):
        try:
            await self.grp.update_one({'id': int(id)}, {'$set': {'settings': settings}})
        except:
            pass
        if self.grp2 is not None:
             try:
                 await self.grp2.update_one({'id': int(id)}, {'$set': {'settings': settings}})
             except:
                 pass
        
    
    async def get_settings(self, id):
        default = {
            'button': SINGLE_BUTTON,
            'botpm': P_TTI_SHOW_OFF,
            'file_secure': PROTECT_CONTENT,
            'imdb': IMDB,
            'spell_check': SPELL_CHECK_REPLY,
            'welcome': MELCOW_NEW_USERS,
            'template': IMDB_TEMPLATE
        }
        chat = await self.grp.find_one({'id':int(id)})
        if not chat and self.grp2 is not None:
             try:
                 chat = await self.grp2.find_one({'id':int(id)})
             except:
                 pass
        if chat:
            return chat.get('settings', default)
        return default
    

    async def disable_chat(self, chat, reason="No Reason"):
        chat_status=dict(
            is_disabled=True,
            reason=reason,
            )
        try:
            await self.grp.update_one({'id': int(chat)}, {'$set': {'chat_status': chat_status}})
        except:
            pass
        if self.grp2 is not None:
             try:
                 await self.grp2.update_one({'id': int(chat)}, {'$set': {'chat_status': chat_status}})
             except:
                 pass
    

    async def total_chat_count(self):
        count = await self.grp.count_documents({})
        if self.grp2 is not None:
             try:
                 count += await self.grp2.count_documents({})
             except:
                 pass
        return count
    

    async def get_all_chats(self):
        return self.grp.find({})


    async def get_db_size(self):
        return (await self.db.command("dbstats"))['dataSize']

    # --- Watchlist Methods ---
    async def add_to_watchlist(self, user_id, movie_name):
        try:
            await self.watchlist.update_one(
                {'user_id': int(user_id)},
                {'$addToSet': {'movies': movie_name}},
                upsert=True
            )
        except:
            pass
        if self.watchlist2 is not None:
             try:
                 await self.watchlist2.update_one(
                    {'user_id': int(user_id)},
                    {'$addToSet': {'movies': movie_name}},
                    upsert=True
                )
             except:
                 pass

    async def remove_from_watchlist(self, user_id, movie_name):
        try:
            await self.watchlist.update_one(
                {'user_id': int(user_id)},
                {'$pull': {'movies': movie_name}}
            )
        except:
            pass
        if self.watchlist2 is not None:
             try:
                 await self.watchlist2.update_one(
                    {'user_id': int(user_id)},
                    {'$pull': {'movies': movie_name}}
                )
             except:
                 pass

    async def get_watchlist(self, user_id):
        user = await self.watchlist.find_one({'user_id': int(user_id)})
        if not user and self.watchlist2 is not None:
             try:
                 user = await self.watchlist2.find_one({'user_id': int(user_id)})
             except:
                 pass
        return user.get('movies', []) if user else []

    async def clear_watchlist(self, user_id):
        try:
            await self.watchlist.delete_one({'user_id': int(user_id)})
        except:
            pass
        if self.watchlist2 is not None:
             try:
                 await self.watchlist2.delete_one({'user_id': int(user_id)})
             except:
                 pass

db = Database(DATABASE_URI, DATABASE_NAME, uri2=DATABASE_URI_2, db_name2=DATABASE_NAME_2)
