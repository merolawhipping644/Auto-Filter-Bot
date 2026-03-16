import motor.motor_asyncio
from datetime import datetime
from info import DATABASE_NAME, DATABASE_URI, DATABASE_URI_2, DATABASE_NAME_2
import logging

logger = logging.getLogger(__name__)


class LinkCacheDB:
    """Database handler for cached download links (Fast Download & GoFile)"""

    def __init__(self, uri, database_name, uri2=None, db_name2=None):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.cache = self.db.cached_links

        # Secondary (optional)
        self._client2 = None
        self.cache2 = None
        if uri2:
            try:
                self._client2 = motor.motor_asyncio.AsyncIOMotorClient(uri2)
                _db2 = self._client2[db_name2 or database_name]
                self.cache2 = _db2.cached_links
                logger.info("✅ Secondary link-cache DB connected")
            except Exception as e:
                logger.warning(f"⚠️ Secondary link-cache DB connection failed: {e}")
                self._client2 = self.cache2 = None

    async def create_indexes(self):
        """Create indexes for efficient querying and TTL"""
        for collection, label in [(self.cache, "primary"), (self.cache2, "secondary")]:
            if collection is None:
                continue
            try:
                await collection.create_index(
                    [("file_id", 1), ("link_type", 1)], unique=True
                )
                await collection.create_index(
                    "created_at", expireAfterSeconds=2592000  # 30 days
                )
                logger.info(f"Link cache indexes created successfully ({label})")
            except Exception as e:
                logger.warning(f"Index creation warning on {label} (may already exist): {e}")

    async def get_cached_link(self, file_id: str, link_type: str) -> str | None:
        """Retrieve cached link, checking primary first then secondary."""
        for collection, label in [(self.cache, "primary"), (self.cache2, "secondary")]:
            if collection is None:
                continue
            try:
                result = await collection.find_one(
                    {"file_id": file_id, "link_type": link_type}
                )
                if result:
                    logger.info(f"Cache HIT ({label}) for {link_type} - file_id: {file_id}")
                    return result.get("url")
            except Exception as e:
                logger.error(f"Error retrieving cached link from {label}: {e}")

        logger.info(f"Cache MISS for {link_type} - file_id: {file_id}")
        return None

    async def save_cached_link(
        self, file_id: str, link_type: str, url: str, file_name: str = None
    ) -> bool:
        """Save a link to cache (primary + secondary)."""
        doc = {
            "file_id": file_id,
            "link_type": link_type,
            "url": url,
            "file_name": file_name,
            "created_at": datetime.utcnow(),
        }
        saved = False
        for collection, label in [(self.cache, "primary"), (self.cache2, "secondary")]:
            if collection is None:
                continue
            try:
                await collection.update_one(
                    {"file_id": file_id, "link_type": link_type},
                    {"$set": doc},
                    upsert=True,
                )
                logger.info(f"Cached {link_type} link ({label}) for file_id: {file_id}")
                saved = True
            except Exception as e:
                logger.error(f"Error saving cached link to {label}: {e}")

        return saved

    async def delete_cached_link(self, file_id: str, link_type: str) -> bool:
        """Delete a specific cached link from both DBs."""
        deleted = False
        for collection, label in [(self.cache, "primary"), (self.cache2, "secondary")]:
            if collection is None:
                continue
            try:
                result = await collection.delete_one(
                    {"file_id": file_id, "link_type": link_type}
                )
                if result.deleted_count > 0:
                    logger.info(f"Deleted {link_type} link ({label}) for file_id: {file_id}")
                    deleted = True
            except Exception as e:
                logger.error(f"Error deleting cached link from {label}: {e}")

        return deleted

    async def clear_all_cache(self, link_type: str = None) -> int:
        """Clear all cached links (or a specific type) from both DBs."""
        total_deleted = 0
        for collection, label in [(self.cache, "primary"), (self.cache2, "secondary")]:
            if collection is None:
                continue
            try:
                query = {"link_type": link_type} if link_type else {}
                result = await collection.delete_many(query)
                total_deleted += result.deleted_count
                logger.info(f"Cleared {result.deleted_count} cached links ({label})")
            except Exception as e:
                logger.error(f"Error clearing cache on {label}: {e}")

        return total_deleted

    async def get_cache_stats(self) -> dict:
        """Get statistics about cached links (primary only)."""
        try:
            total = await self.cache.count_documents({})
            fastdownload_count = await self.cache.count_documents({"link_type": "fastdownload"})
            gofile_count = await self.cache.count_documents({"link_type": "gofile"})
            return {
                "total": total,
                "fastdownload": fastdownload_count,
                "gofile": gofile_count,
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"total": 0, "fastdownload": 0, "gofile": 0}


# Global instance (secondary is optional)
link_cache_db = LinkCacheDB(
    DATABASE_URI, DATABASE_NAME,
    uri2=DATABASE_URI_2 or None,
    db_name2=DATABASE_NAME_2,
)
