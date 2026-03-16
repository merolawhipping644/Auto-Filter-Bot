import logging
from struct import pack
import re
import base64
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Instance, Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from marshmallow.exceptions import ValidationError
from info import (
    DATABASE_URI, DATABASE_NAME, COLLECTION_NAME,
    DATABASE_URI_2, DATABASE_NAME_2, COLLECTION_NAME_2,
    USE_CAPTION_FILTER,
)
from sanitizers import clean_file_name, clean_caption, normalize_for_dedup
import time
from collections import OrderedDict

# Simple LRU Cache for search results
search_cache = OrderedDict()
SEARCH_CACHE_SIZE = 500
SEARCH_CACHE_TTL = 300  # 5 minutes


_LANGUAGE_ALIASES = {
    "english": ("english", "eng"),
    "hindi": ("hindi", "hin"),
    "kannada": ("kannada", "kan"),
    "malayalam": ("malayalam", "mal"),
    "tamil": ("tamil", "tam"),
    "telugu": ("telugu", "tel"),
}

_LANGUAGE_LOOKUP = {
    alias: canonical
    for canonical, aliases in _LANGUAGE_ALIASES.items()
    for alias in aliases
}


def _compile_language_regex(values: tuple[str, ...]) -> re.Pattern:
    pattern = "|".join(sorted({re.escape(value) for value in values}, key=len, reverse=True))
    return re.compile(rf"(?<![A-Za-z0-9])(?:{pattern})(?![A-Za-z0-9])", re.IGNORECASE)


_LANGUAGE_PATTERNS = {
    canonical: _compile_language_regex(aliases)
    for canonical, aliases in _LANGUAGE_ALIASES.items()
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]
instance = Instance.from_db(db)

# ---------- Secondary DB (optional) ----------
_client2 = None
_db2 = None
_instance2 = None

if DATABASE_URI_2:
    try:
        _client2 = AsyncIOMotorClient(DATABASE_URI_2)
        _db2 = _client2[DATABASE_NAME_2]
        _instance2 = Instance.from_db(_db2)
        logger.info("✅ Secondary database connected")
    except Exception as _e:
        logger.warning(f"⚠️ Secondary DB connection failed: {_e}")
        _client2 = _db2 = _instance2 = None
# ---------------------------------------------

@instance.register
class Media(Document):
    file_id = fields.StrField(attribute='_id')
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)
    norm_name = fields.StrField(allow_none=True)  # canonical dedup key

    class Meta:
        indexes = (
            '$file_name',
            ('file_name', 'text'),
            [('norm_name', 1), ('file_size', 1)],  # fast dedup lookup
        )
        collection_name = COLLECTION_NAME


# Register Media2 only when secondary DB is available
Media2 = None
if _instance2 is not None:
    @_instance2.register
    class Media2(Document):  # type: ignore[no-redef]
        file_id = fields.StrField(attribute='_id')
        file_ref = fields.StrField(allow_none=True)
        file_name = fields.StrField(required=True)
        file_size = fields.IntField(required=True)
        file_type = fields.StrField(allow_none=True)
        mime_type = fields.StrField(allow_none=True)
        caption = fields.StrField(allow_none=True)
        norm_name = fields.StrField(allow_none=True)  # canonical dedup key

        class Meta:
            indexes = (
                '$file_name',
                ('file_name', 'text'),
                [('norm_name', 1), ('file_size', 1)],
            )
            collection_name = COLLECTION_NAME_2


async def save_file(media):
    """Save file in primary (and secondary if configured) database.

    Return codes:
        (True, 1)  — saved successfully
        (False, 0) — exact file_id duplicate (DuplicateKeyError)
        (False, 2) — validation error
        (False, 3) — smart duplicate (same content / size, different name)
    """

    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = clean_file_name(media.file_name)
    caption_html = clean_caption(media.caption.html if media.caption else None)
    norm_name = normalize_for_dedup(getattr(media, 'file_name', None))

    # ---- Smart duplicate check (same norm_name + file_size = same content) ----
    if norm_name:
        existing = await Media.collection.find_one({
            'norm_name': norm_name,
            'file_size': media.file_size,
        })
        if existing:
            logger.info(
                f'Smart-dup skip: "{getattr(media, "file_name", "")}" '
                f'matches existing "{existing.get("file_name", "")}"'
            )
            return False, 3

    # ---- Primary DB ----
    try:
        file = Media(
            file_id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=caption_html,
            norm_name=norm_name or None,
        )
    except ValidationError:
        logger.exception('Error occurred while saving file in primary database')
        return False, 2

    primary_ok: bool
    try:
        await file.commit()
        primary_ok = True
        logger.info(f'{getattr(media, "file_name", "NO_FILE")} saved to primary database')
    except DuplicateKeyError:
        logger.warning(f'{getattr(media, "file_name", "NO_FILE")} already in primary database')
        primary_ok = False # Means exact file_id duplicate skip
    except Exception as e:
        logger.error(f'Primary DB save failed for {getattr(media, "file_name", "NO_FILE")}: {e}')
        primary_ok = False # Primary failed (e.g., quota), but proceed to secondary

    # ---- Secondary DB (best-effort) ----
    secondary_ok = False
    if Media2 is not None:
        try:
            file2 = Media2(
                file_id=file_id,
                file_ref=file_ref,
                file_name=file_name,
                file_size=media.file_size,
                file_type=media.file_type,
                mime_type=media.mime_type,
                caption=caption_html,
                norm_name=norm_name or None,
            )
            await file2.commit()
            secondary_ok = True
            logger.info(f'{getattr(media, "file_name", "NO_FILE")} saved to secondary database')
        except DuplicateKeyError:
            logger.warning(f'{getattr(media, "file_name", "NO_FILE")} already in secondary database')
        except Exception as e:
            logger.warning(f'Secondary DB save failed for {getattr(media, "file_name", "NO_FILE")}: {e}')

    return (True, 1) if (primary_ok or secondary_ok) else (False, 0)



async def get_search_results(query, file_type=None, max_results=7, offset=0, filter=False):
    """For given query return (results, next_offset)"""

    query = query.strip()
    
    # Check cache for first page results (offset 0)
    if offset == 0 and not file_type and not filter:
        cache_key = query.lower()
        if cache_key in search_cache:
            timestamp, cached_data = search_cache[cache_key]
            if time.time() - timestamp < SEARCH_CACHE_TTL:
                # Move to end to show recent usage
                search_cache.move_to_end(cache_key)
                return cached_data

    normalized_query = clean_file_name(query)

    tokens = [token for token in re.split(r"\s+", normalized_query) if token]
    language_keys = []
    search_tokens = []

    for token in tokens:
        canonical = _LANGUAGE_LOOKUP.get(token.lower())
        if canonical:
            language_keys.append(canonical)
        else:
            search_tokens.append(token)

    # Remove language hints from the main search so that users can request, for example,
    # "rrr tel" without the "tel" fragment affecting fuzzy matching of the title. When the
    # query becomes empty we fallback to a catch-all regex (".") and rely purely on the
    # language filters constructed below.
    query = " ".join(search_tokens).strip()

    if not query:
        raw_pattern = "."
    else:
        parts = [re.escape(part) for part in re.split(r"\s+", query) if part]
        if not parts:
            raw_pattern = "."
        elif len(parts) == 1:
            token = parts[0]
            raw_pattern = rf"(?:\b|[\.\+\-_]){token}(?:\b|[\.\+\-_])"
        else:
            raw_pattern = parts[0]
            for token in parts[1:]:
                raw_pattern += rf".*[\s\.\+\-_]{token}"
            raw_pattern = rf"(?:\b|[\.\+\-_]){raw_pattern}(?:\b|[\.\+\-_])"

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except Exception:
        return []

    if USE_CAPTION_FILTER:
        base_filter = {'$or': [{'file_name': regex}, {'caption': regex}]}
    else:
        base_filter = {'file_name': regex}

    language_conditions = []
    if language_keys:
        unique_languages = list(dict.fromkeys(language_keys))
        for language in unique_languages:
            language_regex = _LANGUAGE_PATTERNS[language]
            if USE_CAPTION_FILTER:
                language_conditions.append({
                    '$or': [
                        {'file_name': language_regex},
                        {'caption': language_regex},
                    ]
                })
            else:
                language_conditions.append({'file_name': language_regex})

    criteria = [base_filter]
    if language_conditions:
        criteria.extend(language_conditions)
    if file_type:
        criteria.append({'file_type': file_type})

    if len(criteria) == 1:
        mongo_filter = criteria[0]
    else:
        mongo_filter = {'$and': criteria}

    total_results = await Media.count_documents(mongo_filter)
    next_offset = offset + max_results

    if next_offset > total_results:
        next_offset = ''

    cursor = Media.find(mongo_filter)
    # Sort by recent
    cursor.sort('$natural', -1)
    # Slice files according to offset and max results
    cursor.skip(offset).limit(max_results)
    # Get list of files
    files = await cursor.to_list(length=max_results)

    # ---- Secondary DB merge (optional) ----
    if Media2 is not None:
        try:
            total2 = await Media2.count_documents(mongo_filter)
            cursor2 = Media2.find(mongo_filter)
            cursor2.sort('$natural', -1)
            cursor2.limit(max_results)
            files2 = await cursor2.to_list(length=max_results)

            # Deduplicate by file_id (primary takes priority)
            seen_ids = {f.file_id for f in files}
            for f2 in files2:
                if f2.file_id not in seen_ids:
                    files.append(f2)
                    seen_ids.add(f2.file_id)

            files = files[:max_results]
            total_results += total2
        except Exception as _se:
            logger.warning(f'Secondary DB search failed: {_se}')
    # ----------------------------------------

    result = (files, next_offset, total_results)

    # Cache the result if it's the first page
    if offset == 0 and not file_type and not filter:
        cache_key = query.lower()
        search_cache[cache_key] = (time.time(), result)
        if len(search_cache) > SEARCH_CACHE_SIZE:
            search_cache.popitem(last=False)

    return result



async def get_file_details(query):
    """Return file details, checking primary then secondary DB."""
    filt = {'file_id': query}
    cursor = Media.find(filt)
    filedetails = await cursor.to_list(length=1)
    if not filedetails and Media2 is not None:
        try:
            cursor2 = Media2.find(filt)
            filedetails = await cursor2.to_list(length=1)
        except Exception as e:
            logger.warning(f'Secondary DB get_file_details failed: {e}')
    return filedetails


def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref


async def compact_database():
    """Run the compact command on MongoDB to reclaim disk space after deletions."""
    results = {"primary": False, "secondary": False, "error": None}
    try:
        # MongoDB atlas requires admin privilege for compact, or you can run
        # it directly on the database.
        await db.command({"compact": COLLECTION_NAME})
        results["primary"] = True
    except Exception as e:
        logger.error(f"Error compacting primary DB: {e}")
        results["error"] = str(e)

    if _db2 is not None:
        try:
            await _db2.command({"compact": COLLECTION_NAME_2})
            results["secondary"] = True
        except Exception as e:
            logger.warning(f"Error compacting secondary DB: {e}")
            if not results["error"]:
                results["error"] = str(e)
                
    return results
