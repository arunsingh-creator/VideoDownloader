from functools import wraps
from pyrogram.types import Message
from config import settings
import structlog

log = structlog.get_logger(__name__)

def authorized_only(func):
    @wraps(func)
    async def wrapper(client, message: Message, *args, **kwargs):
        if not settings.allowed_users:
            return await func(client, message, *args, **kwargs)

        if message.from_user and message.from_user.id in settings.allowed_users:
            return await func(client, message, *args, **kwargs)

        await message.reply_text("You are not authorized to use this bot.")
    return wrapper
