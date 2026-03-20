import os
import shutil
import sys
import asyncio
import logging
import requests
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyromod import listen
import helper
import json
import structlog
from dataclasses import dataclass
from db import init_db, save_task, update_task_status, get_task_counts, requeue_failed_tasks, get_pending_tasks

from config import settings

# Configure structured logging
import os
import sys

log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.dev.ConsoleRenderer() if sys.stdout.isatty() else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(log_level),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False
)
log = structlog.get_logger(__name__)

# Initialize Bot
bot = Client(
    settings.session_name,
    api_id=settings.api_id,
    api_hash=settings.api_hash,
    bot_token=settings.bot_token,
)

cancel_process = False

from middleware import authorized_only

@dataclass
class DownloadTask:
    id: str
    name: str
    url: str
    caption: str
    resolution: str
    is_doc: bool
    thumb_path: str
    index: int
    chat_id: int
    message: Message
    is_jw: bool

task_queue = asyncio.Queue()

async def worker():
    while True:
        task = await task_queue.get()
        try:
            await process_single_task(task)
        except Exception as e:
            log.error("Worker encountered error", error=str(e))
        finally:
            task_queue.task_done()

async def process_single_task(task: DownloadTask):
    if cancel_process:
        return

    await update_task_status(task.id, 'started')

    name_clean = task.name
    url = task.url
    count = task.index
    target_res = task.resolution
    is_doc = task.is_doc
    thumb_path = task.thumb_path
    batch_title = task.caption

    # JWPlayer Specific Logic
    if task.is_jw and "jwplayer" in url:
        try:
            url = await get_jw_signed_url(url)
        except Exception as e:
            log.error("Error getting JW link", error=str(e), task_id=task.id)
            try:
                await task.message.reply_text(f"Failed to sign JW link: {name_clean}")
            except Exception:
                await bot.send_message(task.chat_id, f"Failed to sign JW link: {name_clean}")
            return

    # Determine Quality/Format
    try:
        if "youtu" in url or "jwplayer" in url or "m3u8" in url:
             cmd = await get_ytdlp_command(url, name_clean, target_res, count)
        elif ".pdf" in url or "drive" in url:
             cmd = "pdf"
        else:
             cookies_arg = ""
             from pathlib import Path
             if Path("cookies.txt").is_file():
                 cookies_arg = "--cookies cookies.txt"

             from utils import get_download_path
             out_path = get_download_path(f"{name_clean}.mp4")
             cmd = f'yt-dlp {cookies_arg} -o "{out_path}" --no-keep-video --remux-video mkv -N 16 "{url}"'

        msg_text = (
            f"**Downloading:**\n"
            f"**Name:** `{name_clean}`\n"
            f"**Quality:** `{target_res}`\n"
            f"**Mode:** `{'Document' if is_doc else 'Video'}`\n"
            f"**Index:** `{count}`"
        )
        try:
            prog_msg = await task.message.reply_text(msg_text)
        except Exception:
            prog_msg = await bot.send_message(task.chat_id, msg_text)

        final_filename = ""
        caption = f"**Title »** {name_clean}\n**Caption »** {batch_title}\n**Index »** {str(count).zfill(3)}"

        if cmd == "pdf":
             final_filename = await helper.download_file(url, name_clean)
             log.info("Download complete", task_id=task.id, filename=final_filename)
             await prog_msg.delete(True)
             try:
                 await task.message.reply_document(final_filename, caption=caption)
             except Exception:
                 await bot.send_document(task.chat_id, final_filename, caption=caption)
             log.info("Upload complete", task_id=task.id)
        else:
             final_filename = await helper.download_video(url, cmd, name_clean)
             from pathlib import Path
             if final_filename and Path(final_filename).exists():
                 log.info("Download complete", task_id=task.id, filename=final_filename)

                 # Helper logic uses message object. We construct a dummy if needed or patch helper
                 dummy_msg = task.message
                 if not hasattr(dummy_msg, 'chat') or dummy_msg.chat is None:
                     dummy_msg = Message(id=0, chat=await bot.get_chat(task.chat_id), date=None)
                     dummy_msg._client = bot

                 if is_doc:
                     await helper.send_doc(bot, dummy_msg, caption, final_filename, thumb_path, name_clean, prog_msg)
                 else:
                     await helper.send_vid(bot, dummy_msg, caption, final_filename, thumb_path, name_clean, prog_msg)
                 log.info("Upload complete", task_id=task.id)
             else:
                 log.error("Download failed", task_id=task.id, url=url)
                 try:
                     await task.message.reply_text(f"Download failed for {name_clean}")
                 except Exception:
                     await bot.send_message(task.chat_id, f"Download failed for {name_clean}")
                 await update_task_status(task.id, 'failed')
                 return

        await update_task_status(task.id, 'done')
        await asyncio.sleep(settings.task_delay_seconds)

    except Exception as e:
        log.error("Error processing link", task_id=task.id, error=str(e), url=url)
        try:
            await task.message.reply_text(f"Error on link {task.index}: {str(e)}")
        except Exception:
            await bot.send_message(task.chat_id, f"Error on link {task.index}: {str(e)}")
        await update_task_status(task.id, 'failed')

@bot.on_message(filters.command(["status"]))
@authorized_only
async def status_handler(bot: Client, m: Message):
    counts = await get_task_counts(m.chat.id)
    pending = counts.get('pending', 0)
    started = counts.get('started', 0)
    done = counts.get('done', 0)
    failed = counts.get('failed', 0)

    await m.reply_text(f"**Task Status:**\n"
                       f"Pending: {pending}\n"
                       f"Started: {started}\n"
                       f"Done: {done}\n"
                       f"Failed: {failed}")

@bot.on_message(filters.command(["retry"]))
@authorized_only
async def retry_handler(bot: Client, m: Message):
    tasks = await requeue_failed_tasks(m.chat.id)
    if not tasks:
        await m.reply_text("No failed tasks found.")
        return

    for row in tasks:
        payload = json.loads(row['payload'])
        task = DownloadTask(
            id=row['id'],
            name=row['name'],
            url=row['url'],
            caption=payload['caption'],
            resolution=payload['resolution'],
            is_doc=payload['is_doc'],
            thumb_path=payload['thumb_path'],
            index=payload['index'],
            chat_id=row['chat_id'],
            message=m, # use current msg context for replies
            is_jw=payload['is_jw']
        )
        task_queue.put_nowait(task)

    await m.reply_text(f"Re-queued {len(tasks)} failed tasks.")

@bot.on_message(filters.command(["start"]))
@authorized_only
async def start_handler(bot: Client, m: Message):
    await m.reply_text(
        "Hello! I am a txt file downloader.\n"
        "Press /pyro to download links listed in a txt file (Name:link).\n"
        "Press /jw for JWPlayer signed links.\n\n"
        "Bot made by BATMAN"
    )

@bot.on_message(filters.command(["cancel"]))
@authorized_only
async def cancel_handler(_, m):
    global cancel_process
    cancel_process = True
    await m.reply_text("Cancelling all processes. Please wait...")

@bot.on_message(filters.command("restart"))
@authorized_only
async def restart_handler(_, m):
    await m.reply_text("Restarted!", True)
    os.execl(sys.executable, sys.executable, *sys.argv)

@bot.on_message(filters.command(["pyro", "jw"]))
@authorized_only
async def batch_download_handler(bot: Client, m: Message):
    global cancel_process
    cancel_process = False

    is_jw = m.command[0] == "jw"

    # 1. Get the TXT file or Link
    editable = await m.reply_text("Please send the **txt file** (Name:Link) OR a **direct link**.")
    input_msg: Message = await bot.listen(editable.chat.id)

    links = []

    if input_msg.document and input_msg.document.file_name.endswith(".txt"):
         # Process TXT file
         file_path = await input_msg.download()
         await input_msg.delete(True)

         try:
             with open(file_path, "r", encoding="utf-8") as f:
                 content = f.read().splitlines()
             os.remove(file_path)

             for line in content:
                 if ":" in line:
                     links.append(line.split(":", 1))
         except Exception as e:
             await m.reply_text(f"Error reading file: {e}")
             return

    elif input_msg.text and input_msg.text.startswith(("http://", "https://")):
         # Process Single Link
         url = input_msg.text.strip()
         await m.reply_text("**Enter Name for this video:**")
         name_input: Message = await bot.listen(editable.chat.id)
         name = name_input.text.strip()
         links.append((name, url))

    else:
        await m.reply_text("Invalid input. Please send a .txt file or a valid link.")
        return

    if not links:
        await m.reply_text("No valid links found.")
        return

    # 2. Get User Inputs
    editable = await m.reply_text(f"Found **{len(links)}** links.\nEnter starting index (default 0):")
    input_start: Message = await bot.listen(editable.chat.id)
    try:
        start_index = int(input_start.text)
    except ValueError:
        start_index = 0

    await m.reply_text("**Enter Batch Title/Caption:**")
    input_title: Message = await bot.listen(editable.chat.id)
    batch_title = input_title.text

    session_state[m.chat.id] = {
        "links": links,
        "start_index": start_index,
        "batch_title": batch_title,
        "is_jw": is_jw
    }

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("360p", callback_data="res_360"), InlineKeyboardButton("480p", callback_data="res_480")],
        [InlineKeyboardButton("720p", callback_data="res_720"), InlineKeyboardButton("1080p", callback_data="res_1080")],
        [InlineKeyboardButton("Best", callback_data="res_best")]
    ])
    await m.reply_text("**Select Resolution:**", reply_markup=keyboard)

session_state = {}

@bot.on_callback_query(filters.regex(r"^res_"))
@authorized_only
async def resolution_callback(bot: Client, query: CallbackQuery):
    chat_id = query.message.chat.id
    if chat_id not in session_state:
        return await query.message.edit_text("Session expired. Start again.")

    res = query.data.split("_")[1]
    session_state[chat_id]["target_res"] = res

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Video", callback_data="mode_v"), InlineKeyboardButton("Document", callback_data="mode_d")]
    ])
    await query.message.edit_text("**Upload Mode:**", reply_markup=keyboard)

@bot.on_callback_query(filters.regex(r"^mode_"))
@authorized_only
async def mode_callback(bot: Client, query: CallbackQuery):
    chat_id = query.message.chat.id
    if chat_id not in session_state:
        return await query.message.edit_text("Session expired. Start again.")

    mode = query.data.split("_")[1]
    session_state[chat_id]["is_doc"] = mode == "d"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("No Thumbnail", callback_data="thumb_no")]
    ])
    await query.message.edit_text("**Send Thumb URL** (or click No Thumbnail):", reply_markup=keyboard)

    # Listen for thumb url text
    import pyromod.listen
    try:
        input_thumb: Message = await bot.listen(chat_id, timeout=300)
        if input_thumb and input_thumb.text:
            await handle_thumb_input(chat_id, input_thumb.text, query.message)
    except pyromod.listen.ListenerTimeout:
        pass
    except pyromod.listen.ListenerCanceled:
        pass

@bot.on_callback_query(filters.regex(r"^thumb_no$"))
@authorized_only
async def thumb_callback(bot: Client, query: CallbackQuery):
    chat_id = query.message.chat.id
    if chat_id not in session_state:
         return await query.message.edit_text("Session expired. Start again.")

    import pyromod.listen
    bot.stop_listening(chat_id=chat_id)

    await query.message.delete()
    await handle_thumb_input(chat_id, "no", query.message)

async def handle_thumb_input(chat_id, thumb_url, reply_msg):
    if chat_id not in session_state:
        return

    state = session_state.pop(chat_id)
    links = state["links"]
    start_index = state["start_index"]
    batch_title = state["batch_title"]
    is_jw = state["is_jw"]
    target_res = state["target_res"]
    is_doc = state["is_doc"]

    thumb_path = "no"
    if thumb_url.startswith(("http://", "https://")):
         thumb_path = settings.thumbnail_path
         proc = await asyncio.create_subprocess_exec("wget", thumb_url, "-O", thumb_path)
         await proc.wait()

    m = reply_msg # for replying

    from utils import safe_filename

    # 3. Queue Tasks
    count = start_index + 1 if start_index > 0 else 1

    queued = 0
    for i in range(start_index, len(links)):
        name_part, url_part = links[i]
        name_clean = safe_filename(name_part)
        url = url_part.strip()

        import uuid
        task_id = str(uuid.uuid4())

        task = DownloadTask(
            id=task_id,
            name=name_clean,
            url=url,
            caption=batch_title,
            resolution=target_res,
            is_doc=is_doc,
            thumb_path=thumb_path,
            index=count,
            chat_id=chat_id,
            message=reply_msg,
            is_jw=is_jw
        )

        payload = {
            "caption": batch_title,
            "resolution": target_res,
            "is_doc": is_doc,
            "thumb_path": thumb_path,
            "index": count,
            "is_jw": is_jw
        }

        await save_task(task_id, name_clean, url, 'pending', chat_id, payload)
        task_queue.put_nowait(task)
        log.info("Task queued", task_id=task_id, url=url)
        queued += 1
        count += 1

    await m.reply_text(f"Queued {queued} tasks.")

async def get_jw_signed_url(url):
    # This logic matches the original extraction
    import aiohttp

    headers = {
        'Host': 'api.classplusapp.com',
        'x-access-token': settings.jw_token,
        'user-agent': settings.jw_user_agent,
        'app-version': '1.4.37.1',
        'api-version': '18',
        'device-id': '5d0d17ac8b3c9f51',
        'device-details': '2848b866799971ca_2848b8667a33216c_SDK-30',
        'accept-encoding': 'gzip',
    }
    params = (('url', f'{url}'),)

    async with aiohttp.ClientSession() as session:
        async with session.get(settings.jw_api_url, headers=headers, params=params) as response:
            data = await response.json()
            jw_url = data['url']

        headers1 = {
            'User-Agent': 'ExoPlayerDemo/1.4.37.1 (Linux;Android 11) ExoPlayerLib/2.14.1',
            'Accept-Encoding': 'gzip',
            'Host': settings.jw_cdn_host,
            'Connection': 'Keep-Alive',
        }
        async with session.get(jw_url, headers=headers1) as response1:
            text = await response1.text()

            # Parsing the response text as typical for m3u8 or similar redirect
            try:
                return text.split("\n")[2]
            except IndexError:
                return jw_url

async def get_ytdlp_command(url, name, resolution, index):
    # Use yt-dlp's built-in format selection logic which is more robust
    # We ask for best video no larger than 'resolution' height

    try:
        height = int(resolution)
        # format: best video with height <= target + best audio, OR best (fallback)
        f_str = f"bestvideo[height<={height}]+bestaudio/best[height<={height}]/best"
    except ValueError:
        # If resolution is not an integer (e.g. "best"), just use best
        f_str = "bestvideo+bestaudio/best"

    # Check for cookies.txt
    cookies_arg = ""
    from pathlib import Path
    if Path("cookies.txt").is_file():
        cookies_arg = "--cookies cookies.txt"

    from utils import get_download_path
    out_path = get_download_path(f"{name}.%(ext)s")

    return f'yt-dlp {cookies_arg} -f "{f_str}" --merge-output-format mkv --no-keep-video --remux-video mkv "{url}" -o "{out_path}" -N 16 --fragment-retries 25'

async def start_bot():
    os.makedirs(settings.download_dir, exist_ok=True)
    await init_db()
    await bot.start()

    # Re-queue pending tasks
    pending_tasks = await get_pending_tasks()
    for row in pending_tasks:
        payload = json.loads(row['payload'])
        # A dummy message to satisfy the parameter - in a real app, you might want to fetch the real one or allow None
        dummy_msg = Message(id=0, chat=None, date=None)
        dummy_msg._client = bot

        task = DownloadTask(
            id=row['id'],
            name=row['name'],
            url=row['url'],
            caption=payload['caption'],
            resolution=payload['resolution'],
            is_doc=payload['is_doc'],
            thumb_path=payload['thumb_path'],
            index=payload['index'],
            chat_id=row['chat_id'],
            message=dummy_msg,
            is_jw=payload['is_jw']
        )
        task_queue.put_nowait(task)

    # Start workers
    workers = []
    for _ in range(settings.max_concurrent):
        workers.append(asyncio.create_task(worker()))

    import pyrogram
    await pyrogram.idle()

    for w in workers:
        w.cancel()
    await bot.stop()

if __name__ == "__main__":
    asyncio.run(start_bot())
