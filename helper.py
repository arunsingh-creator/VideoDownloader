import subprocess
import datetime
import asyncio
import os
import time
import aiohttp
import aiofiles
import structlog
from p_bar import progress_bar

log = structlog.get_logger(__name__)

def duration(filename):
    try:
        result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                                 "format=duration", "-of",
                                 "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        return float(result.stdout)
    except Exception as e:
        log.error("Error getting duration", error=str(e), filename=filename)
        return 0.0

from utils import get_download_path

async def download_file(url, name):
    k = get_download_path(f'{name}.pdf')
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                async with aiofiles.open(k, mode='wb') as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        await f.write(chunk)
    return str(k)

def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = []
    
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i = i.strip()
            parts = i.split("|")[0].split(" ", 3)
            
            try:
                # parts[0] is usually ID/format code, parts[2] is resolution
                if len(parts) > 2:
                    res = parts[2]
                    fmt_code = parts[0]
                    
                    if "RESOLUTION" not in res and res not in temp and "audio" not in res:
                        temp.append(res)
                        new_info[res] = fmt_code
            except Exception as e:
                log.error("Error parsing video info line", error=str(e), line=i)
                pass
                
    return new_info

async def run(cmd):
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await proc.communicate()
        
        if proc.returncode == 0:
            return stdout.decode() if stdout else ""
        else:
            log.error("Command failed", cmd=cmd, returncode=proc.returncode)
            return False
    except Exception as e:
        log.error("Error running command", cmd=cmd, error=str(e))
        return False

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"

def time_name():
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H%M%S")
    return f"{date} {current_time}.mp4"

async def download_video(url, cmd, name):
    # Remove aria2c to prevent file locking issues on Windows
    download_cmd = f"{cmd} -R 25 --fragment-retries 25"
    log.info("Downloading video", cmd=download_cmd, url=url)
    
    # Run the download command
    process = await asyncio.create_subprocess_shell(download_cmd)
    await process.wait()

    try:
        from pathlib import Path
        out_path = get_download_path(name)
        base_path = out_path.with_suffix('') if out_path.suffix else out_path

        if out_path.is_file():
            return str(out_path)

        webm_path = Path(f"{out_path}.webm")
        if webm_path.is_file():
            return str(webm_path)
        
        # Check for other extensions
        extensions = [".mkv", ".mp4", ".mp4.webm"]
        
        for ext in extensions:
            ext_path = Path(f"{base_path}{ext}")
            if ext_path.is_file():
                return str(ext_path)

        # If not found but command succeeded, it might just be name.mp4 in download path
        default_fallback = get_download_path(f"{name}.mp4")
        if default_fallback.is_file():
            return str(default_fallback)

        return str(out_path)
    except Exception as e:
        log.error("Error finding downloaded file", error=str(e), name=name)
        return str(get_download_path(f"{name}.mp4"))

async def send_vid(bot, m, cc, filename, thumb, name, prog):
    
    from pathlib import Path

    # Generate thumbnail if needed
    thumb_gen = f"{filename}.jpg"
    if not Path(thumb_gen).is_file():
        cmd = f'ffmpeg -i "{filename}" -ss 00:01:00 -vframes 1 "{thumb_gen}"'
        await run(cmd)
    
    await prog.delete(revoke=True)
    reply = await m.reply_text(f"**Uploading ...** - `{name}`")
    
    try:
        thumbnail = thumb if thumb != "no" else thumb_gen
        if not Path(thumbnail).is_file():
             thumbnail = None
    except Exception:
        thumbnail = None

    dur = int(duration(filename))
    start_time = time.time()

    try:
        await m.reply_video(
            filename,
            caption=cc,
            supports_streaming=True,
            height=720,
            width=1280,
            thumb=thumbnail,
            duration=dur,
            progress=progress_bar,
            progress_args=(reply, start_time)
        )
    except Exception as e:
        log.warning("Video upload failed, trying as document", error=str(e), filename=filename)
        await m.reply_document(
            filename,
            caption=cc,
            progress=progress_bar,
            progress_args=(reply, start_time)
        )

    from pathlib import Path

    # Cleanup
    if Path(filename).exists():
        Path(filename).unlink()
    
    thumb_gen = f"{filename}.jpg"
    if Path(thumb_gen).exists():
        Path(thumb_gen).unlink()
        
    await reply.delete(revoke=True)

async def send_doc(bot, m, cc, filename, thumb, name, prog):
    from pathlib import Path

    # Generate thumbnail if needed (for document thumb)
    thumb_gen = f"{filename}.jpg"
    if not Path(thumb_gen).is_file():
        cmd = f'ffmpeg -i "{filename}" -ss 00:01:00 -vframes 1 "{thumb_gen}"'
        await run(cmd)
    
    await prog.delete(revoke=True)
    reply = await m.reply_text(f"**Uploading as Doc ...** - `{name}`")
    
    try:
        thumbnail = thumb if thumb != "no" else thumb_gen
        if not Path(thumbnail).is_file():
             thumbnail = None
    except Exception:
        thumbnail = None

    start_time = time.time()

    try:
        await m.reply_document(
            filename,
            caption=cc,
            thumb=thumbnail,
            progress=progress_bar,
            progress_args=(reply, start_time)
        )
    except Exception as e:
        log.error("Deep document upload failed", error=str(e), filename=filename)
        await m.reply_text(f"Upload failed: {e}")

    from pathlib import Path

    # Cleanup
    if Path(filename).exists():
        Path(filename).unlink()
    
    thumb_gen = f"{filename}.jpg"
    if Path(thumb_gen).exists():
        Path(thumb_gen).unlink()
        
    await reply.delete(revoke=True)
