import re
from pathlib import Path
from config import settings
import structlog

log = structlog.get_logger(__name__)

def safe_filename(name: str) -> str:
    """Sanitize the filename by removing invalid characters and truncating."""
    clean_name = re.sub(r'[^\w\s\-.]', '', name).strip()
    return clean_name[:200]

def get_download_path(filename: str) -> Path:
    """Get the full path in the download directory."""
    return Path(settings.download_dir) / safe_filename(filename)
