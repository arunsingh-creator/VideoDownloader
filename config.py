from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List, Union

class Settings(BaseSettings):
    api_id: int
    api_hash: str
    bot_token: str
    jw_token: str
    allowed_users: List[int] = []

    @field_validator('allowed_users', mode='before')
    def parse_allowed_users(cls, v):
        if isinstance(v, str):
            if not v.strip():
                return []
            return [int(x.strip()) for x in v.split(",") if x.strip()]
        return v
    max_concurrent: int = 3
    download_dir: str = "/tmp/downloads"

    jw_api_url: str = "https://api.classplusapp.com/cams/uploader/video/jw-signed-url"
    jw_cdn_host: str = "cdn.jwplayer.com"
    jw_user_agent: str = "Mobile-Android"

    thumbnail_path: str = "thumb.jpg"
    task_delay_seconds: int = 2
    session_name: str = "bot"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
