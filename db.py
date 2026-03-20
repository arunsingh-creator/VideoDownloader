import aiosqlite
from datetime import datetime
import json
import structlog

log = structlog.get_logger(__name__)
DB_NAME = "tasks.db"

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT,
                url TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT,
                chat_id INTEGER,
                payload TEXT
            )
        ''')
        await db.commit()

async def save_task(task_id: str, name: str, url: str, status: str, chat_id: int, payload: dict):
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT INTO tasks (id, name, url, status, created_at, updated_at, chat_id, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, name, url, status, now, now, chat_id, json.dumps(payload)))
        await db.commit()

async def update_task_status(task_id: str, status: str):
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?
        ''', (status, now, task_id))
        await db.commit()

async def get_pending_tasks():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE status = 'pending'") as cursor:
            return await cursor.fetchall()

async def get_task_counts(chat_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute('''
            SELECT status, COUNT(*) FROM tasks
            WHERE chat_id = ? GROUP BY status
        ''', (chat_id,)) as cursor:
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}

async def requeue_failed_tasks(chat_id: int):
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE status = 'failed' AND chat_id = ?", (chat_id,)) as cursor:
            tasks = await cursor.fetchall()

        if tasks:
            await db.execute('''
                UPDATE tasks SET status = 'pending', updated_at = ?
                WHERE status = 'failed' AND chat_id = ?
            ''', (now, chat_id))
            await db.commit()

        return tasks
