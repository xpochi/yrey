#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pasha Banner Manager — Telethon + SQLite

A per-group announcement scheduler for groups where the logged-in account
has administrator rights. It never joins groups, scans dialogs for targets,
or sends unsolicited messages to groups you have not configured.

Commands (send from the same Telegram account running this script):

Saved Messages:
  Reply to a message/media and send:  .تنظیم بنر 1
  List saved banners:                  .بنرها
  Delete an unused banner:             .حذف بنر 1
  Show help:                           .راهنما

Inside a group where you are an admin:
  .تنظیم بنر 1 21600    -> schedule banner 1 every 21600 seconds (6 hours)
  .ارسال بنر 1          -> send banner 1 once
  .وضعیت بنر            -> show this group's schedule
  .توقف بنر             -> stop this group's schedule

Safety guardrail: recurring announcements have a 6-hour minimum interval.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Get these values from https://my.telegram.org/apps
# Better: set environment variables TELEGRAM_API_ID and TELEGRAM_API_HASH.
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")

SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "pasha_banner_session")
DB_PATH = Path(__file__).with_name("pasha_banner_manager.sqlite3")
LOG_PATH = Path(__file__).with_name("pasha_banner_manager.log")

MIN_INTERVAL_SECONDS = 6 * 60 * 60       # 6 hours; prevents rapid repeat posting
MAX_INTERVAL_SECONDS = 30 * 24 * 60 * 60  # 30 days
SCHEDULER_TICK_SECONDS = 10

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("pasha-banner-manager")

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS banners (
                banner_no INTEGER PRIMARY KEY,
                source_message_id INTEGER NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                banner_no INTEGER NOT NULL,
                interval_seconds INTEGER NOT NULL,
                next_run_at REAL NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                updated_at REAL NOT NULL,
                FOREIGN KEY (banner_no) REFERENCES banners(banner_no)
            )
            """
        )
        conn.commit()


def save_banner(banner_no: int, source_message_id: int) -> None:
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO banners(banner_no, source_message_id, created_at) VALUES (?, ?, ?) "
            "ON CONFLICT(banner_no) DO UPDATE SET "
            "source_message_id=excluded.source_message_id, created_at=excluded.created_at",
            (banner_no, source_message_id, time.time()),
        )
        conn.commit()


def get_banner(banner_no: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute("SELECT * FROM banners WHERE banner_no=?", (banner_no,)).fetchone()


def get_banners() -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute("SELECT * FROM banners ORDER BY banner_no").fetchall()


def delete_banner(banner_no: int) -> tuple[bool, int]:
    with db_connect() as conn:
        # Any schedule row keeps a foreign-key reference, even when disabled.
        # Require the user to remove the schedule before deleting its banner.
        used = conn.execute(
            "SELECT COUNT(*) AS n FROM schedules WHERE banner_no=?",
            (banner_no,),
        ).fetchone()["n"]
        if used:
            return False, int(used)
        cur = conn.execute("DELETE FROM banners WHERE banner_no=?", (banner_no,))
        conn.commit()
        return cur.rowcount > 0, 0


def upsert_schedule(chat_id: int, chat_title: str, banner_no: int, interval_seconds: int) -> None:
    now = time.time()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO schedules(chat_id, chat_title, banner_no, interval_seconds,
                                  next_run_at, enabled, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title=excluded.chat_title,
                banner_no=excluded.banner_no,
                interval_seconds=excluded.interval_seconds,
                next_run_at=excluded.next_run_at,
                enabled=1,
                updated_at=excluded.updated_at
            """,
            (chat_id, chat_title, banner_no, interval_seconds, now + interval_seconds, now),
        )
        conn.commit()


def get_schedule(chat_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute("SELECT * FROM schedules WHERE chat_id=?", (chat_id,)).fetchone()


def stop_schedule(chat_id: int) -> bool:
    with db_connect() as conn:
        cur = conn.execute(
            "UPDATE schedules SET enabled=0, updated_at=? WHERE chat_id=? AND enabled=1",
            (time.time(), chat_id),
        )
        conn.commit()
        return cur.rowcount > 0


def delete_schedule(chat_id: int) -> bool:
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM schedules WHERE chat_id=?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0


def due_schedules(now: float) -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM schedules WHERE enabled=1 AND next_run_at<=? ORDER BY next_run_at",
            (now,),
        ).fetchall()


def schedule_next_run(chat_id: int, next_run_at: float) -> None:
    with db_connect() as conn:
        conn.execute(
            "UPDATE schedules SET next_run_at=?, updated_at=? WHERE chat_id=? AND enabled=1",
            (next_run_at, time.time(), chat_id),
        )
        conn.commit()

# ---------------------------------------------------------------------------
# TELEGRAM HELPERS
# ---------------------------------------------------------------------------

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
ME_ID: Optional[int] = None


def parse_positive_int(value: str) -> Optional[int]:
    try:
        number = int(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def format_duration(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400} روز"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} ساعت"
    if seconds % 60 == 0:
        return f"{seconds // 60} دقیقه"
    return f"{seconds} ثانیه"


async def is_admin_in_current_group(event: events.NewMessage.Event) -> bool:
    """Only allow configuration/one-off sends from an admin of this group."""
    try:
        permissions = await client.get_permissions(event.chat_id, "me")
        return bool(getattr(permissions, "is_admin", False) or getattr(permissions, "is_creator", False))
    except (RPCError, ValueError) as exc:
        log.warning("Could not verify admin rights in chat %s: %s", event.chat_id, exc)
        return False


async def send_saved_banner(chat_id: int, banner_no: int) -> None:
    banner = get_banner(banner_no)
    if not banner:
        raise ValueError(f"بنر شماره {banner_no} ثبت نشده است.")
    # Forward the exact message from Saved Messages, preserving text/media.
    await client.forward_messages(chat_id, int(banner["source_message_id"]), "me")


HELP_TEXT = (
    "🔥 راهنمای Pasha Banner Manager\n\n"
    "در Saved Messages، روی پیام یا رسانه ریپلای کن و بفرست:\n"
    ".تنظیم بنر 1\n\n"
    "برای دیدن بنرهای ثبت‌شده در Saved Messages:\n"
    ".بنرها\n\n"
    "برای حذف بنر استفاده‌نشده:\n"
    ".حذف بنر 1\n\n"
    "داخل گروهی که در آن ادمین هستی:\n"
    ".تنظیم بنر 1 21600\n"
    "عدد آخر برحسب ثانیه است؛ حداقل فاصله زمان‌بندی ۲۱۶۰۰ ثانیه (۶ ساعت) است.\n\n"
    "ارسال یک‌باره:\n"
    ".ارسال بنر 1\n\n"
    "وضعیت همین گروه: .وضعیت بنر\n"
    "توقف همین گروه: .توقف بنر\n"
    "پاک کردن تنظیمات همین گروه: .حذف تنظیمات بنر\n\n"
    "این برنامه خودش به هیچ گروهی عضو نمی‌شود و فقط در گروه‌هایی کار می‌کند که تو در آن‌ها ادمین هستی."
)

# ---------------------------------------------------------------------------
# COMMAND HANDLER
# ---------------------------------------------------------------------------

@client.on(events.NewMessage(outgoing=True))
async def command_handler(event: events.NewMessage.Event) -> None:
    raw = (event.raw_text or "").strip()
    if not raw.startswith("."):
        return

    # Saved Messages commands (the chat ID is the logged-in account's own ID).
    if event.chat_id == ME_ID:
        match = re.fullmatch(r"\.تنظیم\s+بنر\s+(\d+)", raw)
        if match:
            banner_no = parse_positive_int(match.group(1))
            if banner_no is None:
                await event.reply("شماره بنر باید عددی بزرگ‌تر از صفر باشد.")
                return
            reply = await event.get_reply_message()
            if reply is None:
                await event.reply("برای ثبت بنر، دستور را حتماً روی پیام یا رسانه موردنظر ریپلای کن.")
                return
            if reply.id == event.id:
                await event.reply("روی خود دستور ریپلای نکن؛ روی پیام اصلی بنر ریپلای کن.")
                return
            save_banner(banner_no, reply.id)
            await event.reply(f"✅ بنر شماره {banner_no} در Saved Messages ذخیره شد.")
            log.info("Saved banner #%s from message %s", banner_no, reply.id)
            return

        if raw == ".بنرها":
            banners = get_banners()
            if not banners:
                await event.reply("هنوز بنری ثبت نشده است. روی پیام موردنظر ریپلای کن و .تنظیم بنر 1 را بفرست.")
                return
            lines = ["🗂 بنرهای ثبت‌شده:"]
            for banner in banners:
                lines.append(f"• بنر {banner['banner_no']} — شناسه پیام {banner['source_message_id']}")
            await event.reply("\n".join(lines))
            return

        match = re.fullmatch(r"\.حذف\s+بنر\s+(\d+)", raw)
        if match:
            banner_no = parse_positive_int(match.group(1))
            if banner_no is None:
                await event.reply("شماره بنر نامعتبر است.")
                return
            deleted, used = delete_banner(banner_no)
            if used:
                await event.reply("⛔ این بنر در یک یا چند گروه فعال است؛ ابتدا تنظیمات آن گروه‌ها را پاک کن یا شماره بنر دیگری انتخاب کن.")
            elif deleted:
                await event.reply(f"🗑 بنر شماره {banner_no} حذف شد.")
            else:
                await event.reply("چنین بنری ثبت نشده است.")
            return

        if raw == ".راهنما":
            await event.reply(HELP_TEXT)
            return

        return

    # Group commands only. No auto-joining, dialog scanning, or private-chat sending.
    if not event.is_group:
        return

    match = re.fullmatch(r"\.تنظیم\s+بنر\s+(\d+)\s+(\d+)", raw)
    if match:
        banner_no = parse_positive_int(match.group(1))
        interval = parse_positive_int(match.group(2))
        if banner_no is None or interval is None:
            await event.reply("فرمت دستور درست نیست. نمونه: .تنظیم بنر 1 21600")
            return
        if not await is_admin_in_current_group(event):
            await event.reply("⛔ برای تنظیم ارسال زمان‌بندی‌شده باید در همین گروه ادمین باشی.")
            return
        banner = get_banner(banner_no)
        if not banner:
            await event.reply(f"بنر شماره {banner_no} در Saved Messages ثبت نشده است.")
            return
        if interval < MIN_INTERVAL_SECONDS:
            await event.reply(
                "⛔ فاصله‌های خیلی کوتاه برای ارسال تکراری می‌توانند اسپم محسوب شوند.\n"
                "حداقل فاصله مجاز این نسخه ۲۱۶۰۰ ثانیه (۶ ساعت) است."
            )
            return
        if interval > MAX_INTERVAL_SECONDS:
            await event.reply("حداکثر فاصله مجاز ۳۰ روز است.")
            return
        chat = await event.get_chat()
        title = getattr(chat, "title", None) or str(event.chat_id)
        upsert_schedule(event.chat_id, title, banner_no, interval)
        await event.reply(
            f"✅ زمان‌بندی فقط برای همین گروه ثبت شد.\n"
            f"بنر: {banner_no}\nفاصله: هر {format_duration(interval)}\n"
            "اولین ارسال در پایان همین فاصله انجام می‌شود.\n"
            "برای توقف: .توقف بنر"
        )
        log.info("Schedule enabled: chat=%s banner=%s interval=%s", event.chat_id, banner_no, interval)
        return

    match = re.fullmatch(r"\.ارسال\s+بنر\s+(\d+)", raw)
    if match:
        banner_no = parse_positive_int(match.group(1))
        if banner_no is None:
            await event.reply("شماره بنر نامعتبر است.")
            return
        if not await is_admin_in_current_group(event):
            await event.reply("⛔ فقط ادمین همین گروه می‌تواند بنر را ارسال کند.")
            return
        try:
            await send_saved_banner(event.chat_id, banner_no)
            await event.reply(f"✅ بنر {banner_no} یک‌بار در همین گروه ارسال شد.")
        except (ValueError, RPCError) as exc:
            await event.reply(f"❌ ارسال انجام نشد: {exc}")
            log.exception("Manual banner send failed")
        return

    if raw == ".وضعیت بنر":
        row = get_schedule(event.chat_id)
        if not row or not row["enabled"]:
            await event.reply("ℹ️ برای این گروه زمان‌بندی فعالی ثبت نشده است.")
            return
        remaining = max(0, int(row["next_run_at"] - time.time()))
        await event.reply(
            f"📊 وضعیت بنر همین گروه\n"
            f"بنر: {row['banner_no']}\n"
            f"فاصله: {format_duration(int(row['interval_seconds']))}\n"
            f"ارسال بعدی حدوداً تا {format_duration(remaining)} دیگر"
        )
        return

    if raw == ".توقف بنر":
        if not await is_admin_in_current_group(event):
            await event.reply("⛔ برای توقف زمان‌بندی باید در همین گروه ادمین باشی.")
            return
        if stop_schedule(event.chat_id):
            await event.reply("🛑 زمان‌بندی بنر فقط برای همین گروه متوقف شد.")
            log.info("Schedule stopped for chat %s", event.chat_id)
        else:
            await event.reply("برای این گروه زمان‌بندی فعالی وجود ندارد.")
        return

    if raw == ".حذف تنظیمات بنر":
        if not await is_admin_in_current_group(event):
            await event.reply("⛔ برای حذف تنظیمات باید در همین گروه ادمین باشی.")
            return
        if delete_schedule(event.chat_id):
            await event.reply("🗑 تنظیمات بنر فقط برای همین گروه پاک شد.")
            log.info("Schedule configuration deleted for chat %s", event.chat_id)
        else:
            await event.reply("برای این گروه تنظیماتی ثبت نشده است.")
        return

    if raw == ".راهنما":
        await event.reply(HELP_TEXT)

# ---------------------------------------------------------------------------
# SCHEDULER
# ---------------------------------------------------------------------------

async def scheduler_loop() -> None:
    """Send due banners only to explicitly configured, admin-managed groups."""
    while True:
        now = time.time()
        for row in due_schedules(now):
            chat_id = int(row["chat_id"])
            banner_no = int(row["banner_no"])
            interval = int(row["interval_seconds"])
            try:
                # Re-check that the account still has admin rights before each send.
                permissions = await client.get_permissions(chat_id, "me")
                is_admin = bool(
                    getattr(permissions, "is_admin", False)
                    or getattr(permissions, "is_creator", False)
                )
                if not is_admin:
                    stop_schedule(chat_id)
                    log.warning("Disabled schedule: admin rights lost in chat %s", chat_id)
                    continue

                await send_saved_banner(chat_id, banner_no)
                schedule_next_run(chat_id, time.time() + interval)
                log.info("Sent banner #%s to configured chat %s", banner_no, chat_id)

            except FloodWaitError as exc:
                # Do not retry rapidly after Telegram asks us to wait.
                delay = max(interval, int(exc.seconds) + 60)
                schedule_next_run(chat_id, time.time() + delay)
                log.warning("Telegram FloodWait in chat %s; next attempt delayed %s sec", chat_id, delay)

            except (RPCError, ValueError) as exc:
                # Stop the schedule on a permanent/unknown send problem instead of looping.
                stop_schedule(chat_id)
                log.exception("Disabled schedule for chat %s after send failure: %s", chat_id, exc)

            except Exception as exc:
                # Fail closed: disable this group's schedule if an unexpected error occurs.
                stop_schedule(chat_id)
                log.exception("Unexpected scheduler error for chat %s: %s", chat_id, exc)

        await asyncio.sleep(SCHEDULER_TICK_SECONDS)

# ---------------------------------------------------------------------------
# STARTUP
# ---------------------------------------------------------------------------

async def main() -> None:
    global ME_ID

    if API_ID <= 0 or not API_HASH:
        print("\nتنظیمات API کامل نیست.")
        print("TELEGRAM_API_ID و TELEGRAM_API_HASH را در Environment Variables قرار بده،")
        print("یا مقدار API_ID و API_HASH را ابتدای فایل وارد کن.\n")
        raise SystemExit(2)

    init_db()
    await client.start()
    me = await client.get_me()
    ME_ID = int(me.id)

    log.info("Connected as %s (id=%s)", getattr(me, "username", None) or me.first_name, ME_ID)
    await client.send_message(
        "me",
        "🔥 Pasha Banner Manager فعال شد.\n"
        "برای راهنما بنویس: .راهنما\n"
        "یادآوری: زمان‌بندی فقط در گروه‌هایی فعال می‌شود که در آن‌ها ادمین هستی؛ "
        "حداقل فاصله ارسال تکراری ۶ ساعت است."
    )

    scheduler_task = asyncio.create_task(scheduler_loop())
    try:
        await client.run_until_disconnected()
    finally:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nبرنامه متوقف شد.")
