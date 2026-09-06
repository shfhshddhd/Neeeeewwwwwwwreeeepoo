"""Owner-scoped private Voice Chat control-group setup and commands."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from html import escape

from telethon.tl import types as tl_types
from telethon.utils import get_peer_id
from telegram import Update
from telegram.ext import ConversationHandler, ContextTypes, MessageHandler, filters

import database.mongo as db
from utils.message_ui import reply_html


def _voice_manager_for_owner(manager, owner_user_id: int):
    """Resolve the one VoiceChatManager attached to an owner's hosted session."""
    hosted = manager.get_client(owner_user_id) if manager is not None else None
    if hosted is None or not hosted.is_running():
        return None
    return getattr(hosted.client, "_voice_chat_manager", None)


GROUP_REFERENCE = 1
_SETUP_RE = re.compile(r"^\s*\.privategroupvcsetup\s*$", re.IGNORECASE)
_COMMAND_RE = re.compile(
    r"^\s*(?:/|\.)"
    r"(?P<command>join|leave|leaveall|leaveplay|leaverecord|level|bass|mute|"
    r"unmute|startrecord|stoprecord|speedtest)"
    r"(?:@[A-Za-z0-9_]+)?"
    r"(?:\s+(?P<args>.*?))?\s*$",
    re.IGNORECASE,
)

COMMAND_HELP = (
    "<b>Private VC commands</b>\n\n"
    "<code>/join &lt;group&gt;</code> — join an active Voice Chat\n"
    "<code>/leave</code> — leave the current Voice Chat\n"
    "<code>/leaveall</code> — leave and clear the current session\n"
    "<code>/leaveplay</code> — stop playback but stay in the call\n"
    "<code>/leaverecord</code> — stop and send the recording\n"
    "<code>/level 1-25</code> — set playback gain\n"
    "<code>/bass 0-15</code> — set bass for the next playback source\n"
    "<code>/mute</code> / <code>/unmute</code> — mute controls\n"
    "<code>/startrecord</code> / <code>/stoprecord</code> — recording controls\n"
    "<code>/speedtest</code> — measure the worker connection\n\n"
    "<i>These controls use your existing hosted Telegram session.\n"
    "Only the registered owner can use this group.\n"
    "Main-bot .vcjoin and .play remain available in the private bot chat.</i>"
)


def _hosted_for_user(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    manager = context.bot_data.get("manager")
    hosted = manager.get_client(user_id) if manager is not None else None
    if hosted is None or not hosted.is_running():
        return None
    return hosted


async def setup_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return ConversationHandler.END
    hosted = _hosted_for_user(context, user.id)
    if hosted is None:
        await reply_html(message, "❌ Host your Telegram account with /host first.")
        return ConversationHandler.END

    context.user_data["private_group_setup"] = {"owner_user_id": user.id}
    await reply_html(
        message,
        "<b>Private VC Control Group setup</b>\n\n"
        "Add this bot to your private group first, then send the group’s numeric "
        "chat ID or a public t.me group link. The hosted account must already be "
        "a group owner or administrator, and this bot must remain a member.\n\n"
        "Private invite links are not auto-joined for safety; use the numeric ID "
        "after the hosted account has joined the group.\n\n"
        "Send the group ID or link now, or /cancel to stop."
    )
    return GROUP_REFERENCE


async def _resolve_group(hosted, raw: str):
    value = raw.strip()
    if not value:
        raise ValueError("Send a Telegram group ID or public t.me group link.")
    if value.lstrip("-").isdigit():
        entity_ref = int(value)
    else:
        normalized = value
        normalized = re.sub(r"^https?://t\.me/", "", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"^t\.me/", "", normalized, flags=re.IGNORECASE)
        normalized = normalized.split("?", 1)[0].strip("/")
        if normalized.startswith("+") or normalized.startswith("joinchat/"):
            raise ValueError(
                "Private invite links are not imported automatically. Add the "
                "hosted account first, then send the group’s numeric chat ID."
            )
        entity_ref = normalized.split("/", 1)[0].lstrip("@")
        if not entity_ref:
            raise ValueError("That link does not contain a public group username.")

    entity = await hosted.client.get_entity(entity_ref)
    if not isinstance(entity, (tl_types.Chat, tl_types.Channel)):
        raise ValueError("The supplied chat is not a Telegram group.")
    if isinstance(entity, tl_types.Channel) and not getattr(entity, "megagroup", False):
        raise ValueError("Broadcast channels cannot be used as private VC control groups.")

    permissions = await hosted.client.get_permissions(entity, "me")
    if not (
        getattr(permissions, "is_creator", False)
        or getattr(permissions, "is_admin", False)
    ):
        raise ValueError(
            "The hosted Telegram account must be the group owner or an administrator."
        )
    return entity, int(get_peer_id(entity))


async def setup_reference(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return ConversationHandler.END
    hosted = _hosted_for_user(context, user.id)
    if hosted is None:
        await reply_html(message, "❌ The hosted session is no longer active. Run /host again.")
        context.user_data.pop("private_group_setup", None)
        return ConversationHandler.END

    try:
        entity, chat_id = await _resolve_group(hosted, message.text or "")
        bot = await context.bot.get_me()
        bot_member = await context.bot.get_chat_member(chat_id, bot.id)
        if bot_member.status not in {"member", "administrator", "creator"}:
            raise ValueError(
                "This bot is not an active member of that group. Add it to the "
                "group and run setup again."
            )
        existing = await db.get_private_control_group_by_chat(chat_id)
        if existing is not None and int(existing.get("owner_user_id", 0)) != user.id:
            raise ValueError("That group is already linked to another hosted owner.")
        hosted_account_id = int(getattr(hosted, "_own_id", 0) or 0)
        if not hosted_account_id:
            me = await hosted.client.get_me()
            hosted_account_id = int(me.id)
        title = str(getattr(entity, "title", None) or chat_id)
        await db.save_private_control_group(
            owner_user_id=user.id,
            hosted_account_id=hosted_account_id,
            private_control_group_id=chat_id,
            title=title,
        )
    except Exception as exc:
        await reply_html(message, f"❌ Setup failed: {escape(str(exc))}")
        return GROUP_REFERENCE

    context.user_data.pop("private_group_setup", None)
    await reply_html(
        message,
        "✅ <b>Private VC Control Group setup completed.</b>\n\n"
        f"Group: <b>{escape(title)}</b> (<code>{chat_id}</code>)\n"
        f"Owner ID: <code>{user.id}</code>\n\n"
        f"{COMMAND_HELP}"
    )
    return ConversationHandler.END


async def setup_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("private_group_setup", None)
    if update.effective_message is not None:
        await reply_html(update.effective_message, "Private VC group setup cancelled.")
    return ConversationHandler.END


def _active_chat_id(voice) -> int:
    if voice.state is None:
        raise RuntimeError("No active Voice Chat. Use /join <group> first.")
    return voice.state.chat_id


def _speedtest_sync() -> dict:
    import speedtest

    result = speedtest.Speedtest()
    result.get_best_server()
    download = result.download()
    upload = result.upload()
    return {
        "server": result.results.server.get("sponsor", "Unknown"),
        "isp": result.results.client.get("isp", "Unknown"),
        "ping": result.results.ping,
        "download": download / 1_000_000,
        "upload": upload / 1_000_000,
    }


async def private_group_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if message is None or user is None or chat is None:
        return
    match = _COMMAND_RE.match(message.text or "")
    if match is None:
        return

    mapping = await db.get_private_control_group_by_chat(int(chat.id))
    if mapping is None:
        return
    if int(mapping.get("owner_user_id", 0)) != user.id:
        await reply_html(message, "❌ Only the owner of this private VC group can use these controls.")
        return

    hosted = _hosted_for_user(context, user.id)
    if hosted is None:
        await db.deactivate_private_control_group_by_chat(int(chat.id))
        await reply_html(message, "❌ The owner’s hosted session is inactive; this group has been disabled.")
        return
    hosted_account_id = int(getattr(hosted, "_own_id", 0) or 0)
    if hosted_account_id and hosted_account_id != int(mapping.get("hosted_account_id", hosted_account_id)):
        await db.deactivate_private_control_group_by_chat(int(chat.id))
        await reply_html(message, "❌ The hosted session identity changed; run setup again.")
        return

    manager = context.bot_data.get("manager")
    voice = _voice_manager_for_owner(manager, user.id)
    if voice is None:
        await reply_html(message, "❌ The owner’s hosted Voice Chat manager is not running.")
        return

    command = match.group("command").lower()
    args = (match.group("args") or "").strip()
    try:
        if command == "join":
            if not args:
                raise ValueError("Usage: /join <group username or chat ID>")
            text = await voice.join_target(args)
        elif command == "leaveall":
            if voice.state is None:
                text = "ℹ️ No active Voice Chat sessions."
            else:
                text = await voice.leave(voice.state.chat_id)
        elif command == "leave":
            if voice.state is None:
                text = "ℹ️ Not connected to any Voice Chat."
            else:
                text = await voice.leave(voice.state.chat_id)
        elif command == "leaveplay":
            if voice.state is None:
                text = "ℹ️ No active Voice Chat sessions."
            else:
                text = await voice.stop(voice.state.chat_id)
        elif command in {"leaverecord", "stoprecord"}:
            if voice.state is None:
                text = "ℹ️ No active Voice Chat session."
            elif voice.state.recording_path is None:
                text = "ℹ️ No recording is currently in progress."
            else:
                text = await voice.stop_recording(message.chat.id, voice.state.chat_id)
        elif command == "level":
            if not args or not args.lstrip("+-").isdigit():
                text = (
                    "ℹ️ <b>Usage:</b> <code>/level &lt;1-25&gt;</code>\n"
                    "Example: <code>/level 10</code>"
                )
            else:
                value = int(args)
                if not 1 <= value <= 25:
                    text = "❌ Level must be between 1 and 25. Example: <code>/level 10</code>"
                elif voice.state is None:
                    text = "ℹ️ No active Voice Chat session to adjust level."
                else:
                    await voice.change_volume(voice.state.chat_id, value * 20)
                    text = f"🎚 Level set to {value}/25."
        elif command == "bass":
            if not args or not args.lstrip("+-").isdigit():
                text = (
                    "ℹ️ <b>Usage:</b> <code>/bass &lt;0-15&gt;</code>\n"
                    "Example: <code>/bass 5</code>"
                )
            else:
                value = int(args)
                if not 0 <= value <= 15:
                    text = "❌ Bass must be between 0 and 15. Example: <code>/bass 5</code>"
                elif voice.state is None:
                    text = "ℹ️ No active Voice Chat session to adjust bass."
                else:
                    text = await voice.set_bass(voice.state.chat_id, value)
        elif command == "mute":
            if voice.state is None:
                text = "ℹ️ No active Voice Chat session."
            else:
                text = await voice.mute(voice.state.chat_id)
        elif command == "unmute":
            if voice.state is None:
                text = "ℹ️ No active Voice Chat session."
            else:
                text = await voice.unmute(voice.state.chat_id)
        elif command == "startrecord":
            if voice.state is None:
                text = "ℹ️ No active Voice Chat session. Use /join <group> first."
            else:
                text = await voice.start_recording(voice.state.chat_id)
        elif command == "speedtest":
            result = await asyncio.to_thread(_speedtest_sync)
            text = (
                "📡 <b>Speed Test Results</b>\n\n"
                f"Server: <code>{escape(str(result['server']))}</code>\n"
                f"ISP: <code>{escape(str(result['isp']))}</code>\n"
                f"Ping: <code>{result['ping']:.2f} ms</code>\n"
                f"Download: <code>{result['download']:.2f} Mbps</code>\n"
                f"Upload: <code>{result['upload']:.2f} Mbps</code>"
            )
        else:
            return
        await reply_html(message, text)
    except Exception as exc:
        await reply_html(message, f"❌ {escape(str(exc))}")


def build_private_group_setup_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            MessageHandler(
                filters.ChatType.PRIVATE & filters.TEXT & filters.Regex(_SETUP_RE),
                setup_start,
            )
        ],
        states={
            GROUP_REFERENCE: [
                MessageHandler(
                    filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
                    setup_reference,
                )
            ]
        },
        fallbacks=[
            MessageHandler(
                filters.ChatType.PRIVATE & filters.Regex(r"^\s*/cancel(?:@\w+)?\s*$"),
                setup_cancel,
            )
        ],
        name="private_group_vc_setup",
        persistent=False,
    )


def build_private_group_control_handler() -> MessageHandler:
    return MessageHandler(
        filters.ChatType.GROUPS & filters.TEXT & filters.Regex(_COMMAND_RE),
        private_group_command,
    )
