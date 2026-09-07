"""Tests for Private VC Control Group database mapping and setup flow."""

import asyncio
import logging
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure target_repo and target_repo/telegram_userbot are on sys.path
BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR / "telegram_userbot"
sys.path.insert(0, str(REPO_DIR))
sys.path.insert(0, str(BASE_DIR))

# Ensure mock telegram and telethon modules exist if not installed in current environment
if "telegram" not in sys.modules:
    try:
        import telegram
        import telegram.ext
    except ModuleNotFoundError:
        telegram_mod = types.ModuleType("telegram")
        telegram_mod.Update = MagicMock()
        telegram_mod.BotCommand = MagicMock()
        telegram_mod.InlineKeyboardButton = MagicMock()
        telegram_mod.InlineKeyboardMarkup = MagicMock()
        telegram_mod.WebAppInfo = MagicMock()
        telegram_ext_mod = types.ModuleType("telegram.ext")
        telegram_ext_mod.Application = MagicMock()
        telegram_ext_mod.ApplicationBuilder = MagicMock()
        telegram_ext_mod.CallbackQueryHandler = MagicMock()
        telegram_ext_mod.CommandHandler = MagicMock()
        telegram_ext_mod.ConversationHandler = MagicMock()
        telegram_ext_mod.ConversationHandler.END = -1
        telegram_ext_mod.ContextTypes = MagicMock()
        telegram_ext_mod.MessageHandler = MagicMock()
        telegram_ext_mod.filters = MagicMock()
        sys.modules["telegram"] = telegram_mod
        sys.modules["telegram.ext"] = telegram_ext_mod

if "telethon" not in sys.modules:
    try:
        import telethon
        import telethon.tl.types
        import telethon.utils
        import telethon.errors
        import telethon.functions
    except ModuleNotFoundError:
        telethon_mod = types.ModuleType("telethon")
        telethon_tl_mod = types.ModuleType("telethon.tl")
        telethon_tl_types_mod = types.ModuleType("telethon.tl.types")
        telethon_tl_types_mod.Chat = type("Chat", (), {})
        telethon_tl_types_mod.Channel = type("Channel", (), {})
        telethon_utils_mod = types.ModuleType("telethon.utils")
        telethon_utils_mod.get_peer_id = lambda entity: getattr(entity, "id", 0)
        telethon_errors_mod = types.ModuleType("telethon.errors")
        telethon_errors_mod.SessionPasswordNeededError = type("SessionPasswordNeededError", (Exception,), {})
        telethon_errors_mod.PhoneCodeInvalidError = type("PhoneCodeInvalidError", (Exception,), {})
        telethon_events_mod = types.ModuleType("telethon.events")
        telethon_functions_mod = types.ModuleType("telethon.functions")
        telethon_mod.TelegramClient = MagicMock()
        telethon_mod.events = telethon_events_mod
        telethon_mod.tl = telethon_tl_mod
        telethon_mod.utils = telethon_utils_mod
        telethon_mod.errors = telethon_errors_mod
        telethon_mod.functions = telethon_functions_mod
        sys.modules["telethon"] = telethon_mod
        sys.modules["telethon.tl"] = telethon_tl_mod
        sys.modules["telethon.tl.types"] = telethon_tl_types_mod
        sys.modules["telethon.utils"] = telethon_utils_mod
        sys.modules["telethon.errors"] = telethon_errors_mod
        sys.modules["telethon.events"] = telethon_events_mod
        sys.modules["telethon.functions"] = telethon_functions_mod

if "google" not in sys.modules:
    try:
        import google.generativeai
    except ModuleNotFoundError:
        google_mod = types.ModuleType("google")
        genai_mod = types.ModuleType("google.generativeai")
        google_mod.generativeai = genai_mod
        sys.modules["google"] = google_mod
        sys.modules["google.generativeai"] = genai_mod

if "pytgcalls" not in sys.modules:
    try:
        import pytgcalls
        import pytgcalls.exceptions
        import pytgcalls.types
        import pytgcalls.types.raw
        import pytgcalls.pytgcalls_session
    except (ModuleNotFoundError, ImportError):
        pytgcalls_mod = types.ModuleType("pytgcalls")
        pytgcalls_exc_mod = types.ModuleType("pytgcalls.exceptions")
        pytgcalls_exc_mod.NoActiveGroupCall = type("NoActiveGroupCall", (Exception,), {})
        pytgcalls_exc_mod.NodeJSNotInstalled = type("NodeJSNotInstalled", (Exception,), {})
        pytgcalls_types_mod = types.ModuleType("pytgcalls.types")
        pytgcalls_types_raw_mod = types.ModuleType("pytgcalls.types.raw")
        pytgcalls_types_raw_mod.AudioParameters = MagicMock()
        for type_name in ("AudioQuality", "Device", "ExternalMedia", "MediaStream", "RecordStream", "StreamEnded", "StreamFrames"):
            setattr(pytgcalls_types_mod, type_name, MagicMock())
        pytgcalls_types_mod.raw = pytgcalls_types_raw_mod
        pytgcalls_session_mod = types.ModuleType("pytgcalls.pytgcalls_session")
        pytgcalls_session_mod.PyTgCallsSession = MagicMock()
        pytgcalls_mod.exceptions = pytgcalls_exc_mod
        pytgcalls_mod.types = pytgcalls_types_mod
        pytgcalls_mod.pytgcalls_session = pytgcalls_session_mod
        pytgcalls_mod.PyTgCalls = MagicMock()
        sys.modules["pytgcalls"] = pytgcalls_mod
        sys.modules["pytgcalls.exceptions"] = pytgcalls_exc_mod
        sys.modules["pytgcalls.types"] = pytgcalls_types_mod
        sys.modules["pytgcalls.types.raw"] = pytgcalls_types_raw_mod
        sys.modules["pytgcalls.pytgcalls_session"] = pytgcalls_session_mod

if "requests" not in sys.modules:
    try:
        import requests
    except ModuleNotFoundError:
        requests_mod = types.ModuleType("requests")
        sys.modules["requests"] = requests_mod

if "aiohttp" not in sys.modules:
    try:
        import aiohttp
        import aiohttp.web
    except ModuleNotFoundError:
        aiohttp_mod = types.ModuleType("aiohttp")
        aiohttp_web_mod = types.ModuleType("aiohttp.web")
        aiohttp_web_mod.Request = MagicMock
        aiohttp_web_mod.Response = MagicMock
        aiohttp_web_mod.StreamResponse = MagicMock
        aiohttp_web_mod.Application = MagicMock
        aiohttp_web_mod.AppRunner = MagicMock
        aiohttp_web_mod.TCPSite = MagicMock
        aiohttp_mod.web = aiohttp_web_mod
        sys.modules["aiohttp"] = aiohttp_mod
        sys.modules["aiohttp.web"] = aiohttp_web_mod

import config
sys.modules["config.config"] = config

import database.mongo as db


class TestPrivateControlGroupLocalDB(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Force local fallback database to test local fallback storage
        self.tmp_fallback = BASE_DIR / "DB" / "test_mongo_fallback.json"
        if self.tmp_fallback.exists():
            self.tmp_fallback.unlink()
        db._db = db._LocalDatabase(self.tmp_fallback)

    async def asyncTearDown(self):
        if self.tmp_fallback.exists():
            self.tmp_fallback.unlink()

    async def test_save_and_retrieve_private_control_group(self):
        """Test that a private group mapping can be saved and retrieved by chat ID."""
        saved = await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100111222333,
            title="My Private VC Group",
        )
        self.assertEqual(saved["owner_user_id"], 12345)
        self.assertEqual(saved["hosted_account_id"], 67890)
        self.assertEqual(saved["private_control_group_id"], -100111222333)
        self.assertEqual(saved["title"], "My Private VC Group")
        self.assertTrue(saved["active"])
        self.assertIn("created_at", saved)
        self.assertIn("updated_at", saved)

        # Retrieve by chat ID
        retrieved = await db.get_private_control_group_by_chat(-100111222333)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["owner_user_id"], 12345)
        self.assertEqual(retrieved["hosted_account_id"], 67890)
        self.assertEqual(retrieved["private_control_group_id"], -100111222333)
        self.assertEqual(retrieved["title"], "My Private VC Group")
        self.assertTrue(retrieved["active"])

    async def test_deactivation_and_inactive_not_returned(self):
        """Test that deactivation works and inactive mappings are not returned."""
        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100111222333,
            title="My Private VC Group",
        )

        # Deactivate
        deactivated = await db.deactivate_private_control_group_by_chat(-100111222333)
        self.assertTrue(deactivated)

        # get_private_control_group_by_chat must return None for inactive mappings
        retrieved = await db.get_private_control_group_by_chat(-100111222333)
        self.assertIsNone(retrieved)

    async def test_owner_uniqueness_enforced(self):
        """Test that one owner can only have one active private control group at a time."""
        # Owner sets group 1
        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100111111111,
            title="Group One",
        )
        group1 = await db.get_private_control_group_by_chat(-100111111111)
        self.assertIsNotNone(group1)
        self.assertTrue(group1["active"])

        # Same owner sets group 2
        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100222222222,
            title="Group Two",
        )

        # Group 1 must now be inactive and therefore NOT returned
        group1_after = await db.get_private_control_group_by_chat(-100111111111)
        self.assertIsNone(group1_after)

        # Group 2 must be active
        group2_after = await db.get_private_control_group_by_chat(-100222222222)
        self.assertIsNotNone(group2_after)
        self.assertEqual(group2_after["title"], "Group Two")
        self.assertTrue(group2_after["active"])

    async def test_duplicate_key_re_setup_same_group(self):
        """Test that updating/re-running setup for the same group updates title/timestamp safely."""
        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100333333333,
            title="Original Title",
        )
        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100333333333,
            title="Updated Title",
        )
        group = await db.get_private_control_group_by_chat(-100333333333)
        self.assertIsNotNone(group)
        self.assertEqual(group["title"], "Updated Title")
        self.assertTrue(group["active"])

    async def test_private_group_module_logger_exists(self):
        """Regression test verifying that private_group module defines logger correctly."""
        import bot.handlers.private_group as pg
        self.assertTrue(hasattr(pg, "logger"))
        self.assertIsInstance(pg.logger, logging.Logger)

    async def test_end_to_end_setup_reference_flow(self):
        """Verify .privategroupvcsetup end-to-end at the code level.

        Flow: setup -> resolve group -> verify bot membership -> database lookup -> save mapping -> success.
        Ensures no AttributeError is raised!
        """
        from bot.handlers.private_group import setup_reference
        from telethon.tl import types as tl_types

        # Mock update & context
        update = MagicMock()
        update.effective_user.id = 99999
        update.effective_message.text = "-100444444444"
        replies = []

        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        context.user_data = {"private_group_setup": {"owner_user_id": 99999}}

        # Mock hosted client & manager
        hosted = MagicMock()
        hosted.is_running.return_value = True
        hosted._own_id = 88888
        hosted.client.get_me = AsyncMock(return_value=MagicMock(id=88888))

        # Mock telethon group entity
        mock_chat = MagicMock(spec=tl_types.Chat)
        mock_chat.id = 44444444
        mock_chat.title = "Test VC Chat"
        hosted.client.get_entity = AsyncMock(return_value=mock_chat)

        mock_permissions = MagicMock()
        mock_permissions.is_creator = True
        mock_permissions.is_admin = True
        hosted.client.get_permissions = AsyncMock(return_value=mock_permissions)

        manager = MagicMock()
        manager.get_client.return_value = hosted
        context.bot_data = {"manager": manager}

        # Mock telegram bot membership
        context.bot.get_me = AsyncMock(return_value=MagicMock(id=77777))
        context.bot.get_chat_member = AsyncMock(return_value=MagicMock(status="administrator"))

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply), \
             patch("bot.handlers.private_group.get_peer_id", return_value=-100444444444):
            result = await setup_reference(update, context)

        # Ensure ConversationHandler.END was returned (success)
        self.assertEqual(result, -1)  # ConversationHandler.END is -1
        self.assertTrue(any("Private VC Control Group setup completed" in r for r in replies))
        self.assertFalse(any("Setup failed" in r for r in replies))

        # Verify mapping was persisted in database
        mapping = await db.get_private_control_group_by_chat(-100444444444)
        self.assertIsNotNone(mapping)
        self.assertEqual(mapping["owner_user_id"], 99999)
        self.assertEqual(mapping["hosted_account_id"], 88888)
        self.assertEqual(mapping["title"], "Test VC Chat")
        self.assertTrue(mapping["active"])

    async def test_join_command_handler_no_active_call_error_message(self):
        """Verify /join -1002967424342 in private control group reports clear error without NameError or crash."""
        from bot.handlers.private_group import private_group_command
        from plugins.voice_chat import VoiceBridgeNoActiveGroupCall

        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100888999,
            title="Private Control HQ",
        )

        update = MagicMock()
        update.effective_chat.id = -100888999
        update.effective_user.id = 12345
        update.effective_message.text = "/join -1002967424342"

        replies = []
        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        hosted = MagicMock()
        hosted.is_running.return_value = True
        hosted._own_id = 67890

        voice = MagicMock()
        voice.join_bridge = AsyncMock(
            side_effect=VoiceBridgeNoActiveGroupCall("The target group (-1002967424342) has no active Voice Chat.")
        )
        hosted.client._voice_chat_manager = voice

        manager = MagicMock()
        manager.get_client.return_value = hosted
        context.bot_data = {"manager": manager}

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply), \
             patch("bot.handlers.private_group._hosted_for_user", return_value=hosted):
            await private_group_command(update, context)

        self.assertTrue(len(replies) > 0)
        self.assertTrue(any("The target group (-1002967424342) has no active Voice Chat" in r for r in replies))

    async def test_admin_authorized_without_own_hosted_session(self):
        """Verify that a group administrator can execute commands using the owner's hosted session."""
        from bot.handlers.private_group import private_group_command

        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100555666,
            title="Admin Test Group",
        )

        update = MagicMock()
        update.effective_chat.id = -100555666
        # Different user (admin)
        update.effective_user.id = 99999
        update.effective_message.text = "/leave"

        replies = []
        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        # Admin check returns administrator status
        admin_member = MagicMock()
        admin_member.status = "administrator"
        context.bot.get_chat_member = AsyncMock(return_value=admin_member)

        # Owner's hosted session
        owner_hosted = MagicMock()
        owner_hosted.is_running.return_value = True
        owner_hosted._own_id = 67890

        voice = MagicMock()
        voice.leave_bridge = AsyncMock(return_value="👋 Left target VC.")
        owner_hosted.client._voice_chat_manager = voice

        manager = MagicMock()
        def get_client_side_effect(uid):
            if uid == 12345:
                return owner_hosted
            return None
        manager.get_client.side_effect = get_client_side_effect
        context.bot_data = {"manager": manager}

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            await private_group_command(update, context)

        self.assertTrue(len(replies) > 0)
        self.assertTrue(any("Left target VC" in r for r in replies))

    async def test_unauthorized_user_rejected(self):
        """Verify that a regular non-admin user is rejected from using private control commands."""
        from bot.handlers.private_group import private_group_command

        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100555666,
            title="Admin Test Group",
        )

        update = MagicMock()
        update.effective_chat.id = -100555666
        update.effective_user.id = 88888  # Random non-owner user
        update.effective_message.text = "/leave"

        replies = []
        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        member = MagicMock()
        member.status = "member"
        context.bot.get_chat_member = AsyncMock(return_value=member)

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            await private_group_command(update, context)

        self.assertTrue(len(replies) > 0)
        self.assertTrue(any("Only the owner or group administrators" in r for r in replies))

    async def test_exception_does_not_kill_subsequent_commands(self):
        """Verify that a command exception is caught, reported to user, and does not break subsequent commands."""
        from bot.handlers.private_group import private_group_command

        await db.save_private_control_group(
            owner_user_id=12345,
            hosted_account_id=67890,
            private_control_group_id=-100777888,
            title="Resilience Test Group",
        )

        update1 = MagicMock()
        update1.effective_chat.id = -100777888
        update1.effective_user.id = 12345
        update1.effective_message.text = "/join -100999"

        replies = []
        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        hosted = MagicMock()
        hosted.is_running.return_value = True
        hosted._own_id = 67890

        voice = MagicMock()
        # Command 1 fails with an error
        voice.join_bridge = AsyncMock(side_effect=RuntimeError("Transient network failure"))
        # Command 2 succeeds
        voice.leave_all = AsyncMock(return_value="👋 Left all Voice Chats.")
        hosted.client._voice_chat_manager = voice

        manager = MagicMock()
        manager.get_client.return_value = hosted
        context.bot_data = {"manager": manager}

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            # First command fails
            await private_group_command(update1, context)
            self.assertTrue(any("Transient network failure" in r for r in replies))

            # Second command runs cleanly
            update2 = MagicMock()
            update2.effective_chat.id = -100777888
            update2.effective_user.id = 12345
            update2.effective_message.text = "/leaveall"
            await private_group_command(update2, context)
            self.assertTrue(any("Left all Voice Chats" in r for r in replies))


if __name__ == "__main__":
    unittest.main()

