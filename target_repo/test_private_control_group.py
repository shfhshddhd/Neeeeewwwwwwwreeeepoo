"""Tests for Private VC Control Group database mapping and setup flow."""

import asyncio
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure target_repo and target_repo/telegram_userbot are on sys.path
BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR / "telegram_userbot"
sys.path.insert(0, str(REPO_DIR))
sys.path.insert(0, str(BASE_DIR))

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


if __name__ == "__main__":
    unittest.main()
