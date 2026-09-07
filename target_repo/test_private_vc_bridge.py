"""Tests for Private VC Control Group admin permissions and Audio Bridge pipeline."""

import asyncio
import os
import sys
import unittest
from array import array
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR / "telegram_userbot"
sys.path.insert(0, str(REPO_DIR))
sys.path.insert(0, str(BASE_DIR))

import config
sys.modules["config.config"] = config

import database.mongo as db
from pytgcalls.exceptions import NoActiveGroupCall
from plugins.voice_chat import (
    BassFilter,
    VoiceChatManager,
    VoiceBridge,
    VoiceState,
    VoiceBridgeNoActiveGroupCall,
)


class TestAdminPermissions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp_fallback = BASE_DIR / "DB" / "test_admin_fallback.json"
        if self.tmp_fallback.exists():
            self.tmp_fallback.unlink()
        db._db = db._LocalDatabase(self.tmp_fallback)

        # Save private control group owned by user 111
        await db.save_private_control_group(
            owner_user_id=111,
            hosted_account_id=999,
            private_control_group_id=-100555666,
            title="Private VC HQ",
        )

    async def asyncTearDown(self):
        if self.tmp_fallback.exists():
            self.tmp_fallback.unlink()

    async def test_owner_can_execute_command(self):
        """Owner can execute commands directly."""
        from bot.handlers.private_group import private_group_command

        update = MagicMock()
        update.effective_chat.id = -100555666
        update.effective_user.id = 111  # Owner
        update.effective_message.text = "/level 15"
        replies = []

        async def fake_reply(message, text):
            replies.append(text)

        voice_mock = MagicMock()
        voice_mock.set_level = AsyncMock(return_value="🎚 Level set to 15/25.")

        hosted_mock = MagicMock()
        hosted_mock.is_running.return_value = True
        hosted_mock._own_id = 999
        hosted_mock.client._voice_chat_manager = voice_mock

        manager_mock = MagicMock()
        manager_mock.get_client.return_value = hosted_mock

        context = MagicMock()
        context.bot_data = {"manager": manager_mock}

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            await private_group_command(update, context)

        voice_mock.set_level.assert_called_once_with(15)
        self.assertTrue(any("Level set to 15/25" in r for r in replies))

    async def test_admin_without_hosted_account_can_execute_command_via_owner_session(self):
        """Administrator (non-owner) can execute commands on the OWNER's session."""
        from bot.handlers.private_group import private_group_command

        update = MagicMock()
        update.effective_chat.id = -100555666
        update.effective_user.id = 222  # Admin (not owner!)
        update.effective_message.text = "/level 12"
        replies = []

        async def fake_reply(message, text):
            replies.append(text)

        voice_mock = MagicMock()
        voice_mock.set_level = AsyncMock(return_value="🎚 Level set to 12/25.")

        # Hosted mock exists ONLY for owner (111), NOT for admin (222)
        def get_client_side_effect(uid):
            if uid == 111:
                hosted_mock = MagicMock()
                hosted_mock.is_running.return_value = True
                hosted_mock._own_id = 999
                hosted_mock.client._voice_chat_manager = voice_mock
                return hosted_mock
            return None

        manager_mock = MagicMock()
        manager_mock.get_client.side_effect = get_client_side_effect

        context = MagicMock()
        context.bot_data = {"manager": manager_mock}
        # get_chat_member confirms user 222 is an administrator
        context.bot.get_chat_member = AsyncMock(
            return_value=MagicMock(status="administrator")
        )

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            await private_group_command(update, context)

        # Confirm voice method was called on owner's voice manager
        voice_mock.set_level.assert_called_once_with(12)
        self.assertTrue(any("Level set to 12/25" in r for r in replies))

    async def test_non_admin_non_owner_is_rejected(self):
        """Regular member is rejected and cannot execute commands."""
        from bot.handlers.private_group import private_group_command

        update = MagicMock()
        update.effective_chat.id = -100555666
        update.effective_user.id = 333  # Random member
        update.effective_message.text = "/level 20"
        replies = []

        async def fake_reply(message, text):
            replies.append(text)

        context = MagicMock()
        context.bot.get_chat_member = AsyncMock(
            return_value=MagicMock(status="member")
        )

        with patch("bot.handlers.private_group.reply_html", side_effect=fake_reply):
            await private_group_command(update, context)

        self.assertTrue(any("Only the owner or group administrators" in r for r in replies))


class TestAudioBridgePipeline(unittest.IsolatedAsyncioTestCase):
    def test_bass_filter(self):
        """Test that BassFilter processes 16-bit PCM samples within bounds."""
        bf = BassFilter(sample_rate=48000, cutoff=120.0)
        # Create 480 test samples (10ms)
        raw_samples = array("h", [int(5000 * (i % 2 * 2 - 1)) for i in range(480)])
        bf.process_samples(raw_samples, bass=5)
        self.assertEqual(len(raw_samples), 480)
        for s in raw_samples:
            self.assertGreaterEqual(s, -32768)
            self.assertLessEqual(s, 32767)

    def test_apply_bridge_gain(self):
        """Test volume scaling, mute, and bass boosting."""
        raw_pcm = array("h", [1000] * 480).tobytes()

        # Normal volume 100%, bass 0
        normal = VoiceChatManager._apply_bridge_gain(raw_pcm, volume=100, bass=0)
        samples_normal = array("h")
        samples_normal.frombytes(normal)
        self.assertEqual(samples_normal[0], 1000)

        # Mute (volume 0)
        muted = VoiceChatManager._apply_bridge_gain(raw_pcm, volume=0, bass=0)
        samples_muted = array("h")
        samples_muted.frombytes(muted)
        self.assertEqual(samples_muted[0], 0)
        self.assertTrue(all(s == 0 for s in samples_muted))

        # Volume 200%
        boosted = VoiceChatManager._apply_bridge_gain(raw_pcm, volume=200, bass=0)
        samples_boosted = array("h")
        samples_boosted.frombytes(boosted)
        self.assertEqual(samples_boosted[0], 2000)

    async def test_bridge_join_and_leave(self):
        """Test join_bridge and leave_bridge logic on VoiceChatManager."""
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        # Mock source entity (Chat with active call)
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 100111
        source_chat.title = "Source VC Group"

        # Mock target entity (Channel megagroup with active call)
        target_chat = MagicMock(spec=tl_types.Channel)
        target_chat.id = 100222
        target_chat.megagroup = True
        target_chat.title = "Target VC Group"

        def get_entity_side_effect(ident):
            if ident in (100111, -100100111):
                return source_chat
            if ident in (100222, -100100222, "targetgroup"):
                return target_chat
            raise ValueError(f"Unknown entity: {ident}")

        client_mock.get_entity = AsyncMock(side_effect=get_entity_side_effect)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)
        # Mock pytgcalls
        vm.calls.start = AsyncMock()
        vm.calls.play = AsyncMock()
        vm.calls.leave_call = AsyncMock()
        vm.calls.send_frame = AsyncMock()
        vm._active_group_call = AsyncMock(return_value=MagicMock())

        with patch("plugins.voice_chat.get_peer_id") as mock_peer_id:
            mock_peer_id.side_effect = lambda ent: -100100111 if ent == source_chat else -100100222
            res = await vm.join_bridge(source_chat_id=-100100111, target_identifier="targetgroup")

        self.assertIn("joined target Voice Chat", res)
        self.assertIsNotNone(vm.bridge)
        self.assertTrue(vm.bridge.active)
        self.assertEqual(vm.bridge.source_chat_id, -100100111)
        self.assertEqual(vm.bridge.target_chat_id, -100100222)

        # Test level and bass adjustment
        level_res = await vm.set_level(10)
        self.assertEqual(vm.bridge.level, 10)
        self.assertEqual(vm.bridge.volume, 200)
        self.assertIn("Level set to 10/25", level_res)

        bass_res = await vm.set_bass(8)
        self.assertEqual(vm.bridge.bass, 8)
        self.assertIn("Bass level set to 8/15", bass_res)

        # Test leave_bridge
        leave_res = await vm.leave_bridge()
        self.assertIn("left target Voice Chat", leave_res)
        self.assertIsNone(vm.bridge)

        # Cleanup
        await vm.shutdown()

    async def test_join_bridge_no_active_vc_source_raises_proper_exception(self):
        """Regression test: verify no active VC in source group does NOT crash with NoActiveGroupCall.__init__()."""
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 100111
        source_chat.title = "Source VC Group"

        client_mock.get_entity = AsyncMock(return_value=source_chat)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)
        vm.calls.start = AsyncMock()
        # Simulate source group having NO active Voice Chat
        vm._active_group_call = AsyncMock(return_value=None)

        with self.assertRaises(NoActiveGroupCall) as ctx:
            await vm.join_bridge(source_chat_id=-100100111, target_identifier="-1002967424342")

        # Must not be a TypeError: NoActiveGroupCall.__init__() takes 1 positional argument but 2 were given
        self.assertNotIsInstance(ctx.exception, TypeError)
        self.assertIsInstance(ctx.exception, NoActiveGroupCall)
        self.assertIn("No active Voice Chat in this private control group", str(ctx.exception))
        await vm.shutdown()

    async def test_join_bridge_no_active_vc_target_reproduces_and_fixes_crash(self):
        """Regression test for user reproduction: /join -1002967424342 when target VC is not active.

        Before the fix, this triggered:
        TypeError: NoActiveGroupCall.__init__() takes 1 positional argument but 2 were given
        After the fix, this raises a proper VoiceBridgeNoActiveGroupCall with clear message.
        """
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 100111
        source_chat.title = "HQ Private VC"

        target_chat = MagicMock(spec=tl_types.Channel)
        target_chat.id = 2967424342
        target_chat.megagroup = True
        target_chat.title = "Target Group VC"

        def get_entity_side_effect(ident):
            if ident in (100111, -100100111):
                return source_chat
            if ident in (2967424342, -1002967424342, "-1002967424342"):
                return target_chat
            raise ValueError(f"Unknown entity: {ident}")

        client_mock.get_entity = AsyncMock(side_effect=get_entity_side_effect)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)
        vm.calls.start = AsyncMock()

        # Source has active call, but target has NO active call
        async def active_group_call_side_effect(entity):
            if entity == source_chat:
                return MagicMock()
            return None

        vm._active_group_call = AsyncMock(side_effect=active_group_call_side_effect)

        with patch("plugins.voice_chat.get_peer_id") as mock_peer_id:
            mock_peer_id.side_effect = lambda ent: -100100111 if ent == source_chat else -1002967424342
            with self.assertRaises(NoActiveGroupCall) as ctx:
                await vm.join_bridge(source_chat_id=-100100111, target_identifier="-1002967424342")

        # Crucial check: verify that TypeError was NOT raised
        self.assertNotIsInstance(ctx.exception, TypeError)
        self.assertIsInstance(ctx.exception, NoActiveGroupCall)
        self.assertIn("The target group", str(ctx.exception))
        self.assertIn("has no active Voice Chat", str(ctx.exception))
        await vm.shutdown()

    async def test_join_bridge_pytgcalls_native_no_active_group_call_handled(self):
        """Verify that if pytgcalls raises native NoActiveGroupCall() (0 args), it is handled cleanly."""
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 100111
        source_chat.title = "Source VC"

        target_chat = MagicMock(spec=tl_types.Channel)
        target_chat.id = 2967424342
        target_chat.megagroup = True
        target_chat.title = "Target Group"

        def get_entity_side_effect(ident):
            if ident in (100111, -100100111):
                return source_chat
            if ident in (2967424342, -1002967424342, "-1002967424342"):
                return target_chat
            raise ValueError(f"Unknown entity: {ident}")

        client_mock.get_entity = AsyncMock(side_effect=get_entity_side_effect)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)
        vm.calls.start = AsyncMock()
        vm.calls.leave_call = AsyncMock()
        vm._active_group_call = AsyncMock(return_value=MagicMock())

        # Simulate pytgcalls.play on target raising native NoActiveGroupCall()
        async def play_side_effect(chat_id, stream):
            if chat_id == -1002967424342:
                # py-tgcalls 2.3.3 raises NoActiveGroupCall() with 0 arguments
                raise NoActiveGroupCall()
            return None

        vm.calls.play = AsyncMock(side_effect=play_side_effect)

        with patch("plugins.voice_chat.get_peer_id") as mock_peer_id:
            mock_peer_id.side_effect = lambda ent: -100100111 if ent == source_chat else -1002967424342
            with self.assertRaises(NoActiveGroupCall) as ctx:
                await vm.join_bridge(source_chat_id=-100100111, target_identifier="-1002967424342")

        self.assertNotIsInstance(ctx.exception, TypeError)
        self.assertIsInstance(ctx.exception, NoActiveGroupCall)
        self.assertIn("The target group", str(ctx.exception))
        self.assertIn("has no active Voice Chat", str(ctx.exception))
        await vm.shutdown()


if __name__ == "__main__":
    unittest.main()
