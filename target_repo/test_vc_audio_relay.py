"""Comprehensive tests for PulseAudio Virtual Sink + HTTP Bridge VC-to-VC Audio Relay."""

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR / "telegram_userbot"
sys.path.insert(0, str(REPO_DIR))
sys.path.insert(0, str(BASE_DIR))

import config
sys.modules["config.config"] = config

try:
    import telethon
    from telethon import functions
    from telethon.tl import types as tl_types
    from telethon.utils import get_peer_id
except ImportError:
    class Chat:
        pass
    class Channel:
        pass
    mock_telethon = MagicMock()
    mock_tl = MagicMock()
    mock_tl_types = MagicMock()
    mock_tl_types.Chat = Chat
    mock_tl_types.Channel = Channel
    mock_tl.types = mock_tl_types
    mock_telethon.tl = mock_tl
    sys.modules["telethon"] = mock_telethon
    sys.modules["telethon.errors"] = mock_telethon
    sys.modules["telethon.functions"] = mock_telethon
    sys.modules["telethon.tl"] = mock_tl
    sys.modules["telethon.tl.types"] = mock_tl_types
    sys.modules["telethon.utils"] = mock_telethon
    mock_telethon.utils.get_peer_id = lambda entity: getattr(entity, "id", 0)

try:
    import aiohttp
    from aiohttp import web
except ImportError:
    import types
    class MockRunner:
        async def setup(self):
            pass
        async def cleanup(self):
            pass
    class MockSite:
        async def start(self):
            pass
        async def stop(self):
            pass
    mock_aiohttp = types.ModuleType("aiohttp")
    mock_web = types.ModuleType("web")
    mock_web.AppRunner = lambda *a, **kw: MockRunner()
    mock_web.TCPSite = lambda *a, **kw: MockSite()
    mock_web.Application = MagicMock
    mock_web.Request = MagicMock
    mock_web.StreamResponse = MagicMock
    mock_web.HTTPNotFound = Exception
    mock_aiohttp.web = mock_web
    sys.modules["aiohttp"] = mock_aiohttp
    sys.modules["aiohttp.web"] = mock_web

try:
    import pytgcalls
    from pytgcalls.exceptions import NoActiveGroupCall
    from pytgcalls.types import AudioQuality, MediaStream
except ImportError:
    class NoActiveGroupCall(Exception):
        pass
    class MediaStream:
        def __init__(self, media_path=None, *args, **kwargs):
            self._media_path = media_path
        Flags = MagicMock()
    class AudioQuality:
        STUDIO = "STUDIO"
    mock_pytgcalls = MagicMock()
    mock_exceptions = MagicMock()
    mock_exceptions.NoActiveGroupCall = NoActiveGroupCall
    mock_types = MagicMock()
    mock_types.MediaStream = MediaStream
    mock_types.AudioQuality = AudioQuality
    sys.modules["pytgcalls"] = mock_pytgcalls
    sys.modules["pytgcalls.exceptions"] = mock_exceptions
    sys.modules["pytgcalls.pytgcalls_session"] = mock_pytgcalls
    sys.modules["pytgcalls.types"] = mock_types
    sys.modules["pytgcalls.types.raw"] = mock_pytgcalls

import database.mongo as db
from telegram_userbot.vc_bridge import (
    AudioHTTPBridge,
    build_audio_filters,
    build_capture_command_stdout,
    build_silence_command_stdout,
    ensure_virtual_sink,
    pulseaudio_available,
    teardown_virtual_sink,
)
from plugins.voice_chat import VoiceChatManager, VoiceBridge, VoiceState


class TestVCAudioRelay(unittest.IsolatedAsyncioTestCase):
    async def test_audio_filters(self):
        """Test audio filter generation for level, bass, and mute."""
        f_default = build_audio_filters(level=5, bass=0, muted=False)
        self.assertIn("volume=1.0", f_default)
        self.assertIn("alimiter=limit=0.95", f_default)

        f_boosted = build_audio_filters(level=15, bass=5, muted=False)
        self.assertIn("volume=3.0", f_boosted)
        self.assertIn("bass=g=10:f=110:w=0.6", f_boosted)

        f_muted = build_audio_filters(level=10, bass=2, muted=True)
        self.assertEqual(f_muted, "volume=0")

    async def test_audio_http_bridge_lifecycle(self):
        """Test AudioHTTPBridge starting, registering silence stream, and serving."""
        bridge = AudioHTTPBridge(port=8765)
        await bridge.start()
        self.assertTrue(bridge._started)

        cmd = build_silence_command_stdout()
        url = await bridge.register_stream("test_stream", cmd, name="test-silence")
        self.assertIn("http://127.0.0.1:8765/audio/test_stream", url)
        await asyncio.sleep(0.5)
        self.assertTrue(bridge.is_stream_alive("test_stream"))

        await bridge.remove_stream("test_stream")
        self.assertFalse(bridge.is_stream_alive("test_stream"))
        await bridge.stop()

    async def test_voice_chat_manager_join_bridge_uses_mediastream(self):
        """Verify join_bridge configures MediaStream with silence on source and capture on target."""
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 1111
        source_chat.title = "Source Control VC"

        target_chat = MagicMock(spec=tl_types.Channel)
        target_chat.id = 2222
        target_chat.megagroup = True
        target_chat.title = "Target VC"

        def get_entity_side_effect(ident):
            if ident in (1111, -1001111):
                return source_chat
            if ident in (2222, -1002222, "targetgroup"):
                return target_chat
            raise ValueError(f"Unknown entity: {ident}")

        client_mock.get_entity = AsyncMock(side_effect=get_entity_side_effect)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)

        vm.calls.start = AsyncMock()
        vm.calls.play = AsyncMock()
        vm.calls.leave_call = AsyncMock()
        vm.calls.mute = AsyncMock()
        vm.calls.unmute = AsyncMock()
        vm._active_group_call = AsyncMock(return_value=MagicMock())

        with patch("plugins.voice_chat.ensure_virtual_sink", new_callable=AsyncMock) as mock_sink, \
             patch("plugins.voice_chat.get_peer_id") as mock_peer_id:
            mock_sink.return_value = "vcrelay.monitor"
            mock_peer_id.side_effect = lambda ent: -1001111 if ent == source_chat else -1002222
            res = await vm.join_bridge(source_chat_id=-1001111, target_identifier="targetgroup")

        self.assertIn("joined target Voice Chat", res)
        self.assertIsNotNone(vm.bridge)
        self.assertTrue(vm.bridge.active)

        # Check PyTgCalls.play calls:
        # First call: source_chat_id with MediaStream
        # Second call: target_chat_id with MediaStream
        self.assertEqual(vm.calls.play.call_count, 2)
        call_1_args = vm.calls.play.call_args_list[0][0]
        call_2_args = vm.calls.play.call_args_list[1][0]

        self.assertEqual(call_1_args[0], -1001111)
        self.assertIsInstance(call_1_args[1], MediaStream)
        self.assertIn("silence", str(call_1_args[1]._media_path))

        self.assertEqual(call_2_args[0], -1002222)
        self.assertIsInstance(call_2_args[1], MediaStream)
        self.assertIn("capture", str(call_2_args[1]._media_path))

        # Test set_level updates capture stream
        with patch.object(vm, "_restart_bridge_capture", new_callable=AsyncMock) as mock_restart:
            await vm.set_level(15)
            self.assertEqual(vm.bridge.level, 15)
            mock_restart.assert_called_once_with(vm.bridge)

        # Test set_bass updates capture stream
        with patch.object(vm, "_restart_bridge_capture", new_callable=AsyncMock) as mock_restart:
            await vm.set_bass(5)
            self.assertEqual(vm.bridge.bass, 5)
            mock_restart.assert_called_once_with(vm.bridge)

        # Test mute_bridge and unmute_bridge
        with patch.object(vm, "_restart_bridge_capture", new_callable=AsyncMock) as mock_restart:
            await vm.mute_bridge()
            self.assertTrue(vm.bridge.muted)
            mock_restart.assert_called_once_with(vm.bridge)
            vm.calls.mute.assert_called_once_with(-1002222)

        with patch.object(vm, "_restart_bridge_capture", new_callable=AsyncMock) as mock_restart:
            await vm.unmute_bridge()
            self.assertFalse(vm.bridge.muted)
            mock_restart.assert_called_once_with(vm.bridge)
            vm.calls.unmute.assert_called_once_with(-1002222)

        # Test leave_bridge
        leave_res = await vm.leave_bridge()
        self.assertIn("left target Voice Chat", leave_res)
        self.assertIsNone(vm.bridge)
        self.assertEqual(vm.calls.leave_call.call_count, 2)  # Left target & source

        # Cleanup
        await vm.shutdown()


if __name__ == "__main__":
    unittest.main()
