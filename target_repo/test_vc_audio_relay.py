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
    ensure_pulseaudio_ready,
    ensure_virtual_sink,
    parse_sink_inputs,
    pulseaudio_available,
    route_sink_inputs_to_vcrelay,
    teardown_virtual_sink,
)
from telegram_userbot.vc_bridge.pulse_audio import is_pytgcalls_sink_input
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

        with patch("plugins.voice_chat.ensure_pulseaudio_ready", new_callable=AsyncMock) as mock_sink, \
             patch("plugins.voice_chat.ensure_virtual_sink", new_callable=AsyncMock), \
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

        def get_stream_path(stream_obj):
            if hasattr(stream_obj, "_media_path") and isinstance(stream_obj._media_path, str):
                return stream_obj._media_path
            if hasattr(stream_obj, "media_path") and isinstance(stream_obj.media_path, str):
                return stream_obj.media_path
            try:
                from pytgcalls.types import MediaStream as MS
                if hasattr(MS, "call_args_list") and MS.call_args_list:
                    urls = [str(c[0][0]) for c in MS.call_args_list if c and c[0]]
                    if urls:
                        return " ".join(urls)
            except Exception:
                pass
            if hasattr(stream_obj, "call_args") and stream_obj.call_args:
                return str(stream_obj.call_args[0][0])
            return str(stream_obj)

        self.assertEqual(call_1_args[0], -1001111)
        self.assertIn("silence", get_stream_path(call_1_args[1]))

        self.assertEqual(call_2_args[0], -1002222)
        self.assertIn("capture", get_stream_path(call_2_args[1]))

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

    async def test_ensure_pulseaudio_ready_already_running(self):
        """Test ensure_pulseaudio_ready when daemon and vcrelay sink/monitor are already active."""
        with patch("telegram_userbot.vc_bridge.pulse_audio.pulseaudio_available", return_value=True), \
             patch("telegram_userbot.vc_bridge.pulse_audio.pulseaudio_daemon_reachable", new_callable=AsyncMock) as mock_reach, \
             patch("telegram_userbot.vc_bridge.pulse_audio._run", new_callable=AsyncMock) as mock_run:
            mock_reach.return_value = (True, "Server Name: PulseAudio")
            mock_run.side_effect = [
                (MagicMock(), "0\tvcrelay\tmodule-null-sink.c", ""),  # sinks
                (MagicMock(), "0\tvcrelay.monitor\tmodule-null-sink.c", ""),  # sources
                (MagicMock(), "", ""),  # set-default-sink
                (MagicMock(), "", ""),  # set-default-source
            ]
            res = await ensure_pulseaudio_ready("vcrelay")
            self.assertEqual(res, "vcrelay.monitor")

    async def test_ensure_pulseaudio_ready_auto_start_and_create_sink(self):
        """Test ensure_pulseaudio_ready starts daemon if unreachable and creates sink + monitor."""
        reach_responses = [
            (False, "Connection refused"),
            (True, "Server Name: PulseAudio"),
        ]
        with patch("telegram_userbot.vc_bridge.pulse_audio.pulseaudio_available", return_value=True), \
             patch("telegram_userbot.vc_bridge.pulse_audio.pulseaudio_daemon_reachable", new_callable=AsyncMock, side_effect=reach_responses), \
             patch("telegram_userbot.vc_bridge.pulse_audio._start_daemon", new_callable=AsyncMock) as mock_start, \
             patch("telegram_userbot.vc_bridge.pulse_audio._run", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [
                (MagicMock(), "", ""),  # sinks (empty initial)
                (MagicMock(), "123", ""),  # load-module
                (MagicMock(), "0\tvcrelay.monitor\tmodule-null-sink.c", ""),  # sources check
                (MagicMock(), "", ""),  # set-default-sink
                (MagicMock(), "", ""),  # set-default-source
                (MagicMock(), "", ""),  # list sink-inputs
                (MagicMock(), "Server Name: PulseAudio", ""),  # info
                (MagicMock(), "0\tvcrelay", ""),  # sinks list
                (MagicMock(), "0\tvcrelay.monitor", ""),  # sources list
                (MagicMock(), "", ""),  # sink-inputs list
                (MagicMock(), "", ""),  # source-outputs list
            ]
            res = await ensure_pulseaudio_ready("vcrelay")
            self.assertEqual(res, "vcrelay.monitor")
            mock_start.assert_called_once()

    async def test_sink_input_filtering(self):
        """Test is_pytgcalls_sink_input filtering for PyTgCalls vs unrelated apps."""
        pytgcalls_item = {
            "app": "python3",
            "media": "PyTgCalls Audio Output",
            "binary": "python3",
            "driver": "protocol-native.c",
        }
        self.assertTrue(is_pytgcalls_sink_input(pytgcalls_item))

        firefox_item = {
            "app": "Firefox",
            "media": "YouTube Video",
            "binary": "firefox",
            "driver": "protocol-native.c",
        }
        self.assertFalse(is_pytgcalls_sink_input(firefox_item))

    async def test_route_sink_inputs_relocates_matching_input(self):
        """Test route_sink_inputs_to_vcrelay moves PyTgCalls sink inputs to vcrelay."""
        sample_inputs = [
            {
                "index": "10",
                "sink": "alsa_output.pci-0000_00_1b.0.analog-stereo",
                "app": "python3",
                "media": "PyTgCalls Audio",
                "binary": "python3",
                "driver": "protocol-native.c",
            }
        ]
        with patch("telegram_userbot.vc_bridge.pulse_audio.pulseaudio_available", return_value=True), \
             patch("telegram_userbot.vc_bridge.pulse_audio.parse_sink_inputs", new_callable=AsyncMock, return_value=sample_inputs), \
             patch("telegram_userbot.vc_bridge.pulse_audio._run", new_callable=AsyncMock) as mock_run:
            mock_run.side_effect = [
                (MagicMock(), "", ""),  # move-sink-input
                (MagicMock(), "Server Name: PulseAudio", ""),  # info for diag
                (MagicMock(), "0\tvcrelay", ""),  # sinks
                (MagicMock(), "0\tvcrelay.monitor", ""),  # sources
                (MagicMock(), "", ""),  # sink-inputs
                (MagicMock(), "", ""),  # source-outputs
            ]
            moved = await route_sink_inputs_to_vcrelay("vcrelay")
            self.assertEqual(moved, 1)
            mock_run.assert_any_call("pactl", "move-sink-input", "10", "vcrelay")

    async def test_watchdog_and_cleanup(self):
        """Test watchdog creation, periodic execution, and cleanup on _stop_bridge."""
        client_mock = MagicMock()
        client_mock.is_connected.return_value = True

        from telethon.tl import types as tl_types
        source_chat = MagicMock(spec=tl_types.Chat)
        source_chat.id = 101
        source_chat.title = "Source VC"

        target_chat = MagicMock(spec=tl_types.Channel)
        target_chat.id = 202
        target_chat.megagroup = True
        target_chat.title = "Target VC"

        def get_entity_side_effect(ident):
            if ident in (101, -100101):
                return source_chat
            if ident in (202, -100202, "targetgroup"):
                return target_chat
            raise ValueError(f"Unknown entity: {ident}")

        client_mock.get_entity = AsyncMock(side_effect=get_entity_side_effect)

        with patch("plugins.voice_chat.PyTgCalls"):
            vm = VoiceChatManager(client_mock)

        vm.calls.start = AsyncMock()
        vm.calls.play = AsyncMock()
        vm.calls.leave_call = AsyncMock()
        vm._active_group_call = AsyncMock(return_value=MagicMock())

        with patch("plugins.voice_chat.ensure_pulseaudio_ready", new_callable=AsyncMock) as mock_ready, \
             patch("plugins.voice_chat.ensure_virtual_sink", new_callable=AsyncMock), \
             patch("plugins.voice_chat.get_peer_id") as mock_peer_id, \
             patch("plugins.voice_chat.route_sink_inputs_to_vcrelay", new_callable=AsyncMock) as mock_route:
            mock_ready.return_value = "vcrelay.monitor"
            mock_peer_id.side_effect = lambda ent: -100101 if ent == source_chat else -100202
            await vm.join_bridge(source_chat_id=-100101, target_identifier="targetgroup")

            self.assertIsNotNone(vm.bridge)
            self.assertIsNotNone(vm.bridge.pulse_watchdog_task)

            # Let the watchdog run a tick
            await asyncio.sleep(0.1)

            bridge = vm.bridge
            await vm._stop_bridge(bridge, leave_target=True)

            self.assertFalse(bridge.active)
            self.assertIsNone(bridge.pulse_watchdog_task)
            self.assertIsNone(vm.bridge)


if __name__ == "__main__":
    unittest.main()
