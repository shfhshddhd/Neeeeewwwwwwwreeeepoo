"""Per-hosted-account Telegram voice-chat playback and recording.

All runtime state belongs to one manager per hosted Telethon client.  A hosted
account can therefore have only one active voice-chat connection, while
different hosted accounts remain isolated from one another.
"""

from __future__ import annotations

import asyncio
import contextlib
from html import escape
import logging
import math
import re
import shutil
import subprocess
import tempfile
import time
from array import array
from collections.abc import Awaitable, Callable
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from pytgcalls.exceptions import NoActiveGroupCall
from pytgcalls import PyTgCalls
from pytgcalls.pytgcalls_session import PyTgCallsSession
from pytgcalls.types import (
    AudioQuality,
    Device,
    ExternalMedia,
    MediaStream,
    RecordStream,
    StreamEnded,
    StreamFrames,
)
from pytgcalls.types.raw import AudioParameters
from telethon import functions
from telethon.tl import types as tl_types
from telethon.utils import get_peer_id

from plugins.bot import add_handler
from telegram_userbot.vc_bridge import (
    AudioHTTPBridge,
    build_capture_command_stdout,
    build_silence_command_stdout,
    ensure_virtual_sink,
    log_pulseaudio_diagnostics,
    pulseaudio_available,
    route_sink_inputs_to_vcrelay,
    set_default_pulse_sink_and_source,
    teardown_virtual_sink,
)

try:
    import yt_dlp
except ImportError:  # pragma: no cover - URL playback reports this at runtime
    yt_dlp = None


logger = logging.getLogger(__name__)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_MAX_TRACKS = 50
# The command uses 100 as unity gain: 200 is 2x, 500 is 5x, and 1000 is 10x.
# This is intentionally much higher than PyTgCalls' call-output range because
# gain is applied to a temporary playback copy before it is streamed.
_MAX_VOLUME = 100_000_000
_SAFE_DEFAULT_VOLUME = 100
_PLAYBACK_END_GRACE_SECONDS = 1.5
_LIVE_FRAME_QUEUE_SIZE = 3
_LIVE_RECEIVE_QUEUE_SIZE = 8
_BRIDGE_QUEUE_SIZE = 20
_BRIDGE_DEFAULT_LEVEL = 5
# NTgCalls AudioSink is fixed to 10 ms PCM frames. At 48 kHz mono, 16-bit
# little-endian PCM that is 480 samples / 960 bytes per external frame.
_LIVE_FRAME_BYTES = 480 * 2


class BassFilter:
    """Zero-latency single-pole low-shelf IIR filter for 48 kHz PCM16 audio."""

    def __init__(self, sample_rate: int = 48000, cutoff: float = 120.0):
        self.sample_rate = sample_rate
        self.cutoff = cutoff
        self._prev_low = 0.0
        dt = 1.0 / sample_rate
        rc = 1.0 / (2.0 * math.pi * cutoff)
        self._alpha = dt / (rc + dt)

    def process_samples(self, samples: array, bass: int) -> None:
        if bass <= 0:
            return
        boost = bass * 0.25
        alpha = self._alpha
        prev_low = self._prev_low
        for i in range(len(samples)):
            x = samples[i]
            prev_low = prev_low + alpha * (x - prev_low)
            y = int(round(x + prev_low * boost))
            samples[i] = max(-32768, min(32767, y))
        self._prev_low = prev_low


@dataclass
class Track:
    title: str
    path: Path
    source: str
    on_complete: Callable[[], Awaitable[None]] | None = None


@dataclass
class VoiceState:
    chat_id: int
    chat_title: str = ""
    queue: deque[Track] = field(default_factory=deque)
    current: Track | None = None
    volume: int = 100
    bass: int = 0
    muted: bool = False
    joined_at: float = field(default_factory=time.monotonic)
    recording_path: Path | None = None
    recording_task: asyncio.Task | None = None
    recording_started_at: float | None = None
    closing: bool = False
    playback_epoch: int = 0
    playback_watchdog: asyncio.Task | None = None
    playback_deadline: float | None = None
    playback_remaining: float | None = None
    playback_paused: bool = False
    live_active: bool = False
    live_mic_enabled: bool = True
    live_push_to_talk: bool = False
    live_push_active: bool = True
    live_started_at: float | None = None
    live_frame_count: int = 0
    live_byte_count: int = 0
    live_last_frame_at: float | None = None
    live_frame_queue: asyncio.Queue[bytes] | None = None
    live_sender_task: asyncio.Task | None = None
    receive_subscribers: set[asyncio.Queue[bytes]] = field(default_factory=set)
    transition_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class VoiceBridge:
    """Source VC -> Target VC real-time audio bridge pipeline."""

    source_chat_id: int
    target_chat_id: int
    source_state: VoiceState
    target_state: VoiceState
    queue: asyncio.Queue[bytes]
    relay_task: asyncio.Task | None = None
    active: bool = False
    volume: int = 100
    level: int = 5
    bass: int = 0
    muted: bool = False
    started_at: float = field(default_factory=time.monotonic)
    relayed_frames: int = 0
    relayed_bytes: int = 0
    last_frame_at: float | None = None
    bass_filter: BassFilter = field(default_factory=BassFilter)
    monitor_source: str = ""
    silence_key: str = ""
    capture_key: str = ""
    silence_url: str = ""
    capture_url: str = ""
    pulse_watchdog_task: asyncio.Task | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class VoiceBridgeNoActiveGroupCall(NoActiveGroupCall, RuntimeError):
    """Exception raised when a required group voice call is not active during bridging."""

    def __init__(self, message: str = "No active Voice Chat found."):
        self.message = message
        super(NoActiveGroupCall, self).__init__(message)

    def __str__(self) -> str:
        return self.message


def _safe_title(value: str) -> str:
    value = " ".join((value or "").split()).strip()
    return value[:160] or "Voice chat audio"


def _format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes}m {seconds:02d}s"


def _download_url(url: str, output_dir: Path) -> tuple[Path, str]:
    if yt_dlp is None:
        raise RuntimeError("URL playback needs the yt-dlp package.")
    template = str(output_dir / "download.%(ext)s")
    options = {
        "format": "bestaudio/best",
        "outtmpl": template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": True,
    }
    with yt_dlp.YoutubeDL(options) as downloader:
        info = downloader.extract_info(url, download=True)
        prepared = Path(downloader.prepare_filename(info))
        title = _safe_title(info.get("title") or prepared.stem)
    if not prepared.exists():
        matches = sorted(output_dir.glob("download.*"))
        if not matches:
            raise FileNotFoundError("The audio download did not produce a file.")
        prepared = matches[0]
    return prepared, title


def _create_gain_copy(source: Path, output_dir: Path, volume: int) -> Path:
    """Create a gain-only, temporary stream copy without touching ``source``."""
    playback_path = output_dir / "playback-gain.wav"
    gain = volume / 100
    result = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(source),
            "-map",
            "0:a:0",
            "-vn",
            "-af",
            f"volume={gain:.12g}:precision=float",
            "-c:a",
            "pcm_f32le",
            str(playback_path),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if result.returncode != 0 or not playback_path.is_file():
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(
            "Could not prepare the audio for Voice Chat playback."
            + (f" {detail[-500:]}" if detail else "")
        )
    if playback_path.stat().st_size == 0:
        raise RuntimeError("The prepared Voice Chat audio file is empty.")
    return playback_path


def _probe_duration(source: Path) -> float | None:
    """Read a local media duration for the end-of-stream safety watchdog."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(source),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        # Playback must remain usable when only ffmpeg is available or when
        # probing a damaged/remote-derived file takes too long. StreamEnded
        # remains the primary completion signal in that case.
        return None
    if result.returncode != 0:
        return None
    try:
        duration = float((result.stdout or "").strip())
    except ValueError:
        return None
    return duration if duration > 0 else None


class VoiceChatManager:
    """One PyTgCalls connection and one voice-chat state per hosted account."""

    def __init__(self, client):
        self.client = client
        self.calls = PyTgCalls(client)
        self.state: VoiceState | None = None
        self.sessions: dict[int, VoiceState] = {}
        self.bridge: VoiceBridge | None = None
        self._started = False
        self._tasks: set[asyncio.Task] = set()
        self._temp_dir = Path(tempfile.mkdtemp(prefix="telegram-userbot-vc-"))
        self.audio_bridge: AudioHTTPBridge = AudioHTTPBridge()
        self.pulse_sink_name: str = "vcrelay"

    def get_state(self, chat_id: int | None = None) -> VoiceState | None:
        if chat_id is None:
            return self.state
        if self.state is not None and self.state.chat_id == chat_id:
            return self.state
        return self.sessions.get(chat_id)

    async def start(self) -> None:
        if self._started:
            return
        if not self.client.is_connected():
            raise RuntimeError("The hosted Telethon client is not connected.")
        if shutil.which("ffmpeg") is None:
            raise RuntimeError(
                "FFmpeg is not available. Install FFmpeg before using Voice Chat."
            )
        # PyTgCalls performs a remote version check during its first start.
        # The check is informational and must not delay the userbot startup.
        PyTgCallsSession.notice_displayed = True

        async def on_update(_, update):
            if isinstance(update, StreamFrames):
                update_chat_id = getattr(update, "chat_id", None)
                state = self.get_state(update_chat_id)
                chat_match = state is not None
                direction = str(
                    getattr(update.direction, "name", update.direction)
                ).upper()
                device = str(
                    getattr(update.device, "name", update.device)
                ).upper()
                if not (direction == "INCOMING" and device == "SPEAKER"):
                    return

                # 1. Source -> Target Audio Bridge Relay (PCM16, 48kHz, mono)
                bridge = self.bridge
                if (
                    bridge is not None
                    and bridge.active
                    and bridge.source_chat_id == update_chat_id
                ):
                    for frame in update.frames:
                        payload = (
                            getattr(
                                frame,
                                "frame",
                                getattr(frame, "data", b""),
                            )
                            or b""
                        )
                        if payload:
                            if bridge.queue.full():
                                with contextlib.suppress(asyncio.QueueEmpty):
                                    bridge.queue.get_nowait()
                            with contextlib.suppress(asyncio.QueueFull):
                                bridge.queue.put_nowait(payload)

                # 2. Local state subscribers (recording, Mini App live mic)
                if state is not None and state.receive_subscribers:
                    for frame in update.frames:
                        payload = (
                            getattr(
                                frame,
                                "frame",
                                getattr(frame, "data", b""),
                            )
                            or b""
                        )
                        if payload:
                            for subscriber in tuple(state.receive_subscribers):
                                if subscriber.full():
                                    with contextlib.suppress(asyncio.QueueEmpty):
                                        subscriber.get_nowait()
                                with contextlib.suppress(asyncio.QueueFull):
                                    subscriber.put_nowait(payload)

                # 3. Voice AI Capture & Debug
                voice_ai_active = (
                    getattr(self, "_voice_ai_enabled", False)
                    and getattr(self, "_voice_ai_capture_chat_id", None)
                    == update_chat_id
                )
                if voice_ai_active:
                    for frame in update.frames:
                        payload = (
                            getattr(
                                frame,
                                "frame",
                                getattr(frame, "data", b""),
                            )
                            or b""
                        )
                        if payload:
                            now = time.monotonic()
                            self._voice_ai_capture_first_packet_at = (
                                getattr(
                                    self,
                                    "_voice_ai_capture_first_packet_at",
                                    None,
                                )
                                or now
                            )
                            self._voice_ai_capture_last_packet_at = now
                            self._voice_ai_capture_packet_count = (
                                getattr(
                                    self,
                                    "_voice_ai_capture_packet_count",
                                    0,
                                )
                                + 1
                            )
                            self._voice_ai_capture_packet_bytes = (
                                getattr(
                                    self,
                                    "_voice_ai_capture_packet_bytes",
                                    0,
                                )
                                + len(payload)
                            )
                            activity_event = getattr(
                                self,
                                "_voice_ai_capture_activity",
                                None,
                            )
                            if activity_event is not None:
                                activity_event.set()
                return

            if not isinstance(update, StreamEnded):
                return
            if getattr(self, "_voice_ai_enabled", False):
                logger.info(
                    "[VOICE_AI_DEBUG] stream ended chat=%s type=%s device=%s.",
                    update.chat_id,
                    update.stream_type,
                    update.device,
                )
            if update.stream_type != StreamEnded.Type.AUDIO:
                return
            state = self.get_state(update.chat_id)
            expected_track = (
                state.current
                if state is not None
                and state.chat_id == update.chat_id
                and not state.closing
                else None
            )
            if expected_track is None:
                return
            task = asyncio.create_task(
                self._advance_after_end(update.chat_id, expected_track)
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        self.calls.on_update()(on_update)
        logger.info(
            "[VOICE_AI_DEBUG] PYTG_CALLS_UPDATE_HANDLER_REGISTERED "
            "api=on_update callback=VoiceChatManager.on_update callbacks=%d.",
            len(getattr(self.calls, "_callbacks", [])),
        )
        await asyncio.wait_for(self.calls.start(), timeout=20)
        self._started = True

    def _ensure_single_connection(self, chat_id: int) -> VoiceState:
        state = self.get_state(chat_id)
        if state is not None:
            return state
        if self.state is not None and self.state.chat_id != chat_id and self.bridge is None:
            raise RuntimeError(
                f"I am already connected to `{self.state.chat_id}`. "
                "Use .vcleave there before joining another voice chat."
            )
        new_state = VoiceState(chat_id=chat_id, volume=_SAFE_DEFAULT_VOLUME)
        self.sessions[chat_id] = new_state
        if self.state is None:
            self.state = new_state
        return new_state

    def _require_state(self, chat_id: int) -> VoiceState:
        state = self.get_state(chat_id)
        if state is None:
            raise RuntimeError("I am not connected to a voice chat here.")
        return state

    async def _active_group_call(self, entity):
        """Return Telegram's active group-call descriptor, if one exists."""
        if isinstance(entity, tl_types.Channel):
            full_chat = await self.client(
                functions.channels.GetFullChannelRequest(channel=entity)
            )
        elif isinstance(entity, tl_types.Chat):
            full_chat = await self.client(
                functions.messages.GetFullChatRequest(chat_id=entity.id)
            )
        else:
            raise ValueError("The target must be a group or supergroup.")
        return getattr(full_chat.full_chat, "call", None)

    async def join_target(self, identifier: str) -> str:
        """Resolve a group, require an active VC, then connect once."""
        if not identifier:
            raise ValueError("Usage: .vcjoin <group username or chat ID>")
        await self.start()
        try:
            entity = await self.client.get_entity(
                int(identifier) if identifier.lstrip("-").isdigit() else identifier.lstrip("@")
            )
        except Exception as exc:
            raise ValueError(
                "Could not find that group. Use a group username or numeric chat ID."
            ) from exc

        if (
            not isinstance(entity, tl_types.Chat)
            and not (
                isinstance(entity, tl_types.Channel)
                and bool(getattr(entity, "megagroup", False))
            )
        ):
            raise ValueError("The target must be a group or supergroup.")

        if await self._active_group_call(entity) is None:
            raise NoActiveGroupCall()

        chat_id = int(get_peer_id(entity))
        if self.state is not None and self.state.chat_id != chat_id:
            raise RuntimeError(
                f"I am already connected to <code>{self.state.chat_id}</code>. "
                "Use .vcleave before joining another voice chat."
            )
        if self.state is not None:
            title = escape(_safe_title(getattr(entity, "title", None)))
            return (
                f"✅ Already connected to <b>{title}</b> "
                f"(<code>{chat_id}</code>)."
            )

        try:
            await self.calls.play(chat_id, None)
        except NoActiveGroupCall:
            raise
        except Exception as exc:
            raise RuntimeError(f"Could not connect to the active Voice Chat: {exc}") from exc

        self.state = VoiceState(
            chat_id=chat_id,
            chat_title=_safe_title(getattr(entity, "title", None)),
            volume=_SAFE_DEFAULT_VOLUME,
        )
        self.sessions[chat_id] = self.state
        return (
            f"✅ Connected to <b>{escape(self.state.chat_title)}</b> "
            f"(<code>{chat_id}</code>)."
        )

    async def _prepare_track(self, event, args: str) -> Track:
        work_dir = Path(tempfile.mkdtemp(prefix="track-", dir=self._temp_dir))
        try:
            url_match = _URL_RE.search(args)
            source = ""
            title = ""
            if url_match:
                source = url_match.group(0).rstrip(".,)>")
                raw_path, title = await asyncio.to_thread(
                    _download_url, source, work_dir
                )
            else:
                reply = await event.get_reply_message()
                if reply is None or not reply.media:
                    raise ValueError(
                        "Reply to an audio, voice, or video message, or provide a URL."
                    )
                downloaded = await reply.download_media(file=str(work_dir / "input"))
                if not downloaded:
                    raise RuntimeError("Telegram did not provide a downloadable media file.")
                raw_path = Path(downloaded)
                source = f"message:{reply.id}"
                title = _safe_title(
                    getattr(reply.file, "name", None)
                    or getattr(reply, "text", None)
                    or "Telegram audio"
                )

            return Track(
                title=_safe_title(title or raw_path.stem),
                path=raw_path,
                source=source,
            )
        except Exception:
            shutil.rmtree(work_dir, ignore_errors=True)
            raise

    async def enqueue(self, event, chat_id: int, args: str) -> str:
        state = self._ensure_single_connection(chat_id)
        if len(state.queue) >= _MAX_TRACKS:
            raise RuntimeError(f"The queue is full ({_MAX_TRACKS} tracks maximum).")
        track = await self._prepare_track(event, args.strip())
        state.queue.append(track)
        if state.current is None:
            await self._play_next(state)
            return self._now_playing_text(state)
        return (
            f"➕ Queued: <b>{escape(track.title)}</b> · "
            f"position {len(state.queue)}"
        )

    async def enqueue_file(
        self,
        source: Path,
        title: str,
        source_label: str,
        on_complete: Callable[[], Awaitable[None]] | None = None,
    ) -> str:
        """Queue a control-bot download without changing its audio quality."""
        if not source.exists() or not source.is_file():
            raise FileNotFoundError("The replied audio file is unavailable.")
        state = self.state
        if state is None:
            raise RuntimeError("Join an active Voice Chat first with .vcjoin.")
        if len(state.queue) >= _MAX_TRACKS:
            raise RuntimeError(f"The queue is full ({_MAX_TRACKS} tracks maximum).")

        work_dir = Path(tempfile.mkdtemp(prefix="track-", dir=self._temp_dir))
        destination = work_dir / source.name
        try:
            await asyncio.to_thread(shutil.copy2, source, destination)
        except Exception:
            shutil.rmtree(work_dir, ignore_errors=True)
            raise

        track = Track(
            title=_safe_title(title or destination.stem),
            path=destination,
            source=source_label,
            on_complete=on_complete,
        )
        state.queue.append(track)
        try:
            if state.current is None:
                await self._play_next(state)
                return self._now_playing_text(state)
            return (
                f"➕ Queued: <b>{escape(track.title)}</b> · "
                f"position {len(state.queue)}"
            )
        except Exception:
            with contextlib.suppress(ValueError):
                state.queue.remove(track)
            self._remove_track(track)
            raise

    async def _play_next(self, state: VoiceState) -> None:
        async with state.transition_lock:
            await self._play_next_locked(state)

    async def _play_next_locked(self, state: VoiceState) -> None:
        """Start the next track while ``state.transition_lock`` is held."""
        if state.closing or state.current is not None or not state.queue:
            return
        track = state.queue.popleft()
        playback_epoch = state.playback_epoch
        try:
            # Keep the downloaded track untouched. The only transform is
            # digital gain on a temporary copy used by the stream.
            playback_path = await asyncio.to_thread(
                _create_gain_copy,
                track.path,
                track.path.parent,
                state.volume,
            )
            playback_duration = await asyncio.to_thread(
                _probe_duration,
                playback_path,
            )
            # .vcstop can request cancellation while the gain copy is being
            # prepared. Do not start a stream that was already cancelled.
            if (
                state.closing
                or state.playback_epoch != playback_epoch
                or self.state is not state
            ):
                self._remove_track(track)
                return
            await self.calls.play(
                state.chat_id,
                MediaStream(
                    playback_path,
                    audio_flags=MediaStream.Flags.REQUIRED,
                    video_flags=MediaStream.Flags.IGNORE,
                ),
            )
            # A stop request may arrive while PyTgCalls is replacing the
            # source. Clear that source here as well; the stop command will
            # perform the same idempotent operation once it owns the lock.
            if (
                state.closing
                or state.playback_epoch != playback_epoch
                or self.state is not state
            ):
                with contextlib.suppress(Exception):
                    await self.calls.play(state.chat_id, None)
                self._remove_track(track)
                return
        except Exception:
            self._remove_track(track)
            raise
        state.current = track
        state.playback_paused = False
        if playback_duration is not None:
            self._schedule_playback_watchdog(
                state,
                track,
                playback_duration + _PLAYBACK_END_GRACE_SECONDS,
                playback_epoch,
            )

    @staticmethod
    def _now_playing_text(state: VoiceState) -> str:
        if state.current is None:
            return "▶️ Playback started."
        muted = " (muted)" if state.muted else ""
        return (
            f"▶️ Now playing: <b>{escape(state.current.title)}</b>\n"
            f"🔊 Volume: <code>{state.volume}%</code>{muted}"
        )

    @staticmethod
    def _apply_live_gain(data: bytes, volume: int) -> bytes:
        """Apply the configured linear gain to little-endian PCM16 samples."""
        if not data or volume == 100:
            return data
        usable_length = len(data) - (len(data) % 2)
        if usable_length <= 0:
            return b""
        samples = array("h")
        samples.frombytes(data[:usable_length])
        if volume == 0:
            samples = array("h", [0]) * len(samples)
        else:
            multiplier = volume / 100
            for index, sample in enumerate(samples):
                amplified = int(round(sample * multiplier))
                samples[index] = max(-32768, min(32767, amplified))
        if samples.itemsize != 2:
            raise RuntimeError("The runtime does not expose 16-bit PCM samples.")
        return samples.tobytes()

    async def start_live(self, chat_id: int) -> str:
        """Replace an idle playback source with an external PCM microphone source."""
        state = self._require_state(chat_id)
        if state.live_active:
            return "🎙️ Live microphone is already streaming."
        if state.current is not None or state.queue:
            raise RuntimeError(
                "Stop playback with .vcstop before starting the live microphone."
            )
        stream = MediaStream(
            ExternalMedia.AUDIO,
            AudioParameters(bitrate=48000, channels=1),
            audio_flags=MediaStream.Flags.REQUIRED,
            video_flags=MediaStream.Flags.IGNORE,
        )
        await self.calls.play(chat_id, stream)
        state.live_active = True
        state.live_mic_enabled = True
        state.live_push_to_talk = False
        state.live_push_active = True
        state.live_started_at = time.monotonic()
        state.live_frame_count = 0
        state.live_byte_count = 0
        state.live_last_frame_at = None
        state.live_frame_queue = asyncio.Queue(maxsize=_LIVE_FRAME_QUEUE_SIZE)
        state.live_sender_task = asyncio.create_task(
            self._send_live_frames(state),
            name=f"live-audio-{chat_id}",
        )
        self._tasks.add(state.live_sender_task)
        state.live_sender_task.add_done_callback(self._tasks.discard)
        logger.info(
            "Live microphone source started for Voice Chat %s using "
            "PyTgCalls external PCM16L frames.",
            chat_id,
        )
        return "🎙️ Live microphone started."

    async def stop_live(self, chat_id: int) -> str:
        """Stop only the external microphone source and keep the VC connection."""
        state = self._require_state(chat_id)
        if not state.live_active:
            return "Live microphone is already stopped."
        state.live_active = False
        await self._stop_live_sender(state)
        await self.calls.play(chat_id, None)
        state.live_started_at = None
        state.live_last_frame_at = None
        state.live_push_active = False
        logger.info(
            "Live microphone source stopped for Voice Chat %s after %d frame(s).",
            chat_id,
            state.live_frame_count,
        )
        return "⏹️ Live microphone stopped; I am still in the voice chat."

    async def _stop_live_sender(self, state: VoiceState) -> None:
        queue = state.live_frame_queue
        state.live_frame_queue = None
        sender_task = state.live_sender_task
        state.live_sender_task = None
        if sender_task is not None and sender_task is not asyncio.current_task():
            sender_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender_task
        if queue is not None:
            while not queue.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()

    async def _send_live_frames(self, state: VoiceState) -> None:
        """Forward live PCM without allowing a slow native call to build latency."""
        queue = state.live_frame_queue
        if queue is None:
            return
        try:
            while state.live_active and state.live_frame_queue is queue:
                data = await queue.get()
                if not data or not state.live_active:
                    continue
                transformed = self._apply_live_gain(data, state.volume)
                if not transformed:
                    continue
                await self.calls.send_frame(
                    state.chat_id,
                    Device.MICROPHONE,
                    transformed,
                )
                state.live_frame_count += 1
                state.live_byte_count += len(transformed)
                state.live_last_frame_at = time.monotonic()
                if state.live_frame_count == 1 or state.live_frame_count % 100 == 0:
                    logger.info(
                        "Live microphone frame sent to Telegram Voice Chat %s: "
                        "frames=%d bytes=%d gain=%d%% queue=%d.",
                        state.chat_id,
                        state.live_frame_count,
                        state.live_byte_count,
                        state.volume,
                        queue.qsize(),
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Live microphone sender stopped for Voice Chat %s.",
                state.chat_id,
            )

    def set_live_controls(
        self,
        chat_id: int,
        *,
        mic_enabled: bool | None = None,
        push_to_talk: bool | None = None,
        push_active: bool | None = None,
    ) -> dict:
        state = self._require_state(chat_id)
        if mic_enabled is not None:
            state.live_mic_enabled = bool(mic_enabled)
        if push_to_talk is not None:
            state.live_push_to_talk = bool(push_to_talk)
        if push_active is not None:
            state.live_push_active = bool(push_active)
        return self.live_snapshot(chat_id)

    async def send_live_frame(self, chat_id: int, data: bytes) -> bool:
        """Queue one browser PCM16L frame without creating an audio backlog."""
        state = self._require_state(chat_id)
        if not state.live_active or not state.live_mic_enabled:
            return False
        if state.live_push_to_talk and not state.live_push_active:
            return False
        if not data:
            return False
        if len(data) != _LIVE_FRAME_BYTES:
            logger.warning(
                "Ignoring malformed live microphone frame for Voice Chat %s: "
                "got %d bytes, expected %d.",
                chat_id,
                len(data),
                _LIVE_FRAME_BYTES,
            )
            return False
        queue = state.live_frame_queue
        if queue is None:
            return False
        # Never wait for the native sender here. Keeping the newest few frames
        # bounds end-to-end latency when Telegram briefly slows down.
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(data)

        # Also relay to active bridge if this chat is the bridge source
        if (
            self.bridge is not None
            and self.bridge.active
            and self.bridge.source_chat_id == chat_id
        ):
            b_queue = self.bridge.queue
            if b_queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    b_queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                b_queue.put_nowait(data)

        return True

    def subscribe_receive(self, chat_id: int) -> asyncio.Queue[bytes]:
        """Subscribe to untouched Telegram playback PCM for one Mini App."""
        state = self._require_state(chat_id)
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=_LIVE_RECEIVE_QUEUE_SIZE)
        state.receive_subscribers.add(queue)
        return queue

    def unsubscribe_receive(self, chat_id: int, queue: asyncio.Queue[bytes]) -> None:
        state = self.get_state(chat_id)
        if state is not None:
            state.receive_subscribers.discard(queue)

    def live_snapshot(self, chat_id: int | None = None) -> dict:
        state = self.get_state(chat_id)
        if state is None:
            return {
                "active": False,
                "mic_enabled": False,
                "push_to_talk": False,
                "push_active": False,
                "frames": 0,
                "bytes": 0,
                "started_at": None,
                "last_frame_at": None,
            }
        return {
            "active": state.live_active,
            "mic_enabled": state.live_mic_enabled,
            "push_to_talk": state.live_push_to_talk,
            "push_active": state.live_push_active,
            "frames": state.live_frame_count,
            "bytes": state.live_byte_count,
            "started_at": state.live_started_at,
            "last_frame_at": state.live_last_frame_at,
        }

    async def _advance_after_end(self, chat_id: int, expected_track: Track) -> None:
        state = self.state
        if state is None or state.chat_id != chat_id:
            return
        completion_callback = None
        async with state.transition_lock:
            if state.closing or state.current is not expected_track:
                return
            finished = expected_track
            await self._cancel_playback_watchdog(state)
            state.playback_deadline = None
            state.playback_remaining = None
            state.playback_paused = False
            state.current = None
            self._remove_track(finished)
            completion_callback = finished.on_complete
            try:
                await self._play_next_locked(state)
            except Exception:
                logger.exception(
                    "Could not advance the voice-chat queue in %s.", chat_id
                )
        if completion_callback is not None:
            await self._notify_playback_complete(finished)

    async def _playback_end_watchdog(
        self,
        state: VoiceState,
        expected_track: Track,
        playback_epoch: int,
    ) -> None:
        try:
            while True:
                deadline = state.playback_deadline
                if deadline is None:
                    return
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    await asyncio.sleep(remaining)
                if not state.playback_paused:
                    break
                await asyncio.sleep(0.25)
            if state.playback_epoch != playback_epoch:
                return
            await self._advance_after_end(state.chat_id, expected_track)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Playback completion watchdog failed in %s.",
                state.chat_id,
            )

    def _schedule_playback_watchdog(
        self,
        state: VoiceState,
        track: Track,
        remaining: float,
        playback_epoch: int,
    ) -> None:
        if remaining <= 0:
            return
        state.playback_remaining = None
        state.playback_deadline = time.monotonic() + remaining
        watchdog = asyncio.create_task(
            self._playback_end_watchdog(
                state,
                track,
                playback_epoch,
            )
        )
        state.playback_watchdog = watchdog
        self._tasks.add(watchdog)
        watchdog.add_done_callback(self._tasks.discard)

    async def _cancel_playback_watchdog(self, state: VoiceState) -> None:
        watchdog = state.playback_watchdog
        state.playback_watchdog = None
        if watchdog is not None and watchdog is not asyncio.current_task():
            watchdog.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog

    async def _notify_playback_complete(self, track: Track) -> None:
        if track.on_complete is None:
            return
        try:
            await track.on_complete()
        except Exception:
            logger.exception(
                "Could not send playback completion notice for %s.",
                track.title,
            )

    @staticmethod
    def _remove_track(track: Track) -> None:
        shutil.rmtree(track.path.parent, ignore_errors=True)

    async def pause(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        await self.calls.pause(chat_id)
        if state.current is not None and state.playback_deadline is not None:
            state.playback_remaining = max(
                0,
                state.playback_deadline - time.monotonic(),
            )
            state.playback_deadline = None
            state.playback_paused = True
            await self._cancel_playback_watchdog(state)
        return "⏸️ Playback paused."

    async def resume(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        await self.calls.resume(chat_id)
        if state.current is not None:
            state.playback_paused = False
            if state.playback_remaining is not None:
                remaining = state.playback_remaining
                state.playback_remaining = None
                self._schedule_playback_watchdog(
                    state,
                    state.current,
                    remaining,
                    state.playback_epoch,
                )
        return "▶️ Playback resumed."

    async def skip(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        if state.current is not None:
            await self._cancel_playback_watchdog(state)
            state.playback_deadline = None
            state.playback_remaining = None
            state.playback_paused = False
            self._remove_track(state.current)
            state.current = None
        await self._play_next(state)
        if state.current is None:
            await self.calls.play(chat_id, None)
            return "⏭️ Skipped. The queue is empty."
        return f"⏭️ {self._now_playing_text(state)}"

    async def stop_ai_voice(self) -> None:
        """Cancel the optional AI voice worker without touching the VC."""
        stop_event = getattr(self, "_voice_ai_stop_event", None)
        if stop_event is not None:
            stop_event.set()
        task = getattr(self, "_voice_ai_task", None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._voice_ai_task = None
        self._voice_ai_stop_event = None
        self._voice_ai_enabled = False
        self._voice_ai_processing = False
        self._voice_ai_state = "IDLE"

    async def stop(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        # Invalidate any end-of-stream callback or in-flight start before
        # waiting for the transition lock. This prevents a queued track from
        # being started while .vcstop is taking control.
        state.playback_epoch += 1
        state.closing = True
        async with state.transition_lock:
            queued = list(state.queue)
            state.queue.clear()
            current = state.current
            # Clear the logical playback state before touching PyTgCalls so a
            # delayed StreamEnded update cannot advance the queue afterward.
            state.current = None
            try:
                await self._cancel_playback_watchdog(state)
                state.playback_deadline = None
                state.playback_remaining = None
                state.playback_paused = False
                if state.live_active:
                    state.live_active = False
                    await self._stop_live_sender(state)
                # PyTgCalls treats play(chat_id, None) as an empty source:
                # it stops its managed FFmpeg process while keeping the
                # existing group-call connection alive.
                await self.calls.play(chat_id, None)
            finally:
                for track in queued:
                    self._remove_track(track)
                if current is not None:
                    self._remove_track(current)
                state.closing = False
            state.live_active = False
            state.live_started_at = None
            state.live_last_frame_at = None
            state.live_push_active = False
        return "⏹️ Playback stopped; I am still in the voice chat."

    async def leave(self, chat_id: int) -> str:
        logger.info("[VC_LEAVE_TRACE] Leaving VC for chat_id=%s", chat_id)
        state = self._require_state(chat_id)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.stop_ai_voice(), timeout=2.0)
        state.closing = True
        try:
            if state.recording_path is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._stop_recording(state, chat_id, send_file=False), timeout=3.0)
            with contextlib.suppress(Exception):
                if state.live_active:
                    await asyncio.wait_for(self.stop_live(chat_id), timeout=3.0)
            with contextlib.suppress(Exception):
                logger.info("[VC_LEAVE_TRACE] calls.leave_call for chat_id=%s", chat_id)
                await asyncio.wait_for(self.calls.leave_call(chat_id), timeout=4.0)
        finally:
            self._clear_state(state)
            self.sessions.pop(chat_id, None)
            if self.state is state or (self.state is not None and self.state.chat_id == chat_id):
                self.state = None
        logger.info("[VC_LEAVE_TRACE] Left VC for chat_id=%s successfully", chat_id)
        return "👋 Left the voice chat and cleared the queue."

    async def change_volume(self, chat_id: int, value: int) -> str:
        state = self._require_state(chat_id)
        if not 0 <= value <= _MAX_VOLUME:
            raise ValueError(f"Volume must be between 0 and {_MAX_VOLUME}.")
        state.volume = value
        state.muted = value == 0
        return f"🔊 Playback gain set to {value}%."

    async def mute(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        await self.calls.mute(chat_id)
        state.muted = True
        return "🔇 Playback muted."

    async def unmute(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        await self.calls.unmute(chat_id)
        state.muted = False
        return "🔊 Playback unmuted."

    async def status(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        lines = [
            "🎙️ <b>Voice chat status</b>",
            f"Group: <b>{escape(state.chat_title or 'Unknown group')}</b>",
            f"Chat: <code>{state.chat_id}</code>",
            f"Connected for: <code>{_format_duration(time.monotonic() - state.joined_at)}</code>",
            f"Volume gain: <code>{state.volume}%</code>{' (muted)' if state.muted else ''}",
            (
                f"Now playing: <b>{escape(state.current.title)}</b>"
                if state.current
                else "Now playing: <i>nothing</i>"
            ),
            f"Queued: <code>{len(state.queue)}</code>",
        ]
        if state.recording_path is not None and state.recording_started_at is not None:
            lines.append(
                "Recording: <code>"
                f"{_format_duration(time.monotonic() - state.recording_started_at)}"
                "</code>"
            )
        return "\n".join(lines)

    async def control_status(self) -> str:
        """Status response for the private control bot."""
        if self.state is None:
            return "❌ Not connected to any Voice Chat."
        return await self.status(self.state.chat_id)

    async def queue_text(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        lines = ["📚 <b>Voice queue</b>"]
        if state.current:
            lines.append(f"▶️ <b>Now:</b> {escape(state.current.title)}")
        if state.queue:
            lines.extend(
                f"{i}. {escape(track.title)}"
                for i, track in enumerate(state.queue, 1)
            )
        else:
            lines.append("<i>The queue is empty.</i>")
        return "\n".join(lines)

    async def clear_queue(self, chat_id: int) -> str:
        state = self._require_state(chat_id)
        while state.queue:
            self._remove_track(state.queue.popleft())
        return "🧹 Queue cleared."

    async def start_recording(self, chat_id: int, seconds: int | None = None) -> str:
        state = self._ensure_single_connection(chat_id)
        if state.recording_path is not None:
            raise RuntimeError("A recording is already in progress. Use /record stop.")
        recording_dir = Path(tempfile.mkdtemp(prefix="recording-", dir=self._temp_dir))
        output = recording_dir / "voice-chat.mp3"
        try:
            await self.calls.record(chat_id, RecordStream(audio=output))
        except Exception:
            shutil.rmtree(recording_dir, ignore_errors=True)
            raise
        state.recording_path = output
        state.recording_started_at = time.monotonic()
        if seconds:
            state.recording_task = asyncio.create_task(
                self._timed_recording_stop(state, chat_id, seconds)
            )
        suffix = f" for {seconds}s" if seconds else ""
        return f"⏺️ Recording started{suffix}. Use /record stop when finished."

    async def _timed_recording_stop(self, state: VoiceState, chat_id: int, seconds: int):
        try:
            await asyncio.sleep(seconds)
            if self.state is state and state.recording_path is not None:
                await self._stop_recording(state, chat_id, send_file=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Timed voice-chat recording failed in %s.", chat_id)

    async def stop_recording(self, event, chat_id: int) -> str:
        state = self._require_state(chat_id)
        if state.recording_path is None:
            raise RuntimeError("No recording is in progress.")
        return await self._stop_recording(state, chat_id, send_file=True, event=event)

    async def _stop_recording(
        self,
        state: VoiceState,
        chat_id: int,
        send_file: bool,
        event=None,
    ) -> str:
        if (
            state.recording_task is not None
            and state.recording_task is not asyncio.current_task()
        ):
            state.recording_task.cancel()
            state.recording_task = None
        path = state.recording_path
        state.recording_path = None
        state.recording_started_at = None
        if path is None:
            return "No recording is in progress."
        with contextlib.suppress(Exception):
            await self.calls.play(chat_id, None)
        if not path.exists():
            shutil.rmtree(path.parent, ignore_errors=True)
            raise RuntimeError("The recording did not produce an audio file.")
        try:
            if send_file:
                target = event if event is not None else chat_id
                await self.client.send_file(
                    target,
                    path,
                    force_document=True,
                    caption="🎧 Voice chat recording",
                )
            return "⏹️ Recording stopped and processed."
        finally:
            shutil.rmtree(path.parent, ignore_errors=True)

    def _clear_state(self, state: VoiceState) -> None:
        while state.queue:
            self._remove_track(state.queue.popleft())
        if state.current:
            self._remove_track(state.current)
        state.current = None
        state.playback_deadline = None
        state.playback_remaining = None
        state.playback_paused = False
        if state.playback_watchdog is not None:
            state.playback_watchdog.cancel()
            state.playback_watchdog = None
        state.live_active = False
        if state.live_sender_task is not None:
            state.live_sender_task.cancel()
            state.live_sender_task = None
        state.live_frame_queue = None
        state.live_started_at = None
        state.live_last_frame_at = None
        state.live_push_active = False

    @staticmethod
    def _apply_bridge_gain(
        data: bytes,
        volume: int,
        bass: int = 0,
        bass_filter: BassFilter | None = None,
    ) -> bytes:
        """Apply volume multiplier and low-shelf bass boost to PCM16 samples."""
        if not data:
            return data
        usable_length = len(data) - (len(data) % 2)
        if usable_length <= 0:
            return b""
        samples = array("h")
        samples.frombytes(data[:usable_length])

        if bass > 0 and bass_filter is not None:
            bass_filter.process_samples(samples, bass)

        if volume == 0:
            samples = array("h", [0]) * len(samples)
        elif volume != 100:
            multiplier = volume / 100.0
            for index, sample in enumerate(samples):
                amplified = int(round(sample * multiplier))
                samples[index] = max(-32768, min(32767, amplified))

        return samples.tobytes()

    async def _send_bridge_frames(self, bridge: VoiceBridge) -> None:
        """Continuously relay audio frames from source VC to target VC."""
        queue = bridge.queue
        chunk_size = _LIVE_FRAME_BYTES
        try:
            while bridge.active:
                data = await queue.get()
                if not data or not bridge.active:
                    continue
                if bridge.muted:
                    continue
                transformed = self._apply_bridge_gain(
                    data,
                    bridge.volume,
                    bridge.bass,
                    bridge.bass_filter,
                )
                if not transformed:
                    continue
                offset = 0
                while offset < len(transformed):
                    chunk = transformed[offset : offset + chunk_size]
                    if len(chunk) == chunk_size:
                        await self.calls.send_frame(
                            bridge.target_chat_id,
                            Device.MICROPHONE,
                            chunk,
                        )
                        bridge.relayed_frames += 1
                        bridge.relayed_bytes += len(chunk)
                    elif len(chunk) > 0 and len(chunk) % 2 == 0:
                        padded = chunk.ljust(chunk_size, b"\x00")
                        await self.calls.send_frame(
                            bridge.target_chat_id,
                            Device.MICROPHONE,
                            padded,
                        )
                        bridge.relayed_frames += 1
                        bridge.relayed_bytes += len(padded)
                    offset += chunk_size

                bridge.last_frame_at = time.monotonic()
                if bridge.relayed_frames == 1 or bridge.relayed_frames % 200 == 0:
                    logger.info(
                        "Relayed bridge frame %s -> %s: frames=%d bytes=%d volume=%d%% bass=%d queue=%d.",
                        bridge.source_chat_id,
                        bridge.target_chat_id,
                        bridge.relayed_frames,
                        bridge.relayed_bytes,
                        bridge.volume,
                        bridge.bass,
                        queue.qsize(),
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Audio bridge relay error forwarding to target %s.",
                bridge.target_chat_id,
            )

    async def join_bridge(self, source_chat_id: int, target_identifier: str) -> str:
        """Connect to source VC and target VC, then establish audio bridge."""
        t_start = time.monotonic()
        logger.info(
            "[VC_JOIN_TRACE] join_bridge initiated: source_chat_id=%s target=%s",
            source_chat_id,
            target_identifier,
        )
        if not target_identifier:
            raise ValueError("Usage: /join <group username or chat ID>")

        t0 = time.monotonic()
        await self.start()
        logger.info("[VC_JOIN_TRACE] Step 0: Ensure PyTgCalls running took %.2fs", time.monotonic() - t0)

        # 1. Resolve source entity & ensure connected to source VC
        t0 = time.monotonic()
        logger.info("[VC_JOIN_TRACE] Step 1: Resolving source group %s...", source_chat_id)
        try:
            source_entity = await asyncio.wait_for(self.client.get_entity(source_chat_id), timeout=10.0)
        except Exception as exc:
            logger.error("[VC_JOIN_TRACE] Failed to resolve source group %s: %s", source_chat_id, exc)
            raise RuntimeError(f"Could not resolve private control group: {exc}") from exc
        logger.info("[VC_JOIN_TRACE] Step 1a: get_entity(source) took %.2fs", time.monotonic() - t0)

        t0 = time.monotonic()
        source_call = await asyncio.wait_for(self._active_group_call(source_entity), timeout=10.0)
        logger.info(
            "[VC_JOIN_TRACE] Step 1b: _active_group_call(source) took %.2fs (call=%s)",
            time.monotonic() - t0,
            source_call is not None,
        )
        if source_call is None:
            raise VoiceBridgeNoActiveGroupCall(
                "No active Voice Chat in this private control group. Start a Voice Chat here first."
            )

        # 2. Resolve target entity
        target_token = target_identifier.strip()
        t0 = time.monotonic()
        logger.info("[VC_JOIN_TRACE] Step 2: Resolving target group %s...", target_token)
        try:
            target_entity = await asyncio.wait_for(
                self.client.get_entity(
                    int(target_token) if target_token.lstrip("-").isdigit() else target_token.lstrip("@")
                ),
                timeout=10.0,
            )
        except Exception as exc:
            logger.error("[VC_JOIN_TRACE] Failed to resolve target %s: %s", target_token, exc)
            raise ValueError(
                "Could not find that target group. Use a group username or numeric chat ID."
            ) from exc
        logger.info("[VC_JOIN_TRACE] Step 2a: get_entity(target) took %.2fs", time.monotonic() - t0)

        if (
            not isinstance(target_entity, tl_types.Chat)
            and not (
                isinstance(target_entity, tl_types.Channel)
                and bool(getattr(target_entity, "megagroup", False))
            )
        ):
            raise ValueError("The target must be a group or supergroup.")

        t0 = time.monotonic()
        target_call = await asyncio.wait_for(self._active_group_call(target_entity), timeout=10.0)
        logger.info(
            "[VC_JOIN_TRACE] Step 2b: _active_group_call(target) took %.2fs (call=%s)",
            time.monotonic() - t0,
            target_call is not None,
        )
        if target_call is None:
            raise VoiceBridgeNoActiveGroupCall(
                f"The target group ({_safe_title(getattr(target_entity, 'title', None)) or target_token}) has no active Voice Chat."
            )

        target_chat_id = int(get_peer_id(target_entity))
        if target_chat_id == source_chat_id:
            raise ValueError("Target Voice Chat cannot be the private control group itself.")

        if (
            self.bridge is not None
            and self.bridge.active
            and self.bridge.target_chat_id == target_chat_id
        ):
            return (
                f"✅ Hosted account is already connected to target Voice Chat "
                f"<b>{escape(self.bridge.target_state.chat_title)}</b> (<code>{target_chat_id}</code>) "
                f"with active audio bridge."
            )

        if self.bridge is not None:
            t0 = time.monotonic()
            logger.info("[VC_JOIN_TRACE] Stopping existing active bridge...")
            await self._stop_bridge(self.bridge, leave_target=True)
            logger.info("[VC_JOIN_TRACE] Stopping existing bridge took %.2fs", time.monotonic() - t0)

        # 3. Setup PulseAudio virtual sink and HTTP streaming bridge
        t0 = time.monotonic()
        sink_name = getattr(self, "pulse_sink_name", "vcrelay")
        logger.info("[VC_JOIN_TRACE] Step 3: Setting up PulseAudio virtual sink %s...", sink_name)
        try:
            monitor_source = await asyncio.wait_for(ensure_virtual_sink(sink_name), timeout=10.0)
        except Exception as exc:
            logger.error("[VC_JOIN_TRACE] Failed to setup virtual sink %s: %s", sink_name, exc)
            raise RuntimeError(f"Could not setup audio relay sink: {exc}") from exc
        logger.info("[VC_JOIN_TRACE] Step 3a: Virtual sink ready: %s (took %.2fs)", monitor_source, time.monotonic() - t0)

        t0 = time.monotonic()
        await self.audio_bridge.start()
        logger.info("[VC_JOIN_TRACE] Step 3b: Audio HTTP bridge listening (took %.2fs)", time.monotonic() - t0)

        # 4. Connect to source VC (feeding silence so connection stays open and plays incoming audio to virtual sink)
        t0 = time.monotonic()
        silence_key = f"silence_{source_chat_id}"
        silence_cmd = build_silence_command_stdout()
        logger.info("[VC_JOIN_TRACE] Step 4: Registering silence stream...")
        silence_url = await self.audio_bridge.register_stream(
            silence_key,
            silence_cmd,
            name=f"silence-{source_chat_id}",
            source_id=source_chat_id,
            target_id=target_chat_id,
        )
        logger.info("[VC_JOIN_TRACE] Step 4a: Silence URL=%s (took %.2fs)", silence_url, time.monotonic() - t0)

        t0 = time.monotonic()
        source_stream = MediaStream(
            silence_url,
            AudioQuality.STUDIO,
            video_flags=MediaStream.Flags.IGNORE,
        )
        logger.info("[VC_JOIN_TRACE] Step 4b: Connecting to source VC %s via PyTgCalls...", source_chat_id)
        try:
            await asyncio.wait_for(self.calls.play(source_chat_id, source_stream), timeout=25.0)
        except NoActiveGroupCall as exc:
            await self.audio_bridge.remove_stream(silence_key)
            raise VoiceBridgeNoActiveGroupCall(
                "No active Voice Chat in this private control group. Start a Voice Chat here first."
            ) from exc
        except Exception as exc:
            await self.audio_bridge.remove_stream(silence_key)
            logger.error("[VC_JOIN_TRACE] Failed connecting to source VC %s: %s", source_chat_id, exc)
            raise RuntimeError(f"Could not connect to private group Voice Chat: {exc}") from exc
        logger.info("[VC_JOIN_TRACE] Step 4b: Connected to source VC %s (took %.2fs)", source_chat_id, time.monotonic() - t0)

        source_state = VoiceState(
            chat_id=source_chat_id,
            chat_title=_safe_title(getattr(source_entity, "title", None)),
            volume=_SAFE_DEFAULT_VOLUME,
        )
        self.sessions[source_chat_id] = source_state
        logger.info(
            "[VC_BRIDGE] source_joined chat_id=%s title=%s",
            source_chat_id,
            source_state.chat_title,
        )

        # 5. Connect to target VC with live capture stream from virtual sink monitor
        t0 = time.monotonic()
        capture_key = f"capture_{target_chat_id}"
        capture_cmd = build_capture_command_stdout(
            monitor_source,
            level=_BRIDGE_DEFAULT_LEVEL,
            bass=0,
            muted=False,
        )
        logger.info("[VC_JOIN_TRACE] Step 5: Registering capture stream...")
        capture_url = await self.audio_bridge.register_stream(
            capture_key,
            capture_cmd,
            name=f"capture-{target_chat_id}",
            source_id=source_chat_id,
            target_id=target_chat_id,
        )
        logger.info("[VC_JOIN_TRACE] Step 5a: Capture URL=%s (took %.2fs)", capture_url, time.monotonic() - t0)

        t0 = time.monotonic()
        target_stream = MediaStream(
            capture_url,
            AudioQuality.STUDIO,
            video_flags=MediaStream.Flags.IGNORE,
        )
        logger.info("[VC_JOIN_TRACE] Step 5b: Connecting to target VC %s via PyTgCalls...", target_chat_id)
        try:
            await asyncio.wait_for(self.calls.play(target_chat_id, target_stream), timeout=25.0)
        except NoActiveGroupCall as exc:
            await self.audio_bridge.remove_stream(capture_key)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.calls.leave_call(source_chat_id), timeout=4.0)
            await self.audio_bridge.remove_stream(silence_key)
            self.sessions.pop(source_chat_id, None)
            raise VoiceBridgeNoActiveGroupCall(
                f"The target group ({_safe_title(getattr(target_entity, 'title', None)) or target_token}) has no active Voice Chat."
            ) from exc
        except Exception as exc:
            await self.audio_bridge.remove_stream(capture_key)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.calls.leave_call(source_chat_id), timeout=4.0)
            await self.audio_bridge.remove_stream(silence_key)
            self.sessions.pop(source_chat_id, None)
            logger.error("[VC_JOIN_TRACE] Failed connecting to target VC %s: %s", target_chat_id, exc)
            raise RuntimeError(f"Could not connect to target Voice Chat: {exc}") from exc
        logger.info("[VC_JOIN_TRACE] Step 5b: Connected to target VC %s (took %.2fs)", target_chat_id, time.monotonic() - t0)

        target_state = VoiceState(
            chat_id=target_chat_id,
            chat_title=_safe_title(getattr(target_entity, "title", None)),
            volume=_SAFE_DEFAULT_VOLUME,
        )
        self.sessions[target_chat_id] = target_state
        self.state = target_state
        logger.info(
            "[VC_BRIDGE] target_joined chat_id=%s title=%s",
            target_chat_id,
            target_state.chat_title,
        )

        # 6. Create bounded relay queue (max 20 frames = ~200ms buffer)
        t0 = time.monotonic()
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=_BRIDGE_QUEUE_SIZE)
        bridge = VoiceBridge(
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            source_state=source_state,
            target_state=target_state,
            queue=queue,
            active=True,
            volume=100,
            level=_BRIDGE_DEFAULT_LEVEL,
            bass=0,
            monitor_source=monitor_source,
            silence_key=silence_key,
            capture_key=capture_key,
            silence_url=silence_url,
            capture_url=capture_url,
        )
        bridge.relay_task = asyncio.create_task(
            self._send_bridge_frames(bridge),
            name=f"bridge-audio-{source_chat_id}-to-{target_chat_id}",
        )
        self._tasks.add(bridge.relay_task)
        bridge.relay_task.add_done_callback(self._tasks.discard)

        bridge.pulse_watchdog_task = asyncio.create_task(
            self._pulse_routing_watchdog(bridge, sink_name),
            name=f"pulse-routing-{source_chat_id}",
        )
        self._tasks.add(bridge.pulse_watchdog_task)
        bridge.pulse_watchdog_task.add_done_callback(self._tasks.discard)

        self.bridge = bridge

        total_elapsed = time.monotonic() - t_start
        logger.info(
            "[VC_JOIN_TRACE] TOTAL join_bridge completed in %.2fs (source=%s target=%s).",
            total_elapsed,
            source_chat_id,
            target_chat_id,
        )
        return (
            f"✅ Hosted account joined target Voice Chat <b>{escape(target_state.chat_title)}</b> "
            f"(<code>{target_chat_id}</code>) and connected audio bridge from private VC."
        )

    async def _restart_bridge_capture(self, bridge: VoiceBridge) -> None:
        if not bridge.active or not bridge.capture_key or not bridge.monitor_source:
            return
        async with bridge.lock:
            cmd = build_capture_command_stdout(
                bridge.monitor_source,
                level=bridge.level,
                bass=bridge.bass,
                muted=bridge.muted,
            )
            await self.audio_bridge.register_stream(
                bridge.capture_key,
                cmd,
                name=f"capture-{bridge.target_chat_id}",
            )

    async def _pulse_routing_watchdog(self, bridge: VoiceBridge, sink_name: str) -> None:
        """Periodically route any active PulseAudio sink-inputs to vcrelay."""
        try:
            while bridge.active:
                await asyncio.sleep(2.0)
                if not bridge.active:
                    break
                await route_sink_inputs_to_vcrelay(sink_name)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("Error in _pulse_routing_watchdog: %s", exc)

    async def _stop_bridge(self, bridge: VoiceBridge, leave_target: bool = True) -> None:
        logger.info(
            "[VC_LEAVE_TRACE] _stop_bridge start: source=%s target=%s leave_target=%s",
            bridge.source_chat_id,
            bridge.target_chat_id,
            leave_target,
        )
        bridge.active = False
        if bridge.pulse_watchdog_task is not None and bridge.pulse_watchdog_task is not asyncio.current_task():
            bridge.pulse_watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(bridge.pulse_watchdog_task, timeout=2.0)
            bridge.pulse_watchdog_task = None
        if bridge.relay_task is not None and bridge.relay_task is not asyncio.current_task():
            bridge.relay_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(bridge.relay_task, timeout=2.0)
            bridge.relay_task = None
        while not bridge.queue.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                bridge.queue.get_nowait()
        if bridge.capture_key:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.audio_bridge.remove_stream(bridge.capture_key), timeout=3.0)
        if bridge.silence_key:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.audio_bridge.remove_stream(bridge.silence_key), timeout=3.0)
        if leave_target:
            logger.info("[VC_LEAVE_TRACE] Calling calls.leave_call on target VC %s", bridge.target_chat_id)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.calls.leave_call(bridge.target_chat_id), timeout=4.0)
            self.sessions.pop(bridge.target_chat_id, None)
            if self.state is bridge.target_state:
                self.state = None
        logger.info("[VC_LEAVE_TRACE] Calling calls.leave_call on source VC %s", bridge.source_chat_id)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.calls.leave_call(bridge.source_chat_id), timeout=4.0)
        self.sessions.pop(bridge.source_chat_id, None)
        if self.bridge is bridge:
            self.bridge = None
        logger.info("[VC_LEAVE_TRACE] _stop_bridge completed successfully")

    async def leave_bridge(self, source_chat_id: int | None = None) -> str:
        logger.info("[VC_LEAVE_TRACE] leave_bridge called (source_chat_id=%s)", source_chat_id)
        if self.bridge is None:
            if self.state is not None:
                return await self.leave(self.state.chat_id)
            return "ℹ️ No active target Voice Chat or bridge."
        target_chat_id = self.bridge.target_chat_id
        target_title = self.bridge.target_state.chat_title
        await self._stop_bridge(self.bridge, leave_target=True)
        logger.info("[VC_LEAVE_TRACE] leave_bridge finished for target %s", target_chat_id)
        return f"👋 Hosted account left target Voice Chat <b>{escape(target_title)}</b> (<code>{target_chat_id}</code>) and stopped audio bridge."

    async def leave_all(self) -> str:
        logger.info("[VC_LEAVE_TRACE] leave_all initiated")
        if self.bridge is not None:
            with contextlib.suppress(Exception):
                await self._stop_bridge(self.bridge, leave_target=True)
        for chat_id in list(self.sessions.keys()):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.leave(chat_id), timeout=4.0)
        if self.state is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.leave(self.state.chat_id), timeout=4.0)
        self.sessions.clear()
        self.state = None
        logger.info("[VC_LEAVE_TRACE] leave_all completed")
        return "👋 Hosted account left all Voice Chats and cleared all sessions."

    async def stop_all_playback(self) -> str:
        if self.bridge is not None:
            with contextlib.suppress(Exception):
                await self.stop(self.bridge.target_chat_id)
            with contextlib.suppress(Exception):
                await self.stop(self.bridge.source_chat_id)
            return "⏹️ Playback stopped in active Voice Chat sessions."
        if self.state is not None:
            return await self.stop(self.state.chat_id)
        return "ℹ️ No active Voice Chat playback to stop."

    async def set_level(self, value: int) -> str:
        if not 1 <= value <= 25:
            raise ValueError("Level must be between 1 and 25.")
        volume = value * 20
        if self.bridge is not None:
            self.bridge.level = value
            self.bridge.volume = volume
            await self._restart_bridge_capture(self.bridge)
        if self.state is not None:
            self.state.volume = volume
        return f"🎚 Level set to {value}/25."

    async def set_bass(self, value: int, chat_id: int | None = None) -> str:
        if not 0 <= value <= 15:
            raise ValueError("Bass must be between 0 and 15.")
        if self.bridge is not None:
            self.bridge.bass = value
            await self._restart_bridge_capture(self.bridge)
        state = self.get_state(chat_id)
        if state is not None:
            state.bass = value
        return f"🎚 Bass level set to {value}/15."

    async def mute_bridge(self) -> str:
        if self.bridge is not None:
            self.bridge.muted = True
            await self._restart_bridge_capture(self.bridge)
            with contextlib.suppress(Exception):
                await self.calls.mute(self.bridge.target_chat_id)
            return "🔇 Target Voice Chat stream muted."
        if self.state is not None:
            return await self.mute(self.state.chat_id)
        return "ℹ️ No active Voice Chat session to mute."

    async def unmute_bridge(self) -> str:
        if self.bridge is not None:
            self.bridge.muted = False
            await self._restart_bridge_capture(self.bridge)
            with contextlib.suppress(Exception):
                await self.calls.unmute(self.bridge.target_chat_id)
            return "🔊 Target Voice Chat stream unmuted."
        if self.state is not None:
            return await self.unmute(self.state.chat_id)
        return "ℹ️ No active Voice Chat session to unmute."

    async def start_record_target(self) -> str:
        target_id = self.bridge.target_chat_id if self.bridge is not None else (self.state.chat_id if self.state is not None else None)
        if target_id is None:
            raise RuntimeError("No active target Voice Chat session. Use /join <group> first.")
        return await self.start_recording(target_id)

    async def stop_record_target(self, event=None) -> str:
        target_id = self.bridge.target_chat_id if self.bridge is not None else (self.state.chat_id if self.state is not None else None)
        if target_id is None:
            raise RuntimeError("No active Voice Chat session.")
        state = self._require_state(target_id)
        if state.recording_path is None:
            raise RuntimeError("No recording is currently in progress.")
        return await self._stop_recording(state, target_id, send_file=True, event=event)

    async def shutdown(self) -> None:
        await self.stop_ai_voice()
        if self.bridge is not None:
            with contextlib.suppress(Exception):
                await self._stop_bridge(self.bridge, leave_target=True)
        with contextlib.suppress(Exception):
            await self.audio_bridge.stop()
        with contextlib.suppress(Exception):
            await teardown_virtual_sink(self.pulse_sink_name)
        for chat_id, state in list(self.sessions.items()):
            state.closing = True
            if state.recording_path is not None:
                with contextlib.suppress(Exception):
                    await self._stop_recording(state, chat_id, send_file=False)
            with contextlib.suppress(Exception):
                if state.live_active:
                    await self.stop_live(chat_id)
            with contextlib.suppress(Exception):
                await self.calls.leave_call(chat_id)
            self._clear_state(state)
        self.sessions.clear()
        if self.state is not None:
            state = self.state
            state.closing = True
            if state.recording_path is not None:
                with contextlib.suppress(Exception):
                    await self._stop_recording(state, state.chat_id, send_file=False)
            with contextlib.suppress(Exception):
                if state.live_active:
                    await self.stop_live(state.chat_id)
            with contextlib.suppress(Exception):
                await self.calls.leave_call(state.chat_id)
            self._clear_state(state)
            self.state = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        shutil.rmtree(self._temp_dir, ignore_errors=True)


_manager: VoiceChatManager | None = None


async def init(client_instance):
    global _manager
    previous = getattr(client_instance, "_voice_chat_manager", None)
    if previous is not None:
        with contextlib.suppress(Exception):
            await previous.shutdown()
    manager = VoiceChatManager(client_instance)
    _manager = manager
    setattr(client_instance, "_voice_chat_manager", manager)


async def register_commands():
    if _manager is None:
        raise RuntimeError("Voice-chat manager could not load.")
    add_handler(
        "voice_chat",
        [
            ".vcjoin <group> — Join an active group Voice Chat from the private control bot",
            ".vcstatus — Show the connected group and playback status",
            ".vcstop — Stop playback and clear the queue without leaving",
            ".vcleave — Leave and clear the Voice Chat",
            ".play — Play replied audio in the connected Voice Chat",
            ".pause / .resume / .queue / .clearqueue — Playback controls",
            ".volume <0-100000000> / .mute / .unmute — Gain-only playback controls",
        ],
        "Private control-bot Voice Chat playback and gain-only controls",
    )