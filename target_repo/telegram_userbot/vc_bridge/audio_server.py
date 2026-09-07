"""Local HTTP streaming bridge for PyTgCalls audio relay."""

import asyncio
import logging
import socket
import struct
from typing import Dict, List, Optional
from aiohttp import web

logger = logging.getLogger("telegram_userbot.vc_bridge.audio_server")

SAMPLE_RATE = 48000
CHANNELS = 2
BITS_PER_SAMPLE = 16
CHUNK_SIZE = 3840  # 20ms at 48000Hz stereo 16-bit


def _wav_header(data_size: int = 0x7FFFFFFF) -> bytes:
    """44-byte canonical WAV header for 48kHz stereo s16le PCM."""
    byte_rate = SAMPLE_RATE * CHANNELS * BITS_PER_SAMPLE // 8
    block_align = CHANNELS * BITS_PER_SAMPLE // 8
    return b"".join([
        b"RIFF",
        struct.pack("<I", data_size + 36),
        b"WAVE",
        b"fmt ",
        struct.pack("<IHHIIHH", 16, 1, CHANNELS, SAMPLE_RATE, byte_rate, block_align, BITS_PER_SAMPLE),
        b"data",
        struct.pack("<I", data_size),
    ])


class _LiveStream:
    def __init__(self, cmd: List[str], name: str):
        self.cmd = cmd
        self.name = name
        self.process: Optional[asyncio.subprocess.Process] = None
        self.subscribers: List[asyncio.Queue] = []
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self.process = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader_task = asyncio.create_task(
            self._read_stdout(),
            name=f"livestream-reader-{self.name}",
        )
        self._stderr_task = asyncio.create_task(
            self._read_stderr(),
            name=f"livestream-stderr-{self.name}",
        )

    async def _read_stdout(self) -> None:
        assert self.process is not None
        assert self.process.stdout is not None
        try:
            while True:
                chunk = await self.process.stdout.read(CHUNK_SIZE)
                if not chunk:
                    break
                for q in list(self.subscribers):
                    try:
                        q.put_nowait(chunk)
                    except asyncio.QueueFull:
                        # Drop oldest chunk to maintain real-time low latency
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            q.put_nowait(chunk)
                        except asyncio.QueueFull:
                            pass
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Error reading stdout from stream %s", self.name)

    async def _read_stderr(self) -> None:
        assert self.process is not None
        assert self.process.stderr is not None
        try:
            while True:
                line = await self.process.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="ignore").rstrip()
                if text:
                    logger.debug("[ffmpeg:%s] %s", self.name, text)
        except Exception:
            pass

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self.subscribers:
            self.subscribers.remove(q)

    def is_alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def stop(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
            self._reader_task = None
        if self._stderr_task:
            self._stderr_task.cancel()
            self._stderr_task = None
        if self.process and self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()


class AudioHTTPBridge:
    """Serves ffmpeg streams as HTTP WAV endpoints for PyTgCalls."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8600):
        self.host = host
        self.port = port
        self._streams: Dict[str, _LiveStream] = {}
        self._app = web.Application()
        self._app.router.add_get("/audio/{key}", self._handle)
        self._runner: Optional[web.AppRunner] = None
        self._started = False

    def url_for(self, key: str) -> str:
        return f"http://{self.host}:{self.port}/audio/{key}"

    def _find_available_port(self, preferred_port: int) -> int:
        for p in range(preferred_port, preferred_port + 50):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind((self.host, p))
                    return p
                except OSError:
                    continue
        return preferred_port

    async def start(self) -> None:
        if self._started:
            return
        actual_port = self._find_available_port(self.port)
        self.port = actual_port
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self._started = True
        logger.info("Audio HTTP bridge listening on http://%s:%s", self.host, self.port)

    async def stop(self) -> None:
        for stream in list(self._streams.values()):
            await stream.stop()
        self._streams.clear()
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._started = False

    async def register_stream(self, key: str, cmd: List[str], name: str) -> str:
        if not self._started:
            await self.start()
        await self.remove_stream(key)
        stream = _LiveStream(cmd, name)
        await stream.start()
        self._streams[key] = stream
        return self.url_for(key)

    async def remove_stream(self, key: str) -> None:
        stream = self._streams.pop(key, None)
        if stream:
            await stream.stop()

    def is_stream_alive(self, key: str) -> bool:
        stream = self._streams.get(key)
        return stream.is_alive() if stream else False

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        key = request.match_info["key"]
        stream = self._streams.get(key)
        if stream is None:
            raise web.HTTPNotFound()

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "audio/wav",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)
        await response.write(_wav_header())

        q = stream.subscribe()
        try:
            while True:
                chunk = await q.get()
                await response.write(chunk)
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            stream.unsubscribe(q)
        return response
