"""FFmpeg audio filters and command builders for Telegram VC-to-VC audio relay."""

from typing import List

SAMPLE_RATE = 48000
CHANNELS = 2
MIN_LEVEL = 1
MAX_LEVEL = 25
MIN_BASS = 0
MAX_BASS = 15


def build_audio_filters(level: int, bass: int, muted: bool = False) -> str:
    """Translate level (1-25) and bass (0-15) into an FFmpeg audio filter chain."""
    if muted:
        return "volume=0"

    level = max(MIN_LEVEL, min(MAX_LEVEL, level))
    bass = max(MIN_BASS, min(MAX_BASS, bass))

    volume_multiplier = round(level / 5, 3)
    bass_gain_db = bass * 2

    filters = [f"volume={volume_multiplier}"]
    if bass_gain_db > 0:
        filters.append(f"bass=g={bass_gain_db}:f=110:w=0.6")
    filters.append("alimiter=limit=0.95")

    return ",".join(filters)


def build_silence_command_stdout() -> List[str]:
    """Generate stereo 48kHz silence to pipe:1 for keeping the source VC connection active."""
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=channel_layout=stereo:sample_rate={SAMPLE_RATE}",
        "-ac",
        str(CHANNELS),
        "-ar",
        str(SAMPLE_RATE),
        "-f",
        "s16le",
        "pipe:1",
    ]


def build_capture_command_stdout(
    monitor_source: str,
    level: int,
    bass: int,
    muted: bool = False,
) -> List[str]:
    """Capture raw PCM from PulseAudio monitor source with volume/bass/limiter filters to pipe:1."""
    filters = build_audio_filters(level, bass, muted)
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-f",
        "pulse",
        "-i",
        monitor_source,
        "-af",
        filters,
        "-ac",
        str(CHANNELS),
        "-ar",
        str(SAMPLE_RATE),
        "-f",
        "s16le",
        "pipe:1",
    ]
