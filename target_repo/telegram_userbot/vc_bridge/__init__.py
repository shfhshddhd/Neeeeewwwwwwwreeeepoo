"""VC-to-VC Audio Relay Bridge package."""

from .audio_server import AudioHTTPBridge
from .ffmpeg_utils import (
    build_audio_filters,
    build_capture_command_stdout,
    build_silence_command_stdout,
)
from .pulse_audio import (
    ensure_pulseaudio_ready,
    ensure_virtual_sink,
    log_pulseaudio_diagnostics,
    parse_sink_inputs,
    pulseaudio_available,
    pulseaudio_daemon_reachable,
    route_sink_inputs_to_vcrelay,
    set_default_pulse_sink_and_source,
    teardown_virtual_sink,
)

__all__ = [
    "AudioHTTPBridge",
    "build_audio_filters",
    "build_capture_command_stdout",
    "build_silence_command_stdout",
    "ensure_pulseaudio_ready",
    "ensure_virtual_sink",
    "log_pulseaudio_diagnostics",
    "parse_sink_inputs",
    "pulseaudio_available",
    "pulseaudio_daemon_reachable",
    "route_sink_inputs_to_vcrelay",
    "set_default_pulse_sink_and_source",
    "teardown_virtual_sink",
]
