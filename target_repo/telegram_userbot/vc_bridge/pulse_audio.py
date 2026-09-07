"""PulseAudio virtual sink manager for Telegram VC-to-VC audio relay."""

import asyncio
import logging
import os
import shutil
import tempfile
from typing import Optional, Tuple

logger = logging.getLogger("telegram_userbot.vc_bridge.pulse_audio")

DEFAULT_SINK_NAME = "vcrelay"
MODULE_OWNER_DESCRIPTION = "telegram_userbot_vc_bridge"

_daemon_process: Optional[asyncio.subprocess.Process] = None


async def _run(*cmd: str) -> Tuple[asyncio.subprocess.Process, str, str]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process, stdout.decode().strip(), stderr.decode().strip()


def pulseaudio_available() -> bool:
    return shutil.which("pactl") is not None


async def pulseaudio_daemon_reachable() -> Tuple[bool, str]:
    """Check if pactl can communicate with a running PulseAudio daemon."""
    if not pulseaudio_available():
        return False, "pactl not installed"
    try:
        _, out, err = await _run("pactl", "info")
        if err:
            return False, err
        return True, out
    except Exception as exc:
        return False, str(exc)


def _prepare_runtime_environment() -> None:
    """Ensure XDG_RUNTIME_DIR and PULSE_ALLOW_ROOT are configured."""
    uid = os.getuid()
    if not os.environ.get("XDG_RUNTIME_DIR"):
        runtime_dir = f"/tmp/pulse-runtime-{uid}"
        os.makedirs(runtime_dir, mode=0o700, exist_ok=True)
        os.environ["XDG_RUNTIME_DIR"] = runtime_dir
    if uid == 0:
        os.environ["PULSE_ALLOW_ROOT"] = "1"


async def _drain_daemon_stderr(process: asyncio.subprocess.Process) -> None:
    if process.stderr is None:
        return
    try:
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            text = line.decode(errors="ignore").rstrip()
            if text:
                logger.debug("[pulseaudio] %s", text)
    except Exception:
        pass


def _write_minimal_pulse_config(sink_name: str) -> str:
    """Write minimal pulse config skipping D-Bus and udev modules."""
    config_path = os.path.join(tempfile.gettempdir(), "pulse-minimal.pa")
    content = (
        "load-module module-native-protocol-unix\n"
        f"load-module module-null-sink sink_name={sink_name} "
        f"sink_properties=device.description={MODULE_OWNER_DESCRIPTION}\n"
    )
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(content)
    return config_path


async def _find_pulse_module_dir() -> Optional[str]:
    _, out, _ = await _run(
        "bash",
        "-c",
        "find /app/.apt /usr -type d -path '*pulse-*/modules' 2>/dev/null | head -n1",
    )
    return out.strip() or None


async def _apply_discovered_library_paths(module_dir: Optional[str]) -> None:
    _, out, _ = await _run(
        "bash",
        "-c",
        "find /app/.apt -name '*.so*' "
        r"\( -iname '*pulse*' -o -iname 'libprotocol-native*' \) "
        "-printf '%h\\n' 2>/dev/null | sort -u",
    )
    dirs = [d for d in out.splitlines() if d.strip()]
    if module_dir:
        dirs.append(module_dir)
        dirs.append(os.path.dirname(module_dir))

    if not dirs:
        return

    existing = os.environ.get("LD_LIBRARY_PATH", "")
    ordered_unique = list(dict.fromkeys(dirs + ([existing] if existing else [])))
    os.environ["LD_LIBRARY_PATH"] = ":".join(ordered_unique)
    logger.debug("Updated LD_LIBRARY_PATH with PulseAudio library dirs: %s", dirs)


async def _start_daemon(sink_name: str = DEFAULT_SINK_NAME) -> None:
    global _daemon_process
    _prepare_runtime_environment()
    logger.info(
        "Starting PulseAudio daemon automatically (XDG_RUNTIME_DIR=%s)...",
        os.environ.get("XDG_RUNTIME_DIR"),
    )

    if _daemon_process is not None and _daemon_process.returncode is None:
        logger.info("PulseAudio process is already running.")
        await asyncio.sleep(1.5)
        return

    minimal_config_path = _write_minimal_pulse_config(sink_name)
    module_dir = await _find_pulse_module_dir()
    await _apply_discovered_library_paths(module_dir)

    cmd = [
        "pulseaudio",
        "-n",
        f"--file={minimal_config_path}",
        "--daemonize=no",
        "--exit-idle-time=-1",
        "--disallow-exit",
    ]
    if module_dir:
        cmd.append(f"--dl-search-path={module_dir}")

    _daemon_process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    asyncio.create_task(_drain_daemon_stderr(_daemon_process))
    await asyncio.sleep(1.5)


async def ensure_virtual_sink(sink_name: str = DEFAULT_SINK_NAME) -> str:
    """Ensure PulseAudio daemon is running and null sink exists.

    Returns the device name of the monitor source, e.g. 'vcrelay.monitor'.
    """
    if not pulseaudio_available():
        raise RuntimeError(
            "pactl / pulseaudio is not installed. "
            "Please install it with: apt-get install -y pulseaudio pulseaudio-utils"
        )

    _prepare_runtime_environment()
    reachable, info_or_error = await pulseaudio_daemon_reachable()
    if not reachable:
        logger.warning(
            "PulseAudio daemon not reachable (%s). Starting automatically...",
            info_or_error,
        )
        await _start_daemon(sink_name)
        reachable, info_or_error = await pulseaudio_daemon_reachable()

    if not reachable:
        raise RuntimeError(
            f"PulseAudio daemon is not reachable after automatic start: {info_or_error}"
        )

    _, existing_sinks, _ = await _run("pactl", "list", "short", "sinks")
    if not any(
        line.split("\t")[1] == sink_name
        for line in existing_sinks.splitlines()
        if "\t" in line
    ):
        _, out, err = await _run(
            "pactl",
            "load-module",
            "module-null-sink",
            f"sink_name={sink_name}",
            f"sink_properties=device.description={MODULE_OWNER_DESCRIPTION}",
        )
        if err:
            logger.error("Failed to create PulseAudio sink '%s': %s", sink_name, err)
        else:
            logger.info("Created PulseAudio virtual sink '%s' (module id=%s).", sink_name, out)
    else:
        logger.debug("PulseAudio virtual sink '%s' already exists.", sink_name)

    return f"{sink_name}.monitor"


async def teardown_virtual_sink(sink_name: str = DEFAULT_SINK_NAME) -> None:
    if not pulseaudio_available():
        return
    _prepare_runtime_environment()
    _, modules, _ = await _run("pactl", "list", "short", "modules")
    for line in modules.splitlines():
        if not line or "\t" not in line:
            continue
        parts = line.split("\t")
        module_id = parts[0]
        module_name = parts[1]
        args = parts[2] if len(parts) > 2 else ""
        if module_name == "module-null-sink" and f"sink_name={sink_name}" in args:
            await _run("pactl", "unload-module", module_id)
            logger.info("Unloaded PulseAudio sink '%s' (module id=%s).", sink_name, module_id)
