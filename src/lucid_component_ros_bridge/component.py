"""
ROS-MQTT Bridge — LUCID component that bridges ROS 1 topics to LUCID MQTT.

Enables any ROS 1 device to participate as a LUCID agent by translating:
  - ROS topics → LUCID telemetry streams
  - LUCID commands → ROS topic publishes

Two modes:
  auto_discover: true   — queries the ROS master at start_ros for all
                          published topics and bridges them automatically.
  auto_discover: false  — uses only the explicit ros_subscriptions /
                          ros_publishers lists from the YAML.

Both modes can coexist: auto-discovered topics are merged with any explicit
entries from the YAML (explicit entries take priority for the same topic).

Lifecycle:
  _start()        — minimal: load cfg, subscribe to MQTT cmds. The ROS layer
                    stays idle. Mirrors chrony's pattern.
  cmd/start_ros   — initialise rospy, register subs/pubs, start watchdog.
  cmd/stop_ros    — unregister subs/pubs, stop watchdog. Idempotent.

A self-healing watchdog runs only while ros_active. Each tick it verifies
the master is reachable, recreates dead subscriptions (no callback for
> subscriber_stale_threshold_s while the topic still has publishers), and
recreates any cfg'd publisher missing from _ros_pubs.

Configuration is loaded from ros_bridge.yaml shipped alongside this module.
To override, set "config_path" in the component context config to point at
a custom YAML file, or place ros_bridge.yaml next to the agent's working dir.

See ros_bridge.yaml for the full config schema and examples.

msg_type format: "package/MessageType" (e.g. "geometry_msgs/Twist")
  This follows the standard ROS 1 convention.
"""
from __future__ import annotations

import copy
import importlib
import json
import logging
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from lucid_component_base import Component, ComponentContext

logger = logging.getLogger(__name__)

# rospy.init_node() may only be called once per Python process.
# This flag prevents a second call when start_ros is invoked again.
_ros_node_initialized: bool = False

# Default config: shipped alongside this module
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "ros_bridge.yaml"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_bridge_config_with_source(
    context_config: dict[str, Any],
) -> tuple[dict[str, Any], Path | None]:
    """Load the bridge YAML config and return the selected source path."""
    explicit = context_config.get("config_path")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"ROS bridge config not found: {p}")
        logger.info("Loading ROS bridge config from explicit path: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    local = Path("ros_bridge.yaml")
    if local.is_file():
        p = local.resolve()
        logger.info("Loading ROS bridge config from working dir: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    if _DEFAULT_CONFIG_PATH.is_file():
        p = _DEFAULT_CONFIG_PATH.resolve()
        logger.info("Loading ROS bridge config from package default: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    logger.warning("No ros_bridge.yaml found — starting with empty config")
    return ({}, None)


def _load_bridge_config(context_config: dict[str, Any]) -> dict[str, Any]:
    """Backward-compat wrapper that drops the source path from the tuple."""
    cfg, _ = _load_bridge_config_with_source(context_config)
    return cfg


def _resolve_setup_script_paths(
    setup_scripts: list[Any],
    *,
    config_source_path: Path | None,
) -> list[Path]:
    """Resolve ordered setup script paths against the active config file."""
    if not isinstance(setup_scripts, list):
        raise TypeError("setup_scripts must be a list of script paths")

    base_dir = config_source_path.parent if config_source_path is not None else Path.cwd()
    resolved: list[Path] = []
    for raw in setup_scripts:
        if not isinstance(raw, str) or not raw.strip():
            raise TypeError("setup_scripts entries must be non-empty strings")

        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = base_dir / path
        resolved.append(path.resolve())

    return resolved


def _sync_sys_path_from_pythonpath(pythonpath: str | None) -> None:
    """Prepend PYTHONPATH entries to sys.path once, preserving order."""
    if not pythonpath:
        return

    entries = [entry for entry in pythonpath.split(os.pathsep) if entry]
    for entry in reversed(entries):
        if entry not in sys.path:
            sys.path.insert(0, entry)


def _source_setup_scripts(paths: list[Path]) -> dict[str, str]:
    """Source setup scripts in order and merge the resulting env into this process."""
    if not paths:
        return {}

    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise RuntimeError(f"ROS setup script not found: {missing[0]}")

    source_chain = " ".join(f"source {shlex.quote(str(path))};" for path in paths)
    # ROS setup scripts commonly reference unset variables internally and are not
    # safe under `set -u`, so keep error/pipefail handling without nounset.
    command = f"set -eo pipefail; {source_chain} env -0"

    try:
        completed = subprocess.run(
            ["bash", "-lc", command],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise RuntimeError(f"Failed to source ROS setup scripts: {exc}") from exc

    if completed.returncode != 0:
        stderr = completed.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or f"exit code {completed.returncode}"
        raise RuntimeError(f"Failed to source ROS setup scripts: {detail}")

    env_updates: dict[str, str] = {}
    for item in completed.stdout.split(b"\x00"):
        if not item:
            continue
        key, sep, value = item.partition(b"=")
        if not sep:
            continue
        env_updates[key.decode("utf-8", errors="replace")] = value.decode(
            "utf-8", errors="replace"
        )

    os.environ.update(env_updates)
    _sync_sys_path_from_pythonpath(env_updates.get("PYTHONPATH"))
    return env_updates


def _topic_to_metric(ros_topic: str) -> str:
    """'/camera/rgb/image_raw' → 'camera_rgb_image_raw'"""
    return ros_topic.strip("/").replace("/", "_")


def _topic_to_command(ros_topic: str) -> str:
    """'/cmd_vel' → 'cmd_vel'"""
    return ros_topic.strip("/").replace("/", "_")


def _discover_ros_topics(
    exclude_topics: set[str],
    explicit_sub_topics: set[str],
    explicit_pub_topics: set[str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Query the ROS master and build subscription/publisher entries."""
    import rospy

    try:
        published = rospy.get_published_topics()
    except Exception as exc:
        logger.error("Failed to query ROS master for topics: %s", exc)
        return [], []

    discovered_subs: list[dict[str, str]] = []
    discovered_pubs: list[dict[str, str]] = []

    for ros_topic, msg_type_str in published:
        if ros_topic in exclude_topics:
            logger.info("Auto-discovery: excluding %s", ros_topic)
            continue

        if ros_topic not in explicit_sub_topics:
            discovered_subs.append({
                "ros_topic": ros_topic,
                "msg_type": msg_type_str,
                "telemetry_metric": _topic_to_metric(ros_topic),
            })

        if ros_topic not in explicit_pub_topics:
            discovered_pubs.append({
                "ros_topic": ros_topic,
                "msg_type": msg_type_str,
                "command": _topic_to_command(ros_topic),
            })

    logger.info(
        "Auto-discovered %d subscriptions and %d publishers from ROS master",
        len(discovered_subs), len(discovered_pubs),
    )
    return discovered_subs, discovered_pubs


def _resolve_msg_type(msg_type_str: str) -> type:
    """Resolve a ROS 1 message type string like 'geometry_msgs/Twist' to its class.

    Accepts both 'geometry_msgs/Twist' (ROS convention) and
    'geometry_msgs.msg.Twist' (Python import style).
    """
    if "/" in msg_type_str:
        parts = msg_type_str.split("/")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid msg_type format: {msg_type_str!r}. "
                "Expected 'package/MessageType' (e.g. 'geometry_msgs/Twist')."
            )
        package, class_name = parts
        module_path = f"{package}.msg"
    else:
        parts = msg_type_str.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Invalid msg_type format: {msg_type_str!r}. "
                "Expected 'package/MessageType' or 'package.msg.MessageType'."
            )
        module_path, class_name = parts

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _msg_to_dict(msg: Any) -> dict[str, Any]:
    """Convert a ROS 1 message to a JSON-serialisable dict."""
    try:
        from rospy_message_converter import message_converter
        return message_converter.convert_ros_message_to_dictionary(msg)
    except ImportError:
        pass

    result: dict[str, Any] = {}
    for slot in getattr(msg, "__slots__", []):
        value = getattr(msg, slot, None)
        if hasattr(value, "__slots__") and hasattr(value, "_type"):
            result[slot] = _msg_to_dict(value)
        elif isinstance(value, (list, tuple)):
            result[slot] = [
                _msg_to_dict(v) if hasattr(v, "__slots__") else v
                for v in value
            ]
        else:
            result[slot] = value
    return result


def _dict_to_msg(data: dict[str, Any], msg_type: type) -> Any:
    """Populate a ROS 1 message instance from a dict."""
    try:
        from rospy_message_converter import message_converter
        return message_converter.convert_dictionary_to_ros_message(
            msg_type._type, data,
        )
    except (ImportError, AttributeError):
        pass

    msg = msg_type()
    for key, value in data.items():
        if not hasattr(msg, key):
            continue
        current = getattr(msg, key)
        if isinstance(value, dict) and hasattr(current, "__slots__"):
            setattr(msg, key, _dict_to_msg(value, type(current)))
        else:
            setattr(msg, key, value)
    return msg


class RosBridgeComponent(Component):
    """Bridges ROS 1 topics to/from LUCID MQTT topics.

    Idle by default. cmd/start_ros activates the ROS layer (init node,
    register subs/pubs, start watchdog). cmd/stop_ros tears it back down.
    The watchdog self-heals dead subscribers and missing publishers.

    Retained: metadata, status, state, cfg.
    Telemetry: one metric per configured ROS subscription.
    Commands: ping, start_ros, stop_ros, cfg/set, roslaunch-start/stop,
              rosbag-start/stop, plus one per configured ROS publisher.
    """

    def __init__(self, context: ComponentContext) -> None:
        super().__init__(context)
        self._log = context.logger()

        self._bridge_cfg, self._bridge_cfg_path = _load_bridge_config_with_source(
            dict(context.config)
        )

        self._node_name: str = self._bridge_cfg.get("node_name", "lucid_ros_bridge")
        self._auto_discover: bool = bool(self._bridge_cfg.get("auto_discover", False))
        self._max_auto_discover_publishers: int = int(
            self._bridge_cfg.get("max_auto_discover_publishers", 50)
        )
        self._max_telemetry_payload_bytes: int | None = (
            self._bridge_cfg.get("max_telemetry_payload_bytes") or None
        )
        self._exclude_topics: set[str] = set(self._bridge_cfg.get("exclude_topics") or [])
        self._ros_subscriptions: list[dict[str, str]] = self._bridge_cfg.get("ros_subscriptions") or []
        self._ros_publishers: list[dict[str, str]] = self._bridge_cfg.get("ros_publishers") or []
        # Set of configured publisher command names — built at init time so
        # __getattr__ can route cmd topics before _ros_pubs is populated.
        self._ros_publisher_commands: set[str] = {
            p["command"] for p in self._ros_publishers if p.get("command")
        }
        self._setup_scripts: list[str] = list(self._bridge_cfg.get("setup_scripts") or [])
        self._setup_script_paths: list[Path] = _resolve_setup_script_paths(
            self._setup_scripts,
            config_source_path=self._bridge_cfg_path,
        )

        # Watchdog tunables
        self._watchdog_interval_s: float = float(
            self._bridge_cfg.get("watchdog_interval_s", 5.0)
        )
        self._subscriber_stale_threshold_s: float = float(
            self._bridge_cfg.get("subscriber_stale_threshold_s", 10.0)
        )

        # Display override for roslaunch/rosbag subprocesses (e.g. ":0").
        # When set, child processes can render rviz/cv windows to the local Xorg.
        raw_display = self._bridge_cfg.get("display")
        self._display: str | None = str(raw_display) if raw_display else None

        # Roslaunch configuration
        roslaunch_raw = self._bridge_cfg.get("roslaunch") or {}
        if not isinstance(roslaunch_raw, dict):
            roslaunch_raw = {}
        self._roslaunch_package: str = str(roslaunch_raw.get("package", ""))
        self._roslaunch_launch_file: str = str(roslaunch_raw.get("launch_file", ""))
        self._roslaunch_args: dict[str, str] = {
            str(k): str(v) for k, v in (roslaunch_raw.get("args") or {}).items()
        }

        # Per-instance ROS env overrides for roslaunch/rosbag subprocesses.
        # Allows multiple ros_bridge instances to target different ROS masters.
        self._ros_env: dict[str, str] = {
            str(k): str(v) for k, v in (self._bridge_cfg.get("ros_env") or {}).items()
        }

        # ROS runtime state — populated only while _ros_active.
        # _ros_subs / _ros_pubs are keyed by ros_topic / command for watchdog lookup.
        self._ros_subs: dict[str, Any] = {}
        self._ros_pubs: dict[str, Any] = {}
        self._pub_msg_types: dict[str, type] = {}
        self._sub_metric_for_topic: dict[str, str] = {}
        self._sub_msg_type_str: dict[str, str] = {}
        self._sub_last_msg_at: dict[str, float] = {}
        # Guards _ros_subs / _ros_pubs / _pub_msg_types and the related maps.
        # Held briefly — never across blocking I/O like unregister().
        self._ros_state_lock = threading.Lock()

        self._ros_active: bool = False
        self._watchdog_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Latest values received from ROS (for state payload)
        self._latest_values: dict[str, dict[str, Any]] = {}
        self._values_lock = threading.Lock()

        # Subprocess management for roslaunch and rosbag
        self._roslaunch_proc: subprocess.Popen | None = None
        self._roslaunch_state: str = "idle"
        self._rosbag_proc: subprocess.Popen | None = None
        self._rosbag_state: str = "idle"
        self._rosbag_path: str | None = None
        self._rosbag_start_time: float | None = None

    @property
    def component_id(self) -> str:
        return self.context.component_id

    def capabilities(self) -> list[str]:
        caps = [
            "ping", "reset", "start_ros", "stop_ros",
            "roslaunch-start", "roslaunch-stop",
            "rosbag-start", "rosbag-stop",
        ]
        for pub_cfg in self._ros_publishers:
            command = pub_cfg.get("command", "")
            if command:
                caps.append(command)
        return caps

    def metadata(self) -> dict[str, Any]:
        out = super().metadata()
        out["capabilities"] = self.capabilities()
        out["node_name"] = self._node_name
        out["ros_subscriptions"] = [
            {"ros_topic": s["ros_topic"], "telemetry_metric": s["telemetry_metric"]}
            for s in self._ros_subscriptions
        ]
        out["ros_publishers"] = [
            {"ros_topic": p["ros_topic"], "command": p["command"]}
            for p in self._ros_publishers
        ]
        if self._roslaunch_package:
            out["roslaunch"] = {
                "package": self._roslaunch_package,
                "launch_file": self._roslaunch_launch_file,
            }
        return out

    def get_state_payload(self) -> dict[str, Any]:
        with self._ros_state_lock:
            subs_active = len(self._ros_subs)
            pubs_active = len(self._ros_pubs)
        with self._values_lock:
            return {
                "node_name": self._node_name,
                "ros_active": self._ros_active,
                "subscriptions_active": subs_active,
                "publishers_active": pubs_active,
                "roslaunch_state": self._roslaunch_state,
                "roslaunch_package": self._roslaunch_package,
                "roslaunch_launch_file": self._roslaunch_launch_file,
                "rosbag_state": self._rosbag_state,
                "latest_values": dict(self._latest_values),
            }

    def get_cfg_payload(self) -> dict[str, Any]:
        return {
            "node_name": self._node_name,
            "auto_discover": self._auto_discover,
            "max_auto_discover_publishers": self._max_auto_discover_publishers,
            "max_telemetry_payload_bytes": self._max_telemetry_payload_bytes,
            "exclude_topics": sorted(self._exclude_topics),
            "ros_subscriptions": self._ros_subscriptions,
            "ros_publishers": self._ros_publishers,
            "setup_scripts": self._setup_scripts,
            "ros_env": dict(self._ros_env),
            "watchdog_interval_s": self._watchdog_interval_s,
            "subscriber_stale_threshold_s": self._subscriber_stale_threshold_s,
            "display": self._display,
            "roslaunch": {
                "package": self._roslaunch_package,
                "launch_file": self._roslaunch_launch_file,
                "args": dict(self._roslaunch_args),
            },
        }

    def schema(self) -> dict[str, Any]:
        s = copy.deepcopy(super().schema())
        s["publishes"]["state"]["fields"].update({
            "node_name": {"type": "string"},
            "ros_active": {"type": "boolean"},
            "subscriptions_active": {"type": "integer"},
            "publishers_active": {"type": "integer"},
            "roslaunch_state": {
                "type": "string",
                "enum": ["idle", "launching", "running", "exited", "stopping"],
            },
            "rosbag_state": {
                "type": "string",
                "enum": ["idle", "recording", "stopping"],
            },
            "latest_values": {
                "type": "object",
                "description": "Latest ROS message values per subscription",
            },
            "roslaunch_package": {"type": "string"},
            "roslaunch_launch_file": {"type": "string"},
        })
        s["publishes"]["cfg"]["fields"].update({
            "node_name": {"type": "string"},
            "auto_discover": {"type": "boolean"},
            "max_auto_discover_publishers": {"type": "integer"},
            "max_telemetry_payload_bytes": {"type": "integer"},
            "exclude_topics": {"type": "array", "items": {"type": "string"}},
            "watchdog_interval_s": {"type": "number"},
            "subscriber_stale_threshold_s": {"type": "number"},
            "display": {"type": ["string", "null"]},
            "roslaunch": {
                "type": "object",
                "fields": {
                    "package": {"type": "string"},
                    "launch_file": {"type": "string"},
                    "args": {"type": "object"},
                },
            },
        })
        s["subscribes"].update({
            "cmd/start_ros": {"fields": {}},
            "cmd/stop_ros": {"fields": {}},
            "cmd/roslaunch-start": {
                "fields": {
                    "package": {"type": "string", "description": "Defaults to config roslaunch.package"},
                    "launch_file": {"type": "string", "description": "Defaults to config roslaunch.launch_file"},
                    "args": {"type": ["object", "string"], "description": "Dict of key:=value pairs or legacy string"},
                },
            },
            "cmd/roslaunch-stop": {"fields": {}},
            "cmd/rosbag-start": {
                "fields": {
                    "output_dir": {"type": "string", "default": "data"},
                    "topics": {"type": "array", "items": {"type": "string"}},
                    "prefix": {"type": "string", "default": "lucid"},
                },
            },
            "cmd/rosbag-stop": {"fields": {}},
        })
        return s

    # -- lifecycle --------------------------------------------------------

    def _start(self) -> None:
        """Idle bring-up: source env, publish retained state. No ROS interaction.

        Mirrors chrony's pattern — the component is observable and addressable
        on MQTT, but ROS subscribers/publishers are not registered until
        cmd/start_ros is received.
        """
        if self._setup_script_paths:
            _source_setup_scripts(self._setup_script_paths)

        # Verify rospy is importable so we fail fast if the env is wrong.
        try:
            import rospy  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "rospy is not available. Install ROS 1 and source both "
                "/opt/ros/noetic/setup.bash and your workspace setup.bash "
                "before running the ROS bridge component."
            ) from exc

        # Telemetry config covers configured subscriptions; auto-discovered
        # metrics are added when start_ros runs auto-discovery.
        self.set_telemetry_config(self._build_telemetry_cfg(self._ros_subscriptions))

        self._publish_all_retained()

    def _stop(self) -> None:
        if self._ros_active:
            self._deactivate_ros()

        self._kill_subprocess(self._roslaunch_proc, "roslaunch", sigint_timeout=15)
        self._roslaunch_proc = None
        self._roslaunch_state = "idle"
        self._kill_subprocess(self._rosbag_proc, "rosbag", sigint_timeout=10)
        self._rosbag_proc = None
        self._rosbag_state = "idle"

        # Final agent shutdown: release process-global rospy resources so
        # the process can exit cleanly instead of hanging in atexit.
        try:
            import rospy
            if rospy.core.is_initialized():
                self._log.info("Final shutdown: calling rospy.signal_shutdown()")
                rospy.signal_shutdown("agent shutdown")
        except Exception:
            self._log.exception("Error during rospy.signal_shutdown()")

        self._log.info("ROS bridge stopped")

    # -- ROS activation ---------------------------------------------------

    def _activate_ros(self) -> None:
        """Initialise rospy, register all subs/pubs, start the watchdog.

        Caller holds responsibility for guarding against concurrent activation
        (start_ros / stop_ros are dispatched on the MQTT thread, serialised by
        the component base's executor — but we still gate on _ros_active).
        """
        global _ros_node_initialized
        import rospy

        if not _ros_node_initialized or not rospy.core.is_initialized():
            rospy.init_node(self._node_name, anonymous=True, disable_signals=True)
            _ros_node_initialized = True
            self._log.info("ROS node '%s' initialised", self._node_name)

        if self._auto_discover:
            explicit_sub_topics = {s["ros_topic"] for s in self._ros_subscriptions}
            explicit_pub_topics = {p["ros_topic"] for p in self._ros_publishers}
            discovered_subs, discovered_pubs = _discover_ros_topics(
                self._exclude_topics, explicit_sub_topics, explicit_pub_topics,
            )
            if len(discovered_pubs) > self._max_auto_discover_publishers:
                raise RuntimeError(
                    f"auto_discover found {len(discovered_pubs)} publishers, exceeds "
                    f"max_auto_discover_publishers={self._max_auto_discover_publishers}. "
                    "Raise the limit in ros_bridge.yaml or add topics to exclude_topics."
                )
            # Merge discovered into the cfg lists (explicit entries win on dupes,
            # which discovery already filtered above).
            for sub in discovered_subs:
                if not any(s["ros_topic"] == sub["ros_topic"] for s in self._ros_subscriptions):
                    self._ros_subscriptions.append(sub)
            for pub in discovered_pubs:
                if not any(p["ros_topic"] == pub["ros_topic"] for p in self._ros_publishers):
                    self._ros_publishers.append(pub)
                    if pub.get("command"):
                        self._ros_publisher_commands.add(pub["command"])

        self.set_telemetry_config(self._build_telemetry_cfg(self._ros_subscriptions))

        for sub_cfg in list(self._ros_subscriptions):
            self._create_subscription(sub_cfg)
        for pub_cfg in list(self._ros_publishers):
            self._create_publisher(pub_cfg)

        self._stop_event.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="ros_bridge_watchdog",
        )
        self._watchdog_thread.start()

        self._ros_active = True
        self._log.info(
            "ROS bridge active: %d subs, %d pubs",
            len(self._ros_subs), len(self._ros_pubs),
        )

    def _deactivate_ros(self) -> None:
        """Stop the watchdog and unregister all subs/pubs."""
        self._stop_event.set()
        if self._watchdog_thread is not None:
            self._watchdog_thread.join(timeout=self._watchdog_interval_s + 2)
            self._watchdog_thread = None

        with self._ros_state_lock:
            subs_to_close = list(self._ros_subs.values())
            pubs_to_close = list(self._ros_pubs.values())
            self._ros_subs.clear()
            self._ros_pubs.clear()
            self._pub_msg_types.clear()
            self._sub_metric_for_topic.clear()
            self._sub_msg_type_str.clear()
            self._sub_last_msg_at.clear()

        for sub in subs_to_close:
            try:
                sub.unregister()
            except Exception:
                self._log.debug("Subscriber unregister failed (ignored)", exc_info=True)
        for pub in pubs_to_close:
            try:
                pub.unregister()
            except Exception:
                self._log.debug("Publisher unregister failed (ignored)", exc_info=True)

        self._ros_active = False
        self._log.info("ROS bridge deactivated")

    def _build_telemetry_cfg(self, subs: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
        return {
            sub["telemetry_metric"]: {
                "enabled": False,
                "interval_s": 0.1,
                "change_threshold_percent": 0.0,
            }
            for sub in subs
        }

    def _create_subscription(self, sub_cfg: dict[str, str]) -> bool:
        import rospy

        ros_topic = sub_cfg["ros_topic"]
        msg_type_str = sub_cfg["msg_type"]
        metric = sub_cfg["telemetry_metric"]

        try:
            msg_type = _resolve_msg_type(msg_type_str)
        except (ImportError, AttributeError, ValueError) as exc:
            self._log.error(
                "Failed to resolve msg type %s for topic %s: %s",
                msg_type_str, ros_topic, exc,
            )
            return False

        callback = self._make_subscription_callback(ros_topic, metric)
        try:
            sub = rospy.Subscriber(ros_topic, msg_type, callback)
        except Exception:
            self._log.exception("Failed to subscribe to %s", ros_topic)
            return False

        with self._ros_state_lock:
            self._ros_subs[ros_topic] = sub
            self._sub_metric_for_topic[ros_topic] = metric
            self._sub_msg_type_str[ros_topic] = msg_type_str
            self._sub_last_msg_at[ros_topic] = time.monotonic()
        self._log.info("Subscribed ROS topic %s → telemetry/%s", ros_topic, metric)
        return True

    def _create_publisher(self, pub_cfg: dict[str, str]) -> bool:
        import rospy

        ros_topic = pub_cfg["ros_topic"]
        msg_type_str = pub_cfg["msg_type"]
        command = pub_cfg["command"]

        try:
            msg_type = _resolve_msg_type(msg_type_str)
        except (ImportError, AttributeError, ValueError) as exc:
            self._log.error(
                "Failed to resolve msg type %s for command %s: %s",
                msg_type_str, command, exc,
            )
            return False

        try:
            pub = rospy.Publisher(ros_topic, msg_type, queue_size=10)
        except Exception:
            self._log.exception("Failed to register publisher for %s", ros_topic)
            return False

        with self._ros_state_lock:
            self._ros_pubs[command] = pub
            self._pub_msg_types[command] = msg_type
        self._log.info("ROS publisher %s ← cmd/%s", ros_topic, command)
        return True

    def _make_subscription_callback(self, ros_topic: str, metric: str):
        def _callback(msg: Any) -> None:
            self._sub_last_msg_at[ros_topic] = time.monotonic()
            data = _msg_to_dict(msg)
            with self._values_lock:
                self._latest_values[metric] = data
            if self.should_publish_telemetry(metric, data):
                if self._max_telemetry_payload_bytes is not None:
                    encoded = json.dumps(data).encode("utf-8")
                    if len(encoded) > self._max_telemetry_payload_bytes:
                        self._log.warning(
                            "Telemetry payload for metric '%s' is %d bytes, "
                            "exceeds max_telemetry_payload_bytes=%d; skipping publish",
                            metric, len(encoded), self._max_telemetry_payload_bytes,
                        )
                        return
                self.publish_telemetry(metric, data)
        return _callback

    # -- watchdog ---------------------------------------------------------

    def _is_ros_master_reachable(self) -> bool:
        """Quick socket check — does not block for long."""
        import socket
        from urllib.parse import urlparse
        master_uri = os.environ.get("ROS_MASTER_URI", "http://localhost:11311")
        try:
            parsed = urlparse(master_uri)
            host = parsed.hostname or "localhost"
            port = parsed.port or 11311
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            return True
        except (OSError, socket.timeout):
            return False

    def _watchdog_loop(self) -> None:
        """Self-heal subscriptions and publishers while ROS is active.

        Each tick:
          1. If master is unreachable, skip — wait for it to come back.
          2. For each subscriber: if no message in stale_threshold AND the
             topic still has publishers, recreate the subscription.
          3. For each cfg'd publisher: if missing from _ros_pubs, recreate.
        """
        while not self._stop_event.wait(timeout=self._watchdog_interval_s):
            try:
                self._watchdog_tick()
            except Exception:
                self._log.exception("Watchdog tick failed — continuing")

    def _watchdog_tick(self) -> None:
        if not self._is_ros_master_reachable():
            return

        import rospy
        try:
            published_topics = {t for t, _ in rospy.get_published_topics()}
        except Exception:
            self._log.debug("Watchdog: get_published_topics failed", exc_info=True)
            return

        now = time.monotonic()
        threshold = self._subscriber_stale_threshold_s
        recreated_any = False

        with self._ros_state_lock:
            sub_topics = list(self._ros_subs.keys())
            sub_msg_types = dict(self._sub_msg_type_str)
            sub_metrics = dict(self._sub_metric_for_topic)
            sub_last = dict(self._sub_last_msg_at)
            cfg_pub_commands = {
                p["command"]: dict(p) for p in self._ros_publishers if p.get("command")
            }
            current_pubs = set(self._ros_pubs.keys())

        for ros_topic in sub_topics:
            last_seen = sub_last.get(ros_topic, 0.0)
            if now - last_seen <= threshold:
                continue
            if ros_topic not in published_topics:
                continue
            self._log.warning(
                "Watchdog: subscription %s has no messages for %.1fs but topic has "
                "publishers — recreating",
                ros_topic, now - last_seen,
            )
            with self._ros_state_lock:
                sub = self._ros_subs.pop(ros_topic, None)
                self._sub_last_msg_at.pop(ros_topic, None)
            if sub is not None:
                try:
                    sub.unregister()
                except Exception:
                    self._log.debug("Stale sub unregister failed (ignored)", exc_info=True)
            self._create_subscription({
                "ros_topic": ros_topic,
                "msg_type": sub_msg_types.get(ros_topic, ""),
                "telemetry_metric": sub_metrics.get(ros_topic, _topic_to_metric(ros_topic)),
            })
            recreated_any = True

        for command, pub_cfg in cfg_pub_commands.items():
            if command in current_pubs:
                continue
            self._log.warning(
                "Watchdog: publisher for cmd/%s missing — recreating", command,
            )
            self._create_publisher(pub_cfg)
            recreated_any = True

        if recreated_any:
            self.publish_state()

    # -- retained publishing ----------------------------------------------

    def _publish_all_retained(self) -> None:
        self.publish_metadata()
        self.publish_schema()
        self.publish_status()
        self.publish_state()
        self.publish_cfg()

    # -- helpers -----------------------------------------------------------

    def _require_ros_active(self, action: str, request_id: str) -> bool:
        if self._ros_active:
            return True
        self.publish_result(
            action, request_id, ok=False,
            error="ros_bridge idle — call cmd/start_ros first",
        )
        return False

    def _subprocess_env(self) -> dict[str, str] | None:
        """Build env for roslaunch/rosbag subprocesses, applying ros_env + display.

        Returns None when neither override is set, so subprocess.Popen inherits
        the agent process env unchanged.
        """
        if not self._ros_env and not self._display:
            return None
        env = {**os.environ, **self._ros_env}
        if self._display:
            env["DISPLAY"] = self._display
        return env

    # -- command handlers --------------------------------------------------

    def on_cmd_ping(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""
        self.publish_result("ping", request_id, ok=True, error=None)

    def on_cmd_reset(self, payload_str: str) -> None:
        """Clear cached latest_values. Does not touch the ROS layer."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        with self._values_lock:
            self._latest_values.clear()

        self.publish_state()
        self.publish_result("reset", request_id, ok=True, error=None)

    def on_cmd_start_ros(self, payload_str: str) -> None:
        """Activate the ROS layer. Idempotent."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if self._ros_active:
            self.publish_result("start_ros", request_id, ok=True, error=None)
            return

        try:
            self._activate_ros()
        except Exception as exc:
            self._log.exception("start_ros failed")
            try:
                self._deactivate_ros()
            except Exception:
                self._log.debug("cleanup after failed start_ros", exc_info=True)
            self.publish_result("start_ros", request_id, ok=False, error=str(exc))
            return

        self.publish_state()
        self.publish_result("start_ros", request_id, ok=True, error=None)

    def on_cmd_stop_ros(self, payload_str: str) -> None:
        """Deactivate the ROS layer. Idempotent."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if not self._ros_active:
            self.publish_result("stop_ros", request_id, ok=True, error=None)
            return

        try:
            self._deactivate_ros()
        except Exception as exc:
            self._log.exception("stop_ros failed")
            self.publish_result("stop_ros", request_id, ok=False, error=str(exc))
            return

        self.publish_state()
        self.publish_result("stop_ros", request_id, ok=True, error=None)

    def on_cmd_cfg_set(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            set_dict = payload.get("set") or {}
        except json.JSONDecodeError:
            request_id = ""
            set_dict = {}

        if not isinstance(set_dict, dict):
            self.publish_cfg_set_result(
                request_id=request_id,
                ok=False,
                applied=None,
                error="payload 'set' must be an object",
                ts=_utc_iso(),
            )
            return

        applied: dict[str, Any] = {}
        rejected: dict[str, str] = {}

        if "node_name" in set_dict:
            self._node_name = str(set_dict["node_name"])
            applied["node_name"] = self._node_name

        if "auto_discover" in set_dict:
            self._auto_discover = bool(set_dict["auto_discover"])
            applied["auto_discover"] = self._auto_discover

        if "max_auto_discover_publishers" in set_dict:
            self._max_auto_discover_publishers = int(set_dict["max_auto_discover_publishers"])
            applied["max_auto_discover_publishers"] = self._max_auto_discover_publishers

        if "max_telemetry_payload_bytes" in set_dict:
            val = set_dict["max_telemetry_payload_bytes"]
            self._max_telemetry_payload_bytes = int(val) if val is not None else None
            applied["max_telemetry_payload_bytes"] = self._max_telemetry_payload_bytes

        if "exclude_topics" in set_dict:
            raw = set_dict["exclude_topics"]
            if isinstance(raw, list):
                self._exclude_topics = {str(t) for t in raw}
                applied["exclude_topics"] = sorted(self._exclude_topics)
            else:
                rejected["exclude_topics"] = "must be a list of topic strings"

        if "watchdog_interval_s" in set_dict:
            try:
                val = float(set_dict["watchdog_interval_s"])
            except (TypeError, ValueError):
                rejected["watchdog_interval_s"] = "must be a number"
            else:
                if val <= 0:
                    rejected["watchdog_interval_s"] = "must be > 0"
                else:
                    self._watchdog_interval_s = val
                    applied["watchdog_interval_s"] = self._watchdog_interval_s

        if "subscriber_stale_threshold_s" in set_dict:
            try:
                val = float(set_dict["subscriber_stale_threshold_s"])
            except (TypeError, ValueError):
                rejected["subscriber_stale_threshold_s"] = "must be a number"
            else:
                if val <= 0:
                    rejected["subscriber_stale_threshold_s"] = "must be > 0"
                else:
                    self._subscriber_stale_threshold_s = val
                    applied["subscriber_stale_threshold_s"] = self._subscriber_stale_threshold_s

        if "display" in set_dict:
            raw = set_dict["display"]
            if raw is None or raw == "":
                self._display = None
                applied["display"] = None
            elif isinstance(raw, str):
                self._display = raw
                applied["display"] = self._display
            else:
                rejected["display"] = "must be a string or null"

        if "roslaunch" in set_dict:
            rl = set_dict["roslaunch"]
            if isinstance(rl, dict):
                if "args" in rl:
                    if isinstance(rl["args"], dict):
                        self._roslaunch_args = {str(k): str(v) for k, v in rl["args"].items()}
                        applied.setdefault("roslaunch", {})["args"] = dict(self._roslaunch_args)
                    else:
                        rejected["roslaunch.args"] = "must be a dict of key→value pairs"
                for rl_key in ("package", "launch_file"):
                    if rl_key in rl:
                        rejected[f"roslaunch.{rl_key}"] = "cannot be changed at runtime; restart required"
            else:
                rejected["roslaunch"] = "must be an object"

        for key in ("ros_subscriptions", "ros_publishers"):
            if key in set_dict:
                rejected[key] = "cannot be changed at runtime; restart required"

        known_keys = {
            "node_name", "auto_discover", "max_auto_discover_publishers",
            "max_telemetry_payload_bytes", "exclude_topics",
            "watchdog_interval_s", "subscriber_stale_threshold_s", "display",
            "ros_subscriptions", "ros_publishers", "roslaunch",
        }
        for key in set_dict:
            if key not in known_keys:
                rejected[key] = "unknown config key"

        self.publish_state()
        self.publish_cfg()
        self.publish_cfg_set_result(
            request_id=request_id,
            ok=True,
            applied=applied if applied else None,
            error=json.dumps({"rejected": rejected}) if rejected else None,
            ts=_utc_iso(),
        )

    # -- subprocess helpers ------------------------------------------------

    def _kill_subprocess(
        self,
        proc: subprocess.Popen | None,
        label: str,
        sigint_timeout: int = 10,
    ) -> int | None:
        """Send SIGINT to a process group, fall back to SIGKILL. Returns exit code."""
        if proc is None or proc.poll() is not None:
            return proc.returncode if proc else None
        self._log.info("Stopping %s subprocess (pid %d)", label, proc.pid)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            proc.wait(timeout=sigint_timeout)
        except subprocess.TimeoutExpired:
            self._log.warning("%s did not exit in %ds, sending SIGKILL", label, sigint_timeout)
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
        except ProcessLookupError:
            pass
        return proc.returncode

    # -- roslaunch command handlers ----------------------------------------

    def _do_roslaunch_start(
        self,
        package: str,
        launch_file: str,
        args: dict[str, str],
        *,
        request_id: str = "",
        publish_result: bool = True,
    ) -> bool:
        if not package or not launch_file:
            if publish_result:
                self.publish_result(
                    "roslaunch_start", request_id, ok=False,
                    error="package and launch_file required",
                )
            return False

        if self._roslaunch_proc is not None and self._roslaunch_proc.poll() is None:
            self._log.info(
                "roslaunch already running (%s), stopping to start %s",
                self._roslaunch_launch_file, launch_file,
            )
            proc = self._roslaunch_proc
            self._roslaunch_proc = None
            self._roslaunch_state = "stopping"
            self.publish_state()
            self._kill_subprocess(proc, "roslaunch", sigint_timeout=15)

        cmd_parts = ["roslaunch", package, launch_file]
        for key, value in args.items():
            cmd_parts.append(f"{key}:={value}")

        self._log.info("Starting roslaunch: %s", " ".join(cmd_parts))
        self._roslaunch_state = "launching"

        try:
            self._roslaunch_proc = subprocess.Popen(
                cmd_parts,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
                env=self._subprocess_env(),
            )
        except FileNotFoundError as exc:
            self._roslaunch_state = "idle"
            if publish_result:
                self.publish_result("roslaunch_start", request_id, ok=False, error=str(exc))
            return False

        self._roslaunch_state = "running"
        self.publish_state()
        if publish_result:
            self.publish_result("roslaunch_start", request_id, ok=True, error=None)

        def _monitor() -> None:
            proc = self._roslaunch_proc
            if proc is not None:
                proc.wait()
                rc = proc.returncode
                if self._roslaunch_proc is proc:
                    self._roslaunch_proc = None
                    self._roslaunch_state = "exited"
                    self._log.info("roslaunch exited with code %s", rc)
                    self.publish_state()

        threading.Thread(target=_monitor, daemon=True).start()
        return True

    def on_cmd_roslaunch_start(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            data = payload.get("data", payload)
        except json.JSONDecodeError:
            request_id, data = "", {}

        if not self._require_ros_active("roslaunch_start", request_id):
            return

        package = data.get("package", "") or self._roslaunch_package
        launch_file = data.get("launch_file", "") or self._roslaunch_launch_file

        args = dict(self._roslaunch_args)
        payload_args = data.get("args", "")
        if isinstance(payload_args, dict):
            args.update({str(k): str(v) for k, v in payload_args.items()})
        elif isinstance(payload_args, str) and payload_args.strip():
            for token in shlex.split(payload_args):
                if ":=" in token:
                    k, _, v = token.partition(":=")
                    args[k] = v

        threading.Thread(
            target=self._do_roslaunch_start,
            args=(package, launch_file, args),
            kwargs={"request_id": request_id},
            daemon=True,
        ).start()

    def on_cmd_roslaunch_stop(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if self._roslaunch_proc is None or self._roslaunch_proc.poll() is not None:
            self.publish_result("roslaunch_stop", request_id, ok=False, error="Not running")
            return

        self._roslaunch_state = "stopping"
        proc = self._roslaunch_proc
        self._roslaunch_proc = None
        self.publish_state()

        def _do_stop() -> None:
            self._kill_subprocess(proc, "roslaunch", sigint_timeout=15)
            self._roslaunch_state = "idle"
            self.publish_state()
            self.publish_result("roslaunch_stop", request_id, ok=True, error=None)

        threading.Thread(target=_do_stop, daemon=True).start()

    # -- rosbag command handlers -------------------------------------------

    def on_cmd_rosbag_start(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            data = payload.get("data", payload)
        except json.JSONDecodeError:
            request_id, data = "", {}

        if not self._require_ros_active("rosbag_start", request_id):
            return

        if self._rosbag_proc is not None and self._rosbag_proc.poll() is None:
            self.publish_result("rosbag_start", request_id, ok=False, error="Already recording")
            return

        raw_out = data.get("output_dir", "data")
        if isinstance(raw_out, str) and not raw_out.strip():
            self._log.warning("output_dir was blank; defaulting to 'data'")
            raw_out = "data"
        elif not isinstance(raw_out, str):
            self.publish_result(
                "rosbag_start", request_id, ok=False,
                error="output_dir must be a string",
            )
            return
        else:
            raw_out = raw_out.strip()

        out_dir = Path(raw_out).expanduser()
        if not out_dir.is_absolute():
            out_dir = Path.cwd() / out_dir
        out_dir = out_dir.resolve()

        if not out_dir.is_dir():
            self._log.error(
                "rosbag_start: output_dir '%s' resolved to '%s' which does not exist",
                raw_out, out_dir,
            )
            self.publish_result(
                "rosbag_start", request_id, ok=False,
                error=f"output_dir must be an existing directory: {out_dir}",
            )
            return

        topics = data.get("topics", "__all__")
        prefix = data.get("prefix", "lucid")

        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        bag_name = f"{prefix}_{timestamp}"
        self._rosbag_path = os.path.join(str(out_dir), bag_name)
        self._rosbag_start_time = time.time()

        cmd = ["rosbag", "record", "-O", self._rosbag_path]
        if topics == "__all__":
            cmd.append("-a")
        elif isinstance(topics, list):
            cmd.extend(topics)
        elif isinstance(topics, str):
            cmd.extend(topics.split())

        self._log.info("Starting rosbag: %s", " ".join(cmd))

        try:
            self._rosbag_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid,
                env=self._subprocess_env(),
            )
        except FileNotFoundError as exc:
            self._rosbag_state = "idle"
            self.publish_result("rosbag_start", request_id, ok=False, error=str(exc))
            return

        self._rosbag_state = "recording"
        self.publish_state()
        self.publish_result("rosbag_start", request_id, ok=True, error=None)

    def on_cmd_rosbag_stop(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if self._rosbag_proc is None or self._rosbag_proc.poll() is not None:
            self.publish_result("rosbag_stop", request_id, ok=False, error="Not recording")
            return

        self._kill_subprocess(self._rosbag_proc, "rosbag", sigint_timeout=10)

        duration_s = round(time.time() - self._rosbag_start_time, 1) if self._rosbag_start_time else 0
        bag_file = f"{self._rosbag_path}.bag"
        size_mb = round(os.path.getsize(bag_file) / (1024 * 1024), 1) if os.path.exists(bag_file) else 0

        self._rosbag_proc = None
        self._rosbag_state = "idle"
        self.publish_state()
        self.publish_result("rosbag_stop", request_id, ok=True, error=None)
        self._log.info("rosbag stopped: %s (%.1fs, %.1fMB)", bag_file, duration_s, size_mb)

    # -- ROS topic publish handler -----------------------------------------

    def handle_ros_publish(self, command: str, payload_str: str) -> None:
        """Handle a LUCID command that maps to a ROS topic publish.

        Expected payload: {"request_id": "...", "data": {<ros message fields>}}
        """
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            data = payload.get("data", {})
        except json.JSONDecodeError:
            request_id = ""
            data = {}

        if not self._require_ros_active(command, request_id):
            return

        with self._ros_state_lock:
            pub = self._ros_pubs.get(command)
            msg_type = self._pub_msg_types.get(command)

        if pub is None or msg_type is None:
            self.publish_result(
                command, request_id, ok=False,
                error=f"No ROS publisher configured for command '{command}'",
            )
            return

        try:
            msg = _dict_to_msg(data, msg_type)
            pub.publish(msg)
            self._log.info("Published to ROS via cmd/%s", command)
            self.publish_result(command, request_id, ok=True, error=None)
        except Exception as exc:
            if "closed topic" in str(exc).lower():
                self._log.debug("cmd/%s skipped: %s", command, exc)
            else:
                self._log.error("Failed to publish ROS message for cmd/%s: %s", command, exc)
            self.publish_result(command, request_id, ok=False, error=str(exc))

    def __getattr__(self, name: str) -> Any:
        """Route on_cmd_<command> calls to handle_ros_publish for configured publishers.

        Checks against _ros_publisher_commands (built from config at __init__ time) so
        the agent can subscribe to these cmd topics at startup — even before start_ros
        has run and populated _ros_pubs.
        """
        if name.startswith("on_cmd_"):
            command = name[7:]
            if command in self._ros_publisher_commands:
                def _handler(payload_str: str) -> None:
                    self.handle_ros_publish(command, payload_str)
                return _handler
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")
