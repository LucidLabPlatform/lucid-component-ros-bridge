"""
ROS-MQTT Bridge — LUCID component that bridges ROS 1 topics to LUCID MQTT.

Enables any ROS 1 device to participate as a LUCID agent by translating:
  - ROS topics → LUCID telemetry streams
  - LUCID commands → ROS topic publishes

Two modes:
  auto_discover: true   — queries the ROS master for all published topics at
                          startup and bridges them automatically.
  auto_discover: false  — uses only the explicit ros_subscriptions /
                          ros_publishers lists from the YAML.

Both modes can coexist: auto-discovered topics are merged with any explicit
entries from the YAML (explicit entries take priority for the same topic).

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
# This flag prevents a second call when the component is restarted.
_ros_node_initialized: bool = False

# Default config: shipped alongside this module
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "ros_bridge.yaml"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_bridge_config_with_source(
    context_config: dict[str, Any],
) -> tuple[dict[str, Any], Path | None]:
    """Load the bridge YAML config and return the selected source path."""
    # 1. Explicit override
    explicit = context_config.get("config_path")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"ROS bridge config not found: {p}")
        logger.info("Loading ROS bridge config from explicit path: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    # 2. Working-directory override
    local = Path("ros_bridge.yaml")
    if local.is_file():
        p = local.resolve()
        logger.info("Loading ROS bridge config from working dir: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    # 3. Default shipped with package
    if _DEFAULT_CONFIG_PATH.is_file():
        p = _DEFAULT_CONFIG_PATH.resolve()
        logger.info("Loading ROS bridge config from package default: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}, p)

    logger.warning("No ros_bridge.yaml found — starting with empty config")
    return ({}, None)


def _load_bridge_config(context_config: dict[str, Any]) -> dict[str, Any]:
    """Load the bridge YAML config.

    Resolution order:
      1. context_config["config_path"] — explicit override from agent config
      2. ./ros_bridge.yaml             — next to the agent's working directory
      3. <package>/ros_bridge.yaml     — default shipped with the component
    """
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
    """Convert a ROS topic name to a LUCID telemetry metric name.

    '/camera/rgb/image_raw' → 'camera_rgb_image_raw'
    """
    return ros_topic.strip("/").replace("/", "_")


def _topic_to_command(ros_topic: str) -> str:
    """Convert a ROS topic name to a LUCID command name.

    '/cmd_vel' → 'cmd_vel'
    """
    return ros_topic.strip("/").replace("/", "_")


def _discover_ros_topics(
    exclude_topics: set[str],
    explicit_sub_topics: set[str],
    explicit_pub_topics: set[str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Query the ROS master for all published topics and build subscription/publisher entries.

    Returns (discovered_subscriptions, discovered_publishers).
    Topics in exclude_topics or already in explicit lists are skipped.
    """
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

        # Auto-subscribe (ROS → LUCID telemetry)
        if ros_topic not in explicit_sub_topics:
            discovered_subs.append({
                "ros_topic": ros_topic,
                "msg_type": msg_type_str,
                "telemetry_metric": _topic_to_metric(ros_topic),
            })

        # Auto-publish (LUCID cmd → ROS) for the same topics
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
        # ROS 1 convention: "package/MessageType" → package.msg module
        parts = msg_type_str.split("/")
        if len(parts) != 2:
            raise ValueError(
                f"Invalid msg_type format: {msg_type_str!r}. "
                "Expected 'package/MessageType' (e.g. 'geometry_msgs/Twist')."
            )
        package, class_name = parts
        module_path = f"{package}.msg"
    else:
        # Python import style: "geometry_msgs.msg.Twist"
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
    """Convert a ROS 1 message to a JSON-serialisable dict.

    Uses rospy's message_converter if available, otherwise falls back
    to iterating __slots__ (standard on all genpy messages).
    """
    try:
        from rospy_message_converter import message_converter
        return message_converter.convert_ros_message_to_dictionary(msg)
    except ImportError:
        pass

    # Fallback: iterate __slots__ (genpy messages expose fields this way)
    result: dict[str, Any] = {}
    for slot in getattr(msg, "__slots__", []):
        value = getattr(msg, slot, None)
        if hasattr(value, "__slots__") and hasattr(value, "_type"):
            # Nested ROS message
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

    # Fallback: manual field assignment
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

    Retained: metadata, status, state, cfg.
    Telemetry: one metric per configured ROS subscription.
    Commands: reset, ping, cfg/set, plus one per configured ROS publisher.
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
        self._setup_scripts: list[str] = list(self._bridge_cfg.get("setup_scripts") or [])
        self._setup_script_paths: list[Path] = _resolve_setup_script_paths(
            self._setup_scripts,
            config_source_path=self._bridge_cfg_path,
        )

        # Roslaunch configuration
        roslaunch_raw = self._bridge_cfg.get("roslaunch") or {}
        if not isinstance(roslaunch_raw, dict):
            roslaunch_raw = {}
        self._roslaunch_package: str = str(roslaunch_raw.get("package", ""))
        self._roslaunch_launch_file: str = str(roslaunch_raw.get("launch_file", ""))
        self._roslaunch_auto_start: bool = bool(roslaunch_raw.get("auto_start", False))
        self._roslaunch_args: dict[str, str] = {
            str(k): str(v) for k, v in (roslaunch_raw.get("args") or {}).items()
        }

        # ROS runtime state (populated on start)
        self._ros_subs: list[Any] = []
        self._ros_pubs: dict[str, Any] = {}  # command_name → rospy.Publisher
        self._pub_msg_types: dict[str, type] = {}  # command_name → msg class

        self._spin_thread: threading.Thread | None = None
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
        caps = ["reset", "ping", "roslaunch_start", "roslaunch_stop", "rosbag_start", "rosbag_stop"]
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
                "auto_start": self._roslaunch_auto_start,
            }
        return out

    def get_state_payload(self) -> dict[str, Any]:
        with self._values_lock:
            return {
                "node_name": self._node_name,
                "subscriptions_active": len(self._ros_subs),
                "publishers_active": len(self._ros_pubs),
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
            "roslaunch": {
                "package": self._roslaunch_package,
                "launch_file": self._roslaunch_launch_file,
                "auto_start": self._roslaunch_auto_start,
                "args": dict(self._roslaunch_args),
            },
        }

    def schema(self) -> dict[str, Any]:
        s = copy.deepcopy(super().schema())
        s["publishes"]["state"]["fields"].update({
            "node_name": {"type": "string"},
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
        })
        s["publishes"]["state"]["fields"].update({
            "roslaunch_package": {"type": "string"},
            "roslaunch_launch_file": {"type": "string"},
        })
        s["publishes"]["cfg"]["fields"].update({
            "node_name": {"type": "string"},
            "auto_discover": {"type": "boolean"},
            "max_auto_discover_publishers": {"type": "integer"},
            "max_telemetry_payload_bytes": {"type": "integer"},
            "exclude_topics": {"type": "array", "items": {"type": "string"}},
            "roslaunch": {
                "type": "object",
                "fields": {
                    "package": {"type": "string"},
                    "launch_file": {"type": "string"},
                    "auto_start": {"type": "boolean"},
                    "args": {"type": "object"},
                },
            },
        })
        s["subscribes"].update({
            "cmd/roslaunch_start": {
                "fields": {
                    "package": {"type": "string", "description": "Defaults to config roslaunch.package"},
                    "launch_file": {"type": "string", "description": "Defaults to config roslaunch.launch_file"},
                    "args": {"type": ["object", "string"], "description": "Dict of key:=value pairs or legacy string"},
                },
            },
            "cmd/roslaunch_stop": {"fields": {}},
            "cmd/rosbag_start": {
                "fields": {
                    "output_dir": {"type": "string", "default": "/tmp"},
                    "topics": {"type": "array", "items": {"type": "string"}},
                    "prefix": {"type": "string", "default": "lucid"},
                },
            },
            "cmd/rosbag_stop": {"fields": {}},
        })
        return s

    # -- lifecycle --------------------------------------------------------

    def _start(self) -> None:
        if self._setup_script_paths:
            _source_setup_scripts(self._setup_script_paths)

        try:
            import rospy
        except ImportError as exc:
            raise RuntimeError(
                "rospy is not available. Install ROS 1 and source both "
                "/opt/ros/noetic/setup.bash and your workspace setup.bash "
                "before running the ROS bridge component."
            ) from exc

        # Check ROS master is reachable before attempting init_node(),
        # which blocks indefinitely if the master is down.
        master_uri = os.environ.get("ROS_MASTER_URI", "http://localhost:11311")
        try:
            from urllib.parse import urlparse
            parsed = urlparse(master_uri)
            host = parsed.hostname or "localhost"
            port = parsed.port or 11311
            import socket
            sock = socket.create_connection((host, port), timeout=5)
            sock.close()
        except (OSError, socket.timeout) as exc:
            raise RuntimeError(
                f"ROS master unreachable at {master_uri} — "
                "ensure roscore is running and ROS_MASTER_URI is correct."
            ) from exc

        # Initialise the ROS node (anonymous=True avoids name collisions if
        # multiple bridges run on the same machine, disable_signals=True so
        # rospy doesn't hijack SIGINT from the LUCID agent process).
        # rospy.init_node() may only be called once per process — skip on restart.
        global _ros_node_initialized
        if not _ros_node_initialized:
            rospy.init_node(self._node_name, anonymous=True, disable_signals=True)
            _ros_node_initialized = True
            self._log.info("ROS node '%s' initialised", self._node_name)
        else:
            self._log.info(
                "ROS node already initialised (process-level singleton); "
                "skipping rospy.init_node() on restart"
            )

        # Auto-discover topics from ROS master if enabled
        if self._auto_discover:
            explicit_sub_topics = {s["ros_topic"] for s in self._ros_subscriptions}
            explicit_pub_topics = {p["ros_topic"] for p in self._ros_publishers}
            discovered_subs, discovered_pubs = _discover_ros_topics(
                self._exclude_topics, explicit_sub_topics, explicit_pub_topics,
            )
            if len(discovered_pubs) > self._max_auto_discover_publishers:
                raise RuntimeError(
                    f"auto_discover found {len(discovered_pubs)} publishers, which exceeds "
                    f"max_auto_discover_publishers={self._max_auto_discover_publishers}. "
                    "Raise the limit in ros_bridge.yaml or add topics to exclude_topics."
                )
            self._ros_subscriptions.extend(discovered_subs)
            self._ros_publishers.extend(discovered_pubs)

        self._setup_ros_subscriptions()
        self._setup_ros_publishers()

        # Build telemetry config from subscriptions
        telemetry_cfg = {
            sub["telemetry_metric"]: {
                "enabled": True,
                "interval_s": 1,
                "change_threshold_percent": 0.0,
            }
            for sub in self._ros_subscriptions
        }
        self.set_telemetry_config(telemetry_cfg)

        self._publish_all_retained()

        # Keep the ROS node alive in a background thread.
        # rospy.spin() blocks until rospy.is_shutdown(), so we use a polling
        # loop that also checks our own stop event.
        self._stop_event.clear()
        self._spin_thread = threading.Thread(target=self._spin_loop, daemon=True)
        self._spin_thread.start()

        # Auto-launch roslaunch if configured
        if self._roslaunch_auto_start and self._roslaunch_package and self._roslaunch_launch_file:
            self._log.info(
                "Auto-starting roslaunch: %s %s",
                self._roslaunch_package,
                self._roslaunch_launch_file,
            )
            ok = self._do_roslaunch_start(
                self._roslaunch_package,
                self._roslaunch_launch_file,
                self._roslaunch_args,
                publish_result=False,
            )
            if not ok:
                self._log.error("Auto-start roslaunch failed")

    def _stop(self) -> None:
        self._stop_event.set()

        # Stop any running subprocesses
        self._kill_subprocess(self._roslaunch_proc, "roslaunch", sigint_timeout=15)
        self._roslaunch_proc = None
        self._roslaunch_state = "idle"
        self._kill_subprocess(self._rosbag_proc, "rosbag", sigint_timeout=10)
        self._rosbag_proc = None
        self._rosbag_state = "idle"

        if self._spin_thread is not None:
            self._spin_thread.join(timeout=5.0)
            self._spin_thread = None

        # Important: do NOT call rospy.signal_shutdown() here.
        # rospy.init_node() is process-wide and may only be called once; shutting
        # down rospy would make a component restart impossible without restarting
        # the whole agent process. Instead, unregister all pubs/subs so this
        # component stops all ROS network activity when stopped.
        for sub in list(self._ros_subs):
            try:
                sub.unregister()
            except Exception:
                self._log.exception("Failed to unregister ROS subscriber")

        for pub in list(self._ros_pubs.values()):
            try:
                pub.unregister()
            except Exception:
                self._log.exception("Failed to unregister ROS publisher")

        self._ros_subs.clear()
        self._ros_pubs.clear()
        self._pub_msg_types.clear()

        self._log.info("ROS bridge stopped")

    # -- ROS setup --------------------------------------------------------

    def _setup_ros_subscriptions(self) -> None:
        import rospy

        for sub_cfg in self._ros_subscriptions:
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
                continue

            def _make_callback(metric_name: str):
                def _callback(msg: Any) -> None:
                    data = _msg_to_dict(msg)
                    with self._values_lock:
                        self._latest_values[metric_name] = data
                    if self.should_publish_telemetry(metric_name, data):
                        if self._max_telemetry_payload_bytes is not None:
                            encoded = json.dumps(data).encode("utf-8")
                            if len(encoded) > self._max_telemetry_payload_bytes:
                                self._log.warning(
                                    "Telemetry payload for metric '%s' is %d bytes, "
                                    "exceeds max_telemetry_payload_bytes=%d; skipping publish",
                                    metric_name, len(encoded), self._max_telemetry_payload_bytes,
                                )
                                return
                        self.publish_telemetry(metric_name, data)
                return _callback

            sub = rospy.Subscriber(ros_topic, msg_type, _make_callback(metric))
            self._ros_subs.append(sub)
            self._log.info(
                "Subscribed ROS topic %s → telemetry/%s", ros_topic, metric,
            )

    def _setup_ros_publishers(self) -> None:
        import rospy

        for pub_cfg in self._ros_publishers:
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
                continue

            pub = rospy.Publisher(ros_topic, msg_type, queue_size=10)
            self._ros_pubs[command] = pub
            self._pub_msg_types[command] = msg_type
            self._log.info(
                "ROS publisher %s ← cmd/%s", ros_topic, command,
            )

    def _spin_loop(self) -> None:
        """Keep the ROS node alive until stop is requested."""
        import rospy

        rate = rospy.Rate(10)  # 10 Hz check loop
        while not self._stop_event.is_set() and not rospy.is_shutdown():
            try:
                rate.sleep()
            except rospy.ROSInterruptException:
                break

    # -- retained publishing -----------------------------------------------

    def _publish_all_retained(self) -> None:
        self.publish_metadata()
        self.publish_schema()
        self.publish_status()
        self.publish_state()
        self.publish_cfg()

    # -- command handlers --------------------------------------------------

    def on_cmd_reset(self, payload_str: str) -> None:
        """Handle cmd/reset → evt/reset/result."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        with self._values_lock:
            self._latest_values.clear()

        self.publish_state()
        self.publish_result("reset", request_id, ok=True, error=None)

    def on_cmd_ping(self, payload_str: str) -> None:
        """Handle cmd/ping → evt/ping/result."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""
        self.publish_result("ping", request_id, ok=True, error=None)

    def on_cmd_cfg_set(self, payload_str: str) -> None:
        """Handle cmd/cfg/set → evt/cfg/set/result."""
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

        # Takes effect immediately
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

        if "roslaunch" in set_dict:
            rl = set_dict["roslaunch"]
            if isinstance(rl, dict):
                if "auto_start" in rl:
                    self._roslaunch_auto_start = bool(rl["auto_start"])
                    applied.setdefault("roslaunch", {})["auto_start"] = self._roslaunch_auto_start
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

        # Cannot change at runtime — restart required
        for key in ("ros_subscriptions", "ros_publishers"):
            if key in set_dict:
                rejected[key] = "cannot be changed at runtime; restart required"

        # Unknown keys
        known_keys = {
            "node_name", "auto_discover", "max_auto_discover_publishers",
            "max_telemetry_payload_bytes", "exclude_topics",
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

    # -- subprocess helpers -------------------------------------------------

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

    # -- roslaunch command handlers -----------------------------------------

    def _do_roslaunch_start(
        self,
        package: str,
        launch_file: str,
        args: dict[str, str],
        *,
        request_id: str = "",
        publish_result: bool = True,
    ) -> bool:
        """Launch a roslaunch subprocess. Returns True on success."""
        if self._roslaunch_proc is not None and self._roslaunch_proc.poll() is None:
            if publish_result:
                self.publish_result("roslaunch_start", request_id, ok=False, error="Already running")
            return False

        if not package or not launch_file:
            if publish_result:
                self.publish_result(
                    "roslaunch_start", request_id, ok=False,
                    error="package and launch_file required",
                )
            return False

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

        # Monitor thread: updates state if roslaunch exits on its own
        def _monitor() -> None:
            if self._roslaunch_proc is not None:
                self._roslaunch_proc.wait()
                rc = self._roslaunch_proc.returncode
                self._roslaunch_proc = None
                self._roslaunch_state = "exited"
                self._log.info("roslaunch exited with code %s", rc)
                self.publish_state()

        threading.Thread(target=_monitor, daemon=True).start()
        return True

    def on_cmd_roslaunch_start(self, payload_str: str) -> None:
        """Start a roslaunch subprocess. Uses YAML config as defaults; payload overrides."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            data = payload.get("data", payload)
        except json.JSONDecodeError:
            request_id, data = "", {}

        package = data.get("package", "") or self._roslaunch_package
        launch_file = data.get("launch_file", "") or self._roslaunch_launch_file

        # Merge args: config defaults + payload overrides
        args = dict(self._roslaunch_args)
        payload_args = data.get("args", "")
        if isinstance(payload_args, dict):
            args.update({str(k): str(v) for k, v in payload_args.items()})
        elif isinstance(payload_args, str) and payload_args.strip():
            # Backward compat: parse "key:=value" pairs from string
            for token in shlex.split(payload_args):
                if ":=" in token:
                    k, _, v = token.partition(":=")
                    args[k] = v

        self._do_roslaunch_start(package, launch_file, args, request_id=request_id)

    def on_cmd_roslaunch_stop(self, payload_str: str) -> None:
        """Stop the running roslaunch subprocess."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if self._roslaunch_proc is None or self._roslaunch_proc.poll() is not None:
            self.publish_result("roslaunch_stop", request_id, ok=False, error="Not running")
            return

        self._roslaunch_state = "stopping"
        rc = self._kill_subprocess(self._roslaunch_proc, "roslaunch", sigint_timeout=15)
        self._roslaunch_proc = None
        self._roslaunch_state = "idle"
        self.publish_state()
        self.publish_result("roslaunch_stop", request_id, ok=True, error=None)

    # -- rosbag command handlers --------------------------------------------

    def on_cmd_rosbag_start(self, payload_str: str) -> None:
        """Start a rosbag record subprocess."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
            data = payload.get("data", payload)
        except json.JSONDecodeError:
            request_id, data = "", {}

        output_dir = data.get("output_dir", "/tmp")
        topics = data.get("topics", "__all__")
        prefix = data.get("prefix", "lucid")

        if self._rosbag_proc is not None and self._rosbag_proc.poll() is None:
            self.publish_result("rosbag_start", request_id, ok=False, error="Already recording")
            return

        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        bag_name = f"{prefix}_{timestamp}"
        self._rosbag_path = os.path.join(output_dir, bag_name)
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
            )
        except FileNotFoundError as exc:
            self._rosbag_state = "idle"
            self.publish_result("rosbag_start", request_id, ok=False, error=str(exc))
            return

        self._rosbag_state = "recording"
        self.publish_state()
        self.publish_result("rosbag_start", request_id, ok=True, error=None)

    def on_cmd_rosbag_stop(self, payload_str: str) -> None:
        """Stop the running rosbag record subprocess."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "")
        except json.JSONDecodeError:
            request_id = ""

        if self._rosbag_proc is None or self._rosbag_proc.poll() is None:
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

    # -- ROS topic publish handler ------------------------------------------

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
            self._log.error("Failed to publish ROS message for cmd/%s: %s", command, exc)
            self.publish_result(
                command, request_id, ok=False, error=str(exc),
            )

    def __getattr__(self, name: str) -> Any:
        """Route on_cmd_<command> calls to handle_ros_publish for configured publishers."""
        if name.startswith("on_cmd_"):
            command = name[7:]  # strip "on_cmd_"
            if command in self._ros_pubs:
                def _handler(payload_str: str) -> None:
                    self.handle_ros_publish(command, payload_str)
                return _handler
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")
