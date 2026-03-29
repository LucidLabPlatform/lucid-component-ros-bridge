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

import importlib
import json
import logging
import threading
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


def _load_bridge_config(context_config: dict[str, Any]) -> dict[str, Any]:
    """Load the bridge YAML config.

    Resolution order:
      1. context_config["config_path"] — explicit override from agent config
      2. ./ros_bridge.yaml             — next to the agent's working directory
      3. <package>/ros_bridge.yaml     — default shipped with the component
    """
    # 1. Explicit override
    explicit = context_config.get("config_path")
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise FileNotFoundError(f"ROS bridge config not found: {p}")
        logger.info("Loading ROS bridge config from explicit path: %s", p)
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # 2. Working-directory override
    local = Path("ros_bridge.yaml")
    if local.is_file():
        logger.info("Loading ROS bridge config from working dir: %s", local.resolve())
        with local.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    # 3. Default shipped with package
    if _DEFAULT_CONFIG_PATH.is_file():
        logger.info("Loading ROS bridge config from package default: %s", _DEFAULT_CONFIG_PATH)
        with _DEFAULT_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    logger.warning("No ros_bridge.yaml found — starting with empty config")
    return {}


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

        self._bridge_cfg = _load_bridge_config(dict(context.config))

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

        # ROS runtime state (populated on start)
        self._ros_subs: list[Any] = []
        self._ros_pubs: dict[str, Any] = {}  # command_name → rospy.Publisher
        self._pub_msg_types: dict[str, type] = {}  # command_name → msg class

        self._spin_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Latest values received from ROS (for state payload)
        self._latest_values: dict[str, dict[str, Any]] = {}
        self._values_lock = threading.Lock()

    @property
    def component_id(self) -> str:
        return "ros_bridge"

    def capabilities(self) -> list[str]:
        caps = ["reset", "ping"]
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
        return out

    def get_state_payload(self) -> dict[str, Any]:
        with self._values_lock:
            return {
                "node_name": self._node_name,
                "subscriptions_active": len(self._ros_subs),
                "publishers_active": len(self._ros_pubs),
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
        }

    # -- lifecycle --------------------------------------------------------

    def _start(self) -> None:
        try:
            import rospy
        except ImportError as exc:
            raise RuntimeError(
                "rospy is not available. Install ROS 1 and source both "
                "/opt/ros/noetic/setup.bash and your workspace setup.bash "
                "before running the ROS bridge component."
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

    def _stop(self) -> None:
        self._stop_event.set()

        try:
            import rospy
            rospy.signal_shutdown("LUCID component stopping")
        except ImportError:
            pass

        if self._spin_thread is not None:
            self._spin_thread.join(timeout=5.0)
            self._spin_thread = None

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

        # Cannot change at runtime — restart required
        for key in ("ros_subscriptions", "ros_publishers"):
            if key in set_dict:
                rejected[key] = "cannot be changed at runtime; restart required"

        # Unknown keys
        known_keys = {
            "node_name", "auto_discover", "max_auto_discover_publishers",
            "max_telemetry_payload_bytes", "exclude_topics",
            "ros_subscriptions", "ros_publishers",
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
