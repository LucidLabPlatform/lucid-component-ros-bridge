"""
Extended coverage tests for the ROS Bridge component.

Covers paths not reached by test_contract.py:
  - handle_ros_publish (success, unknown command, publish exception)
  - __getattr__ routing for configured ros_publishers
  - _msg_to_dict with nested messages and list fields
  - _dict_to_msg with nested messages (fallback path)
  - _discover_ros_topics when ROS master query raises
  - _resolve_msg_type both formats (slash and dotted)
  - _load_bridge_config working-directory fallback
  - on_cmd_reset publishes result
  - on_cmd_ping with malformed JSON
  - on_cmd_cfg_set with no recognised keys (noop applied)
  - get_state_payload after values are populated
  - get_cfg_payload with exclude_topics populated
  - metadata with zero subs/pubs
  - Component stop from RUNNING state via mock rospy
  - _spin_loop exit via stop_event
"""
from __future__ import annotations

import json
import os
import sys
import textwrap
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from lucid_component_base import ComponentContext, ComponentStatus

from lucid_component_ros_bridge import RosBridgeComponent
from lucid_component_ros_bridge.component import (
    _dict_to_msg,
    _discover_ros_topics,
    _load_bridge_config,
    _load_bridge_config_with_source,
    _msg_to_dict,
    _resolve_setup_script_paths,
    _resolve_msg_type,
    _topic_to_command,
    _topic_to_metric,
    _utc_iso,
)
from tests.conftest import FakeMqtt, make_context as _fake_context, write_yaml as _write_yaml


def _comp_with_publishers(tmp_path: Path) -> RosBridgeComponent:
    """Return a component configured with one ros_publisher."""
    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_bot
        ros_subscriptions: []
        ros_publishers:
          - ros_topic: /cmd_vel
            msg_type: geometry_msgs/Twist
            command: cmd_vel
    """)
    return RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))


# ---------------------------------------------------------------------------
# _utc_iso
# ---------------------------------------------------------------------------

def test_utc_iso_returns_string():
    ts = _utc_iso()
    assert isinstance(ts, str)
    assert "T" in ts


# ---------------------------------------------------------------------------
# _topic_to_metric / _topic_to_command edge cases
# ---------------------------------------------------------------------------

def test_topic_to_metric_already_no_slash():
    assert _topic_to_metric("odom") == "odom"


def test_topic_to_metric_deep_path():
    assert _topic_to_metric("/a/b/c/d") == "a_b_c_d"


def test_topic_to_command_deep_path():
    assert _topic_to_command("/arm/joint/1") == "arm_joint_1"


# ---------------------------------------------------------------------------
# _load_bridge_config — working-directory fallback
# ---------------------------------------------------------------------------

def test_load_bridge_config_working_dir_fallback(tmp_path: Path, monkeypatch):
    """When no config_path is given and there is a ros_bridge.yaml in cwd, use it."""
    yaml_content = textwrap.dedent("""\
        node_name: cwd_bot
        auto_discover: false
        ros_subscriptions: []
        ros_publishers: []
    """)
    cwd_yaml = tmp_path / "ros_bridge.yaml"
    cwd_yaml.write_text(yaml_content, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    cfg = _load_bridge_config({})
    assert cfg["node_name"] == "cwd_bot"


def test_load_bridge_config_with_source_returns_selected_path(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: sourced_bot
        setup_scripts:
          - ./scripts/setup.bash
    """)
    cfg, source_path = _load_bridge_config_with_source({"config_path": str(cfg_file)})
    assert cfg["node_name"] == "sourced_bot"
    assert source_path == cfg_file.resolve()


def test_load_bridge_config_no_file_returns_empty(tmp_path: Path, monkeypatch):
    """When neither override nor working-dir yaml nor package default exists, return {}."""
    monkeypatch.chdir(tmp_path)
    # Patch _DEFAULT_CONFIG_PATH to a nonexistent file
    with patch(
        "lucid_component_ros_bridge.component._DEFAULT_CONFIG_PATH",
        tmp_path / "nonexistent.yaml",
    ):
        cfg = _load_bridge_config({})
    assert cfg == {}


def test_resolve_setup_script_paths_uses_config_directory(tmp_path: Path):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    setup_dir = cfg_dir / "scripts"
    setup_dir.mkdir()
    setup_script = setup_dir / "setup.bash"
    setup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    resolved = _resolve_setup_script_paths(
        ["./scripts/setup.bash"],
        config_source_path=(cfg_dir / "ros_bridge.yaml").resolve(),
    )

    assert resolved == [setup_script.resolve()]


# ---------------------------------------------------------------------------
# _resolve_msg_type
# ---------------------------------------------------------------------------

def test_resolve_msg_type_slash_format(monkeypatch):
    """geometry_msgs/Twist → import geometry_msgs.msg, getattr Twist."""
    fake_module = MagicMock()
    fake_class = MagicMock()
    fake_module.Twist = fake_class
    with patch.dict("sys.modules", {"geometry_msgs.msg": fake_module}):
        result = _resolve_msg_type("geometry_msgs/Twist")
    assert result is fake_class


def test_resolve_msg_type_dotted_format(monkeypatch):
    """geometry_msgs.msg.Twist → same result via dotted import."""
    fake_module = MagicMock()
    fake_class = MagicMock()
    fake_module.Twist = fake_class
    with patch.dict("sys.modules", {"geometry_msgs.msg": fake_module}):
        result = _resolve_msg_type("geometry_msgs.msg.Twist")
    assert result is fake_class


def test_resolve_msg_type_import_error_propagates():
    """If the module doesn't exist, ImportError propagates."""
    with pytest.raises(ModuleNotFoundError):
        _resolve_msg_type("nonexistent_pkg/Msg")


# ---------------------------------------------------------------------------
# _msg_to_dict — nested and list fields
# ---------------------------------------------------------------------------

def test_msg_to_dict_nested_message():
    """Nested ROS messages (objects with __slots__ and _type) are recursed."""
    class Inner:
        __slots__ = ["z"]
        _type = "std_msgs/Float32"
        def __init__(self):
            self.z = 9.9

    class Outer:
        __slots__ = ["inner_field"]
        def __init__(self):
            self.inner_field = Inner()

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        result = _msg_to_dict(Outer())

    assert isinstance(result["inner_field"], dict)
    assert result["inner_field"]["z"] == 9.9


def test_msg_to_dict_list_of_primitives():
    class Msg:
        __slots__ = ["values"]
        def __init__(self):
            self.values = [1, 2, 3]

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        result = _msg_to_dict(Msg())

    assert result["values"] == [1, 2, 3]


def test_msg_to_dict_list_of_messages():
    """Lists containing nested ROS messages are recursed element-by-element."""
    class Item:
        __slots__ = ["v"]
        def __init__(self, v):
            self.v = v

    class Container:
        __slots__ = ["items"]
        def __init__(self):
            self.items = [Item(10), Item(20)]

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        result = _msg_to_dict(Container())

    assert result["items"] == [{"v": 10}, {"v": 20}]


def test_msg_to_dict_uses_message_converter_when_available():
    fake_converter = MagicMock()
    fake_converter.message_converter.convert_ros_message_to_dictionary.return_value = {
        "x": 1.0
    }
    msg = MagicMock()
    with patch.dict("sys.modules", {"rospy_message_converter": fake_converter}):
        result = _msg_to_dict(msg)
    assert result == {"x": 1.0}


# ---------------------------------------------------------------------------
# _dict_to_msg — nested messages (fallback path)
# ---------------------------------------------------------------------------

def test_dict_to_msg_nested_message_fallback():
    """Nested dicts map to nested message objects when fallback is used."""
    class Inner:
        __slots__ = ["z"]  # ROS messages use __slots__; required for recursive conversion
        def __init__(self):
            self.z = 0.0

    class Outer:
        def __init__(self):
            self.inner = Inner()

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        result = _dict_to_msg({"inner": {"z": 7.7}}, Outer)

    assert result.inner.z == 7.7


def test_dict_to_msg_uses_message_converter_when_available():
    fake_msg = MagicMock()
    fake_converter = MagicMock()
    fake_converter.message_converter.convert_dictionary_to_ros_message.return_value = fake_msg

    class FakeType:
        _type = "geometry_msgs/Twist"

    with patch.dict("sys.modules", {"rospy_message_converter": fake_converter}):
        result = _dict_to_msg({"linear": {}}, FakeType)

    assert result is fake_msg


def test_dict_to_msg_message_converter_missing_type_attr():
    """If msg_type has no _type, message_converter path raises AttributeError → fallback."""
    fake_converter_module = MagicMock()
    fake_converter_module.message_converter.convert_dictionary_to_ros_message.side_effect = AttributeError

    class FakeType:
        def __init__(self):
            self.x = 0.0

    with patch.dict("sys.modules", {"rospy_message_converter": fake_converter_module}):
        result = _dict_to_msg({"x": 5.5}, FakeType)

    assert result.x == 5.5


# ---------------------------------------------------------------------------
# _discover_ros_topics — error path
# ---------------------------------------------------------------------------

def test_discover_ros_topics_master_query_fails():
    """If get_published_topics raises, return empty lists."""
    fake_rospy = MagicMock()
    fake_rospy.get_published_topics.side_effect = Exception("connection refused")
    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        subs, pubs = _discover_ros_topics(set(), set(), set())
    assert subs == []
    assert pubs == []


def test_max_telemetry_payload_bytes_none_by_default():
    comp = RosBridgeComponent(_fake_context())
    assert comp._max_telemetry_payload_bytes is None


def test_max_telemetry_payload_bytes_from_yaml(tmp_path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        max_telemetry_payload_bytes: 65536
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    assert comp._max_telemetry_payload_bytes == 65536


def test_subscription_callback_skips_oversized_payload(tmp_path, monkeypatch):
    """Payload exceeding max_telemetry_payload_bytes is not published."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        max_telemetry_payload_bytes: 10
        ros_subscriptions:
          - ros_topic: /odom
            msg_type: nav_msgs/Odometry
            telemetry_metric: odom
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_nav_msgs = MagicMock()
    fake_nav_msgs.Odometry = MagicMock()
    captured_callbacks = []

    def fake_subscriber(topic, msg_type, callback):
        captured_callbacks.append(callback)
        return MagicMock()

    fake_rospy.Subscriber.side_effect = fake_subscriber

    with patch.dict("sys.modules", {"rospy": fake_rospy, "nav_msgs.msg": fake_nav_msgs}):
        comp.start()

        class BigMsg:
            __slots__ = ["data"]
            def __init__(self):
                self.data = "x" * 100  # well over 10 bytes

        with patch.dict("sys.modules", {"rospy_message_converter": None}):
            captured_callbacks[0](BigMsg())

        # latest_values is still updated
        assert "odom" in comp._latest_values
        # but telemetry was NOT published
        telemetry_topics = [t for t, _, _, _ in ctx.mqtt.published if "telemetry/odom" in t]
        assert len(telemetry_topics) == 0
        comp.stop()


def test_subscription_callback_publishes_within_limit(tmp_path, monkeypatch):
    """Payload within max_telemetry_payload_bytes is published normally."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        max_telemetry_payload_bytes: 1000000
        ros_subscriptions:
          - ros_topic: /odom
            msg_type: nav_msgs/Odometry
            telemetry_metric: odom
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_nav_msgs = MagicMock()
    fake_nav_msgs.Odometry = MagicMock()
    captured_callbacks = []

    def fake_subscriber(topic, msg_type, callback):
        captured_callbacks.append(callback)
        return MagicMock()

    fake_rospy.Subscriber.side_effect = fake_subscriber

    with patch.dict("sys.modules", {"rospy": fake_rospy, "nav_msgs.msg": fake_nav_msgs}):
        comp.start()

        class SmallMsg:
            __slots__ = ["x"]
            def __init__(self):
                self.x = 1.0

        with patch.dict("sys.modules", {"rospy_message_converter": None}):
            captured_callbacks[0](SmallMsg())

        telemetry_topics = [t for t, _, _, _ in ctx.mqtt.published if "telemetry/odom" in t]
        assert len(telemetry_topics) >= 1
        comp.stop()


def test_cfg_payload_includes_max_telemetry_payload_bytes(tmp_path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        max_telemetry_payload_bytes: 524288
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    assert comp.get_cfg_payload()["max_telemetry_payload_bytes"] == 524288


def test_max_auto_discover_publishers_default(tmp_path):
    """Default cap is 50."""
    comp = RosBridgeComponent(_fake_context())
    assert comp._max_auto_discover_publishers == 50


def test_max_auto_discover_publishers_from_yaml(tmp_path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: true
        max_auto_discover_publishers: 10
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    assert comp._max_auto_discover_publishers == 10


def test_max_auto_discover_publishers_exceeded_raises(tmp_path, monkeypatch):
    """start() raises RuntimeError when discovered publishers exceed the cap."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: true
        max_auto_discover_publishers: 2
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_rospy.get_published_topics.return_value = [
        ("/a", "std_msgs/String"),
        ("/b", "std_msgs/String"),
        ("/c", "std_msgs/String"),  # 3 topics > cap of 2
    ]

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        with pytest.raises(RuntimeError, match="max_auto_discover_publishers"):
            comp.start()

    assert comp.state.status == ComponentStatus.FAILED


def test_max_auto_discover_publishers_within_limit_starts_ok(tmp_path, monkeypatch):
    """start() succeeds when discovered publishers are within the cap."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: true
        max_auto_discover_publishers: 5
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_nav_msgs = MagicMock()
    fake_nav_msgs.Odometry = MagicMock()
    fake_rospy.get_published_topics.return_value = [("/odom", "nav_msgs/Odometry")]

    with patch.dict("sys.modules", {"rospy": fake_rospy, "nav_msgs.msg": fake_nav_msgs}):
        comp.start()
        assert comp.state.status == ComponentStatus.RUNNING
        comp.stop()


# ---------------------------------------------------------------------------
# cfg/set expanded keys
# ---------------------------------------------------------------------------

def test_cmd_cfg_set_auto_discover():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-ad", "set": {"auto_discover": False}}))
    assert comp._auto_discover is False
    result = [json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t]
    assert result[0]["ok"] is True
    assert result[0]["applied"]["auto_discover"] is False


def test_cmd_cfg_set_max_auto_discover_publishers():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-m", "set": {"max_auto_discover_publishers": 10}}))
    assert comp._max_auto_discover_publishers == 10


def test_cmd_cfg_set_max_telemetry_payload_bytes():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-tp", "set": {"max_telemetry_payload_bytes": 262144}}))
    assert comp._max_telemetry_payload_bytes == 262144


def test_cmd_cfg_set_max_telemetry_payload_bytes_null():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-null", "set": {"max_telemetry_payload_bytes": None}}))
    assert comp._max_telemetry_payload_bytes is None


def test_cmd_cfg_set_exclude_topics():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-et", "set": {"exclude_topics": ["/rosout", "/clock"]}}))
    assert "/rosout" in comp._exclude_topics
    assert "/clock" in comp._exclude_topics
    result = [json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t]
    assert result[0]["ok"] is True


def test_cmd_cfg_set_exclude_topics_invalid_type():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-bad", "set": {"exclude_topics": "not_a_list"}}))
    result = [json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t]
    assert result[0]["ok"] is True
    rejected = json.loads(result[0]["error"])["rejected"]
    assert "exclude_topics" in rejected


def test_cmd_cfg_set_ros_subscriptions_rejected():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-rs", "set": {"ros_subscriptions": []}}))
    result = [json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t]
    assert result[0]["ok"] is True
    rejected = json.loads(result[0]["error"])["rejected"]
    assert "ros_subscriptions" in rejected


def test_cmd_cfg_set_unknown_key_rejected():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r-uk", "set": {"totally_unknown": 99}}))
    result = [json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t]
    assert result[0]["ok"] is True
    rejected = json.loads(result[0]["error"])["rejected"]
    assert "totally_unknown" in rejected


# ---------------------------------------------------------------------------
# handle_ros_publish
# ---------------------------------------------------------------------------

def test_handle_ros_publish_no_publisher_configured():
    """Command with no matching publisher → result ok=False."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.handle_ros_publish("unknown_cmd", json.dumps({"request_id": "r1"}))
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/unknown_cmd/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is False
    assert "unknown_cmd" in result_msgs[0]["error"]


def test_handle_ros_publish_publish_exception(tmp_path: Path):
    """If pub.publish raises, result is ok=False with error message."""
    comp = _comp_with_publishers(tmp_path)

    fake_pub = MagicMock()
    fake_pub.publish.side_effect = RuntimeError("ROS not running")
    fake_msg_type = MagicMock(return_value=MagicMock())

    comp._ros_pubs["cmd_vel"] = fake_pub
    comp._pub_msg_types["cmd_vel"] = fake_msg_type

    comp.handle_ros_publish(
        "cmd_vel",
        json.dumps({"request_id": "r99", "data": {"linear": {"x": 1.0}}}),
    )

    ctx = comp.context
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cmd_vel/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is False
    assert "ROS not running" in result_msgs[0]["error"]


def test_handle_ros_publish_success(tmp_path: Path):
    """Successful ROS publish → result ok=True."""
    comp = _comp_with_publishers(tmp_path)

    fake_pub = MagicMock()
    fake_msg_instance = MagicMock()
    fake_msg_type = MagicMock(return_value=fake_msg_instance)

    comp._ros_pubs["cmd_vel"] = fake_pub
    comp._pub_msg_types["cmd_vel"] = fake_msg_type

    comp.handle_ros_publish(
        "cmd_vel",
        json.dumps({"request_id": "r10", "data": {"linear": {"x": 0.5}}}),
    )

    fake_pub.publish.assert_called_once()
    ctx = comp.context
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cmd_vel/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is True


def test_handle_ros_publish_malformed_json(tmp_path: Path):
    """Malformed JSON payload → falls back gracefully, result ok=False (no pub)."""
    comp = _comp_with_publishers(tmp_path)
    # No actual publisher registered → no-pub branch, but exercises JSON parse error path
    comp.handle_ros_publish("cmd_vel", "NOT_JSON")
    ctx = comp.context
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cmd_vel/result" in t
    ]
    # No publisher set up, so result is the "no publisher" error
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is False


# ---------------------------------------------------------------------------
# __getattr__ routing
# ---------------------------------------------------------------------------

def test_getattr_routes_to_handle_ros_publish(tmp_path: Path):
    """on_cmd_<command> for a configured publisher is routed to handle_ros_publish."""
    comp = _comp_with_publishers(tmp_path)

    fake_pub = MagicMock()
    fake_msg_type = MagicMock(return_value=MagicMock())
    comp._ros_pubs["cmd_vel"] = fake_pub
    comp._pub_msg_types["cmd_vel"] = fake_msg_type

    # Access via __getattr__ (on_cmd_cmd_vel is not defined on the class)
    handler = comp.on_cmd_cmd_vel
    handler(json.dumps({"request_id": "r20", "data": {}}))

    fake_pub.publish.assert_called_once()


def test_getattr_on_cmd_not_in_ros_pubs_raises(tmp_path: Path):
    """on_cmd_<command> for a command not in _ros_pubs raises AttributeError."""
    comp = _comp_with_publishers(tmp_path)
    # cmd_vel is configured but not wired up with a real pub — __getattr__ should
    # return a handler because 'cmd_vel' IS in _ros_publishers config, but the
    # publisher object is in _ros_pubs dict. Without a pub in _ros_pubs, the
    # __getattr__ guard `command in self._ros_pubs` is False.
    with pytest.raises(AttributeError):
        _ = comp.on_cmd_completely_unknown_xyz


# ---------------------------------------------------------------------------
# Command handlers — edge cases
# ---------------------------------------------------------------------------

def test_cmd_reset_publishes_result():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_reset(json.dumps({"request_id": "reset-1"}))
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/reset/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is True
    assert result_msgs[0]["request_id"] == "reset-1"


def test_cmd_ping_malformed_json():
    """on_cmd_ping with invalid JSON still publishes a result (empty request_id)."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_ping("NOT_JSON")
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/ping/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is True


def test_cmd_reset_malformed_json():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp._latest_values["x"] = {"v": 1}
    comp.on_cmd_reset("INVALID")
    assert comp._latest_values == {}


def test_cmd_cfg_set_no_recognised_keys():
    """cfg/set with no recognised key → ok=True, unknown key appears in rejected."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({"request_id": "r5", "set": {"unknown_key": 42}}))
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t
    ]
    assert len(result_msgs) == 1
    assert result_msgs[0]["ok"] is True
    rejected = json.loads(result_msgs[0]["error"])["rejected"]
    assert "unknown_key" in rejected


def test_cmd_cfg_set_empty_payload():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set("")
    # Empty payload — should not crash; result published with empty request_id
    result_msgs = [
        json.loads(p) for t, p, _, _ in ctx.mqtt.published if "evt/cfg/set/result" in t
    ]
    assert len(result_msgs) == 1


# ---------------------------------------------------------------------------
# get_state_payload with populated latest_values
# ---------------------------------------------------------------------------

def test_get_state_payload_with_values():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp._latest_values["odom"] = {"x": 1.0, "y": 2.0}
    state = comp.get_state_payload()
    assert state["latest_values"]["odom"]["x"] == 1.0
    assert state["subscriptions_active"] == 0  # subs not wired yet


# ---------------------------------------------------------------------------
# get_cfg_payload with exclude_topics
# ---------------------------------------------------------------------------

def test_get_cfg_payload_with_exclude_topics(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: true
        exclude_topics:
          - /rosout
          - /clock
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    cfg = comp.get_cfg_payload()
    assert "/rosout" in cfg["exclude_topics"]
    assert "/clock" in cfg["exclude_topics"]
    assert cfg["auto_discover"] is True


# ---------------------------------------------------------------------------
# metadata shape
# ---------------------------------------------------------------------------

def test_metadata_shape_default():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    meta = comp.metadata()
    assert "component_id" in meta
    assert "version" in meta
    assert "node_name" in meta
    assert meta["ros_subscriptions"] == []
    assert meta["ros_publishers"] == []


# ---------------------------------------------------------------------------
# Component start / stop lifecycle with mocked rospy
# ---------------------------------------------------------------------------

def _make_fake_rospy():
    """Build a mock rospy module that behaves well enough for _start()/_stop()."""
    fake_rospy = MagicMock()
    fake_rospy.is_shutdown.return_value = False

    # Rate mock: sleep does nothing
    rate_mock = MagicMock()
    rate_mock.sleep = MagicMock()
    fake_rospy.Rate.return_value = rate_mock

    # Subscriber mock
    fake_rospy.Subscriber.return_value = MagicMock()
    # Publisher mock
    fake_rospy.Publisher.return_value = MagicMock()
    return fake_rospy


def test_start_runs_with_mocked_rospy(tmp_path: Path, monkeypatch):
    """Component starts successfully when rospy is mocked."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        comp.start()
        assert comp.state.status == ComponentStatus.RUNNING
        fake_rospy.init_node.assert_called_once()
        comp.stop()


def test_start_sources_setup_scripts_before_import(tmp_path: Path, monkeypatch):
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    setup_script = scripts_dir / "setup.bash"
    setup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    rospy_dir = tmp_path / "fake_ros"
    rospy_dir.mkdir()
    (rospy_dir / "rospy.py").write_text(
        textwrap.dedent(
            """\
            shutdown = False

            class ROSInterruptException(Exception):
                pass

            def init_node(*args, **kwargs):
                return None

            def is_shutdown():
                return True

            class _Rate:
                def __init__(self, hz):
                    self.hz = hz

                def sleep(self):
                    return None

            def Rate(hz):
                return _Rate(hz)

            def Subscriber(*args, **kwargs):
                return object()

            def Publisher(*args, **kwargs):
                return object()

            def signal_shutdown(reason):
                global shutdown
                shutdown = True

            def get_published_topics():
                return []
            """
        ),
        encoding="utf-8",
    )

    cfg_file = _write_yaml(tmp_path, f"""\
        node_name: test_node
        auto_discover: false
        setup_scripts:
          - ./scripts/setup.bash
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    run_result = MagicMock()
    run_result.returncode = 0
    run_result.stdout = f"PYTHONPATH={rospy_dir}\x00".encode()
    run_result.stderr = b""

    old_pythonpath = os.environ.get("PYTHONPATH")
    sys.modules.pop("rospy", None)
    try:
        with patch("lucid_component_ros_bridge.component.subprocess.run", return_value=run_result) as mock_run:
            comp.start()
            assert comp.state.status == ComponentStatus.RUNNING
            assert os.environ["PYTHONPATH"] == str(rospy_dir)
            assert str(rospy_dir) in sys.path
            source_cmd = mock_run.call_args.args[0][2]
            assert source_cmd.startswith("set -eo pipefail; ")
            assert "set -euo pipefail" not in source_cmd
            comp.stop()
    finally:
        sys.modules.pop("rospy", None)
        if old_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = old_pythonpath
        while str(rospy_dir) in sys.path:
            sys.path.remove(str(rospy_dir))


def test_stop_after_start_with_mocked_rospy(tmp_path: Path, monkeypatch):
    """Component transitions to STOPPED after stop() when rospy is mocked."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    # Make is_shutdown return True immediately so spin loop exits fast
    fake_rospy.is_shutdown.return_value = True

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        comp.start()
        comp.stop()

    assert comp.state.status == ComponentStatus.STOPPED


def test_restart_skips_init_node(tmp_path: Path, monkeypatch):
    """Second start() does not call rospy.init_node() again."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions: []
        ros_publishers: []
    """)
    fake_rospy = _make_fake_rospy()
    fake_rospy.is_shutdown.return_value = True

    comp1 = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    comp2 = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        comp1.start()
        comp1.stop()
        comp2.start()
        comp2.stop()

    assert fake_rospy.init_node.call_count == 1


def test_repeated_start_does_not_duplicate_pythonpath(tmp_path: Path, monkeypatch):
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    setup_script = scripts_dir / "setup.bash"
    setup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    rospy_dir = tmp_path / "fake_ros"
    rospy_dir.mkdir()
    (rospy_dir / "rospy.py").write_text(
        textwrap.dedent(
            """\
            class ROSInterruptException(Exception):
                pass

            def init_node(*args, **kwargs):
                return None

            def is_shutdown():
                return True

            class _Rate:
                def __init__(self, hz):
                    self.hz = hz

                def sleep(self):
                    return None

            def Rate(hz):
                return _Rate(hz)

            def Subscriber(*args, **kwargs):
                return object()

            def Publisher(*args, **kwargs):
                return object()

            def signal_shutdown(reason):
                return None
            """
        ),
        encoding="utf-8",
    )

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        setup_scripts:
          - ./scripts/setup.bash
        ros_subscriptions: []
        ros_publishers: []
    """)
    run_result = MagicMock(returncode=0, stdout=f"PYTHONPATH={rospy_dir}\x00".encode(), stderr=b"")

    comp1 = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))
    comp2 = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))

    old_pythonpath = os.environ.get("PYTHONPATH")
    sys.modules.pop("rospy", None)
    try:
        with patch("lucid_component_ros_bridge.component.subprocess.run", return_value=run_result):
            comp1.start()
            comp1.stop()
            comp2.start()
            comp2.stop()

        assert sys.path.count(str(rospy_dir)) == 1
    finally:
        sys.modules.pop("rospy", None)
        if old_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = old_pythonpath
        while str(rospy_dir) in sys.path:
            sys.path.remove(str(rospy_dir))


def test_start_with_missing_setup_script_raises(tmp_path: Path, monkeypatch):
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        setup_scripts:
          - ./missing/setup.bash
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))

    with pytest.raises(RuntimeError, match="ROS setup script not found"):
        comp.start()


def test_start_with_setup_script_shell_failure_raises(tmp_path: Path, monkeypatch):
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    setup_script = scripts_dir / "setup.bash"
    setup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        setup_scripts:
          - ./scripts/setup.bash
        ros_subscriptions: []
        ros_publishers: []
    """)
    comp = RosBridgeComponent(_fake_context(config={"config_path": str(cfg_file)}))

    run_result = MagicMock(returncode=1, stdout=b"", stderr=b"boom")
    with patch("lucid_component_ros_bridge.component.subprocess.run", return_value=run_result):
        with pytest.raises(RuntimeError, match="Failed to source ROS setup scripts: boom"):
            comp.start()


def test_stop_idempotent_when_already_stopped():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    assert comp.state.status == ComponentStatus.STOPPED
    comp.stop()  # Should not raise
    assert comp.state.status == ComponentStatus.STOPPED


def test_start_with_auto_discover_and_mocked_rospy(tmp_path: Path, monkeypatch):
    """auto_discover path is exercised: _discover_ros_topics is called."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: true
        exclude_topics:
          - /rosout
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_rospy.get_published_topics.return_value = [("/odom", "nav_msgs/Odometry")]

    # _resolve_msg_type will try to import nav_msgs.msg — mock that too
    fake_nav_msgs = MagicMock()
    fake_nav_msgs.Odometry = MagicMock()

    with patch.dict("sys.modules", {"rospy": fake_rospy, "nav_msgs.msg": fake_nav_msgs}):
        comp.start()
        assert comp.state.status == ComponentStatus.RUNNING
        # One subscription should have been auto-discovered
        assert len(comp._ros_subscriptions) == 1
        assert comp._ros_subscriptions[0]["ros_topic"] == "/odom"
        comp.stop()


def test_start_with_ros_subscription_msg_type_resolution_failure(tmp_path: Path, monkeypatch):
    """If msg_type resolution fails for a subscription, it is skipped (component still starts)."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions:
          - ros_topic: /bad
            msg_type: nonexistent_pkg/Msg
            telemetry_metric: bad_metric
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        comp.start()
        assert comp.state.status == ComponentStatus.RUNNING
        # Sub was skipped due to import error, so ros_subs list is empty
        assert comp._ros_subs == []
        comp.stop()


def test_start_with_ros_publisher_msg_type_resolution_failure(tmp_path: Path, monkeypatch):
    """If msg_type resolution fails for a publisher, it is skipped (component still starts)."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions: []
        ros_publishers:
          - ros_topic: /bad_pub
            msg_type: nonexistent_pkg/Twist
            command: bad_cmd
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()

    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        comp.start()
        assert comp.state.status == ComponentStatus.RUNNING
        assert comp._ros_pubs == {}
        comp.stop()


# ---------------------------------------------------------------------------
# _spin_loop exits when stop_event is set
# ---------------------------------------------------------------------------

def test_spin_loop_exits_on_stop_event():
    """_spin_loop terminates when _stop_event is set."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_rospy.is_shutdown.return_value = False

    call_count = 0
    def fake_sleep():
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            comp._stop_event.set()

    fake_rospy.Rate.return_value.sleep = fake_sleep

    comp._stop_event.clear()
    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        t = threading.Thread(target=comp._spin_loop)
        t.start()
        t.join(timeout=3.0)

    assert not t.is_alive()
    assert call_count >= 2


def test_spin_loop_exits_on_ros_interrupt():
    """_spin_loop exits cleanly when rospy.ROSInterruptException is raised."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)

    fake_rospy = MagicMock()
    fake_rospy.is_shutdown.return_value = False

    class FakeROSInterruptException(Exception):
        pass

    fake_rospy.ROSInterruptException = FakeROSInterruptException
    fake_rate = MagicMock()
    fake_rate.sleep.side_effect = FakeROSInterruptException("interrupted")
    fake_rospy.Rate.return_value = fake_rate

    comp._stop_event.clear()
    with patch.dict("sys.modules", {"rospy": fake_rospy}):
        t = threading.Thread(target=comp._spin_loop)
        t.start()
        t.join(timeout=3.0)

    assert not t.is_alive()


# ---------------------------------------------------------------------------
# Telemetry callback wiring (via ROS subscriber callback)
# ---------------------------------------------------------------------------

def test_ros_subscription_callback_publishes_telemetry(tmp_path: Path, monkeypatch):
    """When a ROS message is received, the callback publishes telemetry."""
    import lucid_component_ros_bridge.component as m
    monkeypatch.setattr(m, "_ros_node_initialized", False)

    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_node
        auto_discover: false
        ros_subscriptions:
          - ros_topic: /odom
            msg_type: nav_msgs/Odometry
            telemetry_metric: odom
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)

    fake_rospy = _make_fake_rospy()
    fake_nav_msgs = MagicMock()
    fake_nav_msgs.Odometry = MagicMock()

    # Capture the subscriber callback so we can invoke it manually
    captured_callbacks = []

    def fake_subscriber(topic, msg_type, callback):
        captured_callbacks.append(callback)
        return MagicMock()

    fake_rospy.Subscriber.side_effect = fake_subscriber

    with patch.dict("sys.modules", {"rospy": fake_rospy, "nav_msgs.msg": fake_nav_msgs}):
        comp.start()

        assert len(captured_callbacks) == 1

        # Simulate a ROS message arriving
        class FakeMsg:
            __slots__ = ["x"]
            def __init__(self):
                self.x = 42.0

        with patch.dict("sys.modules", {"rospy_message_converter": None}):
            captured_callbacks[0](FakeMsg())

        # latest_values should be updated
        assert comp._latest_values["odom"] == {"x": 42.0}

        # Telemetry should have been published (first call always passes gating)
        telemetry_topics = [
            t for t, _, _, _ in ctx.mqtt.published if "telemetry/odom" in t
        ]
        assert len(telemetry_topics) >= 1

        comp.stop()
