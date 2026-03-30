"""
Contract tests for the ROS Bridge component.

These tests verify the component contract (instantiation, state transitions,
capabilities, command handlers) without requiring a live ROS 1 environment.
ROS imports are mocked so tests run in any CI/dev environment.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from lucid_component_base import ComponentContext, ComponentStatus

from lucid_component_ros_bridge import RosBridgeComponent
from tests.conftest import make_context as _fake_context, write_yaml as _write_yaml


# -- instantiation --------------------------------------------------------

def test_component_instantiation():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    assert comp.component_id == "ros_bridge"
    assert comp.state.status == ComponentStatus.STOPPED


def test_component_loads_from_explicit_config_path(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: my_robot
        ros_subscriptions:
          - ros_topic: /odom
            msg_type: nav_msgs/Odometry
            telemetry_metric: odom
        ros_publishers:
          - ros_topic: /cmd_vel
            msg_type: geometry_msgs/Twist
            command: cmd_vel
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    assert comp._node_name == "my_robot"
    assert len(comp._ros_subscriptions) == 1
    assert len(comp._ros_publishers) == 1


def test_component_loads_default_yaml():
    """With no overrides, loads the bundled ros_bridge.yaml."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    assert comp._node_name == "lucid_ros_bridge"
    assert comp._auto_discover is True
    assert comp._ros_subscriptions == []
    assert comp._ros_publishers == []


def test_config_path_not_found_raises(tmp_path: Path):
    ctx = _fake_context(config={"config_path": str(tmp_path / "nonexistent.yaml")})
    with pytest.raises(FileNotFoundError, match="ROS bridge config not found"):
        RosBridgeComponent(ctx)


# -- capabilities ---------------------------------------------------------

def test_default_capabilities():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    caps = comp.capabilities()
    assert "reset" in caps
    assert "ping" in caps


def test_capabilities_include_configured_publishers(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: test
        ros_subscriptions: []
        ros_publishers:
          - ros_topic: /cmd_vel
            msg_type: geometry_msgs/Twist
            command: cmd_vel
          - ros_topic: /arm
            msg_type: std_msgs/String
            command: arm_move
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    caps = comp.capabilities()
    assert "cmd_vel" in caps
    assert "arm_move" in caps


# -- state payload --------------------------------------------------------

def test_get_state_payload_returns_dict():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    state = comp.get_state_payload()
    assert isinstance(state, dict)
    assert state["node_name"] == "lucid_ros_bridge"
    assert state["subscriptions_active"] == 0
    assert state["publishers_active"] == 0
    assert state["latest_values"] == {}


# -- cfg payload ----------------------------------------------------------

def test_get_cfg_payload(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_bot
        setup_scripts:
          - /opt/ros/noetic/setup.bash
        ros_subscriptions:
          - ros_topic: /scan
            msg_type: sensor_msgs/LaserScan
            telemetry_metric: lidar
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    cfg_payload = comp.get_cfg_payload()
    assert cfg_payload["node_name"] == "test_bot"
    assert cfg_payload["setup_scripts"] == ["/opt/ros/noetic/setup.bash"]
    assert len(cfg_payload["ros_subscriptions"]) == 1


# -- metadata -------------------------------------------------------------

def test_metadata_includes_ros_info(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: test_bot
        ros_subscriptions:
          - ros_topic: /odom
            msg_type: nav_msgs/Odometry
            telemetry_metric: odom
        ros_publishers:
          - ros_topic: /cmd_vel
            msg_type: geometry_msgs/Twist
            command: cmd_vel
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    meta = comp.metadata()
    assert meta["node_name"] == "test_bot"
    assert len(meta["ros_subscriptions"]) == 1
    assert len(meta["ros_publishers"]) == 1


# -- start requires rospy ------------------------------------------------

def test_start_fails_without_rospy():
    """Component should raise RuntimeError if rospy is not installed."""
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    with patch.dict("sys.modules", {"rospy": None}):
        with pytest.raises(RuntimeError, match="rospy is not available"):
            comp.start()
    assert comp.state.status == ComponentStatus.FAILED


# -- command handlers (no ROS needed) -------------------------------------

def test_cmd_ping():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_ping(json.dumps({"request_id": "r1"}))
    published = ctx.mqtt.published
    result_topics = [t for t, _, _, _ in published if "evt/ping/result" in t]
    assert len(result_topics) == 1


def test_cmd_reset_clears_values():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp._latest_values["odom"] = {"x": 1.0}
    comp.on_cmd_reset(json.dumps({"request_id": "r2"}))
    assert comp._latest_values == {}


def test_cmd_cfg_set():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "r3",
        "set": {"node_name": "new_name"},
    }))
    assert comp._node_name == "new_name"


def test_cmd_cfg_set_invalid():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "r4",
        "set": "not_a_dict",
    }))
    published = ctx.mqtt.published
    result_payloads = [
        json.loads(p) for t, p, _, _ in published if "evt/cfg/set/result" in t
    ]
    assert len(result_payloads) == 1
    assert result_payloads[0]["ok"] is False


# -- auto-discovery helpers -----------------------------------------------

def test_topic_to_metric():
    from lucid_component_ros_bridge.component import _topic_to_metric
    assert _topic_to_metric("/odom") == "odom"
    assert _topic_to_metric("/camera/rgb/image_raw") == "camera_rgb_image_raw"
    assert _topic_to_metric("/cmd_vel") == "cmd_vel"


def test_topic_to_command():
    from lucid_component_ros_bridge.component import _topic_to_command
    assert _topic_to_command("/cmd_vel") == "cmd_vel"
    assert _topic_to_command("/arm/joint1") == "arm_joint1"


def test_discover_ros_topics_basic():
    from lucid_component_ros_bridge.component import _discover_ros_topics

    fake_topics = [
        ("/odom", "nav_msgs/Odometry"),
        ("/cmd_vel", "geometry_msgs/Twist"),
        ("/scan", "sensor_msgs/LaserScan"),
    ]

    with patch.dict("sys.modules", {"rospy": MagicMock()}):
        import sys
        sys.modules["rospy"].get_published_topics.return_value = fake_topics

        subs, pubs = _discover_ros_topics(
            exclude_topics=set(),
            explicit_sub_topics=set(),
            explicit_pub_topics=set(),
        )

    assert len(subs) == 3
    assert len(pubs) == 3
    assert subs[0]["ros_topic"] == "/odom"
    assert subs[0]["telemetry_metric"] == "odom"
    assert subs[0]["msg_type"] == "nav_msgs/Odometry"


def test_discover_ros_topics_with_excludes():
    from lucid_component_ros_bridge.component import _discover_ros_topics

    fake_topics = [
        ("/odom", "nav_msgs/Odometry"),
        ("/rosout", "rosgraph_msgs/Log"),
        ("/clock", "rosgraph_msgs/Clock"),
    ]

    with patch.dict("sys.modules", {"rospy": MagicMock()}):
        import sys
        sys.modules["rospy"].get_published_topics.return_value = fake_topics

        subs, pubs = _discover_ros_topics(
            exclude_topics={"/rosout", "/clock"},
            explicit_sub_topics=set(),
            explicit_pub_topics=set(),
        )

    assert len(subs) == 1
    assert subs[0]["ros_topic"] == "/odom"


def test_discover_skips_explicit_topics():
    from lucid_component_ros_bridge.component import _discover_ros_topics

    fake_topics = [
        ("/odom", "nav_msgs/Odometry"),
        ("/cmd_vel", "geometry_msgs/Twist"),
    ]

    with patch.dict("sys.modules", {"rospy": MagicMock()}):
        import sys
        sys.modules["rospy"].get_published_topics.return_value = fake_topics

        subs, pubs = _discover_ros_topics(
            exclude_topics=set(),
            explicit_sub_topics={"/odom"},
            explicit_pub_topics={"/cmd_vel"},
        )

    # /odom already explicit sub → not discovered as sub
    assert len(subs) == 1
    assert subs[0]["ros_topic"] == "/cmd_vel"
    # /cmd_vel already explicit pub → not discovered as pub
    assert len(pubs) == 1
    assert pubs[0]["ros_topic"] == "/odom"


def test_auto_discover_config_from_yaml(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: true
        exclude_topics:
          - /rosout
          - /clock
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    assert comp._auto_discover is True
    assert comp._exclude_topics == {"/rosout", "/clock"}


def test_auto_discover_disabled(tmp_path: Path):
    cfg_file = _write_yaml(tmp_path, """\
        node_name: bot
        auto_discover: false
        ros_subscriptions: []
        ros_publishers: []
    """)
    ctx = _fake_context(config={"config_path": str(cfg_file)})
    comp = RosBridgeComponent(ctx)
    assert comp._auto_discover is False


# -- helper functions -----------------------------------------------------

def test_resolve_msg_type_ros1_slash_format_invalid():
    from lucid_component_ros_bridge.component import _resolve_msg_type
    with pytest.raises(ValueError, match="Invalid msg_type format"):
        _resolve_msg_type("a/b/c")


def test_resolve_msg_type_plain_string_invalid():
    from lucid_component_ros_bridge.component import _resolve_msg_type
    with pytest.raises(ValueError, match="Invalid msg_type format"):
        _resolve_msg_type("bad_format")


def test_msg_to_dict_fallback():
    """Test _msg_to_dict fallback when rospy_message_converter is not available."""
    from lucid_component_ros_bridge.component import _msg_to_dict

    class FakeMsg:
        __slots__ = ["x", "y"]
        def __init__(self) -> None:
            self.x = 1.0
            self.y = 2.0

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        result = _msg_to_dict(FakeMsg())
        assert result["x"] == 1.0
        assert result["y"] == 2.0


def test_dict_to_msg_fallback():
    from lucid_component_ros_bridge.component import _dict_to_msg

    class FakeMsg:
        def __init__(self) -> None:
            self.x = 0.0
            self.y = 0.0

    with patch.dict("sys.modules", {"rospy_message_converter": None}):
        msg = _dict_to_msg({"x": 3.0, "y": 4.0, "z": 5.0}, FakeMsg)
        assert msg.x == 3.0
        assert msg.y == 4.0
        assert not hasattr(msg, "z")


# -- __getattr__ routing --------------------------------------------------

def test_getattr_unknown_attribute_raises():
    ctx = _fake_context()
    comp = RosBridgeComponent(ctx)
    with pytest.raises(AttributeError):
        comp.nonexistent_method()
