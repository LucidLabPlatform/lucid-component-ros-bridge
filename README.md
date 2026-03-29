# lucid-component-ros-bridge

ROS 1 ↔ LUCID MQTT bridge component. Enables any ROS-enabled device to participate as a LUCID agent by translating ROS topics to LUCID telemetry streams and forwarding LUCID commands as ROS topic publishes.

## Overview

`lucid-component-ros-bridge` is a [LUCID component](https://github.com/LucidLabPlatform/lucid-component-base) that runs inside a `lucid-agent-core` process on a ROS-capable machine. It registers as `ros_bridge` in the LUCID component registry and participates fully in the LUCID MQTT contract (retained metadata/status/state/cfg, telemetry streams, command/result pairs).

**Two bridging modes:**

| Mode | Behaviour |
|---|---|
| `auto_discover: true` | Queries the ROS master at startup for all published topics and bridges them automatically. Explicit entries in `ros_subscriptions` / `ros_publishers` are merged in and take priority. |
| `auto_discover: false` | Only bridges the topics listed explicitly in `ros_subscriptions` and `ros_publishers`. |

## Requirements

- Python 3.11+
- ROS 1 (e.g. ROS Noetic) installed and workspace sourced:
  ```bash
  source /opt/ros/noetic/setup.bash
  source /path/to/your/ws/devel/setup.bash
  ```
- `rospy` must be importable in the Python environment used to run the agent. The component will raise `RuntimeError` at start if `rospy` is absent.
- (Optional) `rospy_message_converter` for richer message serialization — the component falls back gracefully to `__slots__` iteration if it is not installed.

## Install

```bash
pip install lucid-component-ros-bridge
# with richer ROS message serialization:
# pip install lucid-component-ros-bridge[message-converter]
# or in development (from the mono-workspace root):
make setup-venv && source .venv/bin/activate
```

## Configuration

Place a `ros_bridge.yaml` file in the agent's working directory, or set `config_path` in the component's LUCID config block. The package ships a default `ros_bridge.yaml` (all topics empty, `auto_discover: true`) as a starting point.

**Resolution order:**
1. `config_path` key in the component's LUCID config block (explicit override)
2. `./ros_bridge.yaml` in the agent's working directory
3. The default `ros_bridge.yaml` bundled with the package

### Full config reference

```yaml
node_name: lucid_ros_bridge        # ROS node name (default: lucid_ros_bridge)

# Auto-discovery: query ROS master for all published topics at startup.
# Explicit entries below are merged in (explicit takes priority for same topic).
auto_discover: true

# Skip these topics during auto-discovery (only used when auto_discover: true).
exclude_topics:
  - /rosout
  - /rosout_agg
  - /clock

# ROS → LUCID telemetry: subscribe to ROS topics and publish as telemetry.
ros_subscriptions:
  - ros_topic: /odom
    msg_type: nav_msgs/Odometry
    telemetry_metric: odom          # → telemetry/odom
  - ros_topic: /camera/rgb/image_raw
    msg_type: sensor_msgs/Image
    telemetry_metric: camera_rgb_image_raw

# LUCID cmd → ROS publish: a LUCID command that publishes to a ROS topic.
ros_publishers:
  - ros_topic: /cmd_vel
    msg_type: geometry_msgs/Twist
    command: cmd_vel                # ← cmd/cmd_vel
  - ros_topic: /arm/joint1
    msg_type: std_msgs/String
    command: arm_joint1             # ← cmd/arm_joint1
```

`msg_type` follows the standard ROS 1 convention: `"package/MessageType"` (e.g. `"geometry_msgs/Twist"`). The Python import style `"package.msg.MessageType"` is also accepted.

## MQTT Topic Mappings

All topics are rooted at `lucid/agents/{agent_id}/components/ros_bridge/`.

### Retained topics (QoS 1)

| Topic suffix | Content |
|---|---|
| `metadata` | `component_id`, `version`, `capabilities`, `node_name`, `ros_subscriptions`, `ros_publishers` |
| `status` | `{ "state": "idle" \| "running" \| "error" }` |
| `state` | `node_name`, `subscriptions_active`, `publishers_active`, `latest_values` |
| `cfg` | `node_name`, `auto_discover`, `exclude_topics`, `ros_subscriptions`, `ros_publishers` |
| `cfg/logging` | `{ "log_level": "ERROR" }` |
| `cfg/telemetry` | Per-metric telemetry gating config |

### Telemetry streams (QoS 0)

One topic per `ros_subscriptions` entry (or auto-discovered topic):

```
telemetry/{telemetry_metric}
```

Payload: `{ "value": { <ROS message fields as dict> } }`

Telemetry gating (enabled/interval/threshold) is configurable per metric via `cmd/cfg/telemetry/set`.

### Commands

| Command topic | Payload | Result topic |
|---|---|---|
| `cmd/reset` | `{"request_id": "..."}` | `evt/reset/result` |
| `cmd/ping` | `{"request_id": "..."}` | `evt/ping/result` |
| `cmd/cfg/set` | `{"request_id": "...", "set": {"node_name": "..."}}` | `evt/cfg/set/result` |
| `cmd/cfg/logging/set` | `{"request_id": "...", "set": {"log_level": "INFO"}}` | `evt/cfg/logging/set/result` |
| `cmd/cfg/telemetry/set` | `{"request_id": "...", "set": {<metric>: {enabled, interval_s, ...}}}` | `evt/cfg/telemetry/set/result` |
| `cmd/{command}` | `{"request_id": "...", "data": {<ROS message fields>}}` | `evt/{command}/result` |

The last row applies to every entry in `ros_publishers` — sending a LUCID command to `cmd/{command}` constructs a ROS message from `data` and publishes it to the configured ROS topic.

All command payloads require `request_id` (a UUID string). Every command publishes an `evt/*/result` with `{ request_id, ok, error }`.

## Component Contract

This component conforms to the LUCID Component Contract (`lucid-component-base`):

- **Entry point:** `ros_bridge` (registered in `pyproject.toml` under `[project.entry-points."lucid_components"]`)
- **component_id:** `ros_bridge`
- **Lifecycle:** `_start()` initialises the ROS node and creates subscriptions/publishers; `_stop()` signals shutdown and joins the spin thread. Both are idempotent.
- **State source of truth:** The LUCID agent (MQTT). Central Command holds only derived state.
- **Telemetry gating:** Each subscription metric gets a default config (`enabled: true`, `interval_s: 1`, `change_threshold_percent: 0.0`). Adjust via `cmd/cfg/telemetry/set`.

### Component context config keys

| Key | Type | Description |
|---|---|---|
| `config_path` | string | Absolute or relative path to a custom `ros_bridge.yaml` |

## State payload

```json
{
  "node_name": "lucid_ros_bridge",
  "subscriptions_active": 2,
  "publishers_active": 1,
  "latest_values": {
    "odom": { "pose": { ... }, "twist": { ... } },
    "battery": { "voltage": 12.1 }
  }
}
```

## Development

```bash
# Run unit tests (no ROS environment required — rospy is mocked)
cd lucid-component-ros-bridge
pytest tests/ -q

# Lint
ruff check .
```

Tests use `unittest.mock.patch` to replace `rospy` so the full CI/dev workflow runs without a ROS installation.

## Limitations / Known Issues

- ROS 1 only (`rospy`). ROS 2 (`rclpy`) is **not** currently supported despite the package description saying "ROS 2". See `ISSUES.md`.
- `auto_discover` creates both a subscriber and a publisher for every discovered topic, which may be undesirable. Use `exclude_topics` to filter.
- Binary message fields (e.g. raw image buffers) are included verbatim in the telemetry JSON, which can produce very large payloads. Exclude high-bandwidth topics using `exclude_topics` or explicit `ros_subscriptions` with lower-frequency metrics.
