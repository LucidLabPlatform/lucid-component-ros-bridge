# ISSUES — lucid-component-ros-bridge

Catalog of issues found during audit. **No fixes applied — documentation only.**

---

## CRIT-001 — No git repository initialised

**Severity:** Critical
**File:** package root
The `lucid-component-ros-bridge` directory has no `.git` folder. Every other package in the LUCID mono-workspace has its own git repo per the design. This means the package has no version history, no ability to tag releases, and the `./release.sh` script will silently skip it. The `pyproject.toml` uses `setuptools_scm` which relies on git tags — without a repo the package always falls back to version `0.0.0`.

---

## CRIT-002 — README.md was missing from the package root

**Severity:** Critical
**File:** package root
`pyproject.toml` declares `readme = "README.md"` but no such file existed. This causes `pip install` (in sdist/wheel build mode) to fail. Documentation content existed only at `docs/lucid-component-ros-bridge/README.md` in the workspace docs mirror — not co-located with the package.
**Status:** Fixed in this session (README.md created).

---

## WARN-001 — ROS version mismatch: code is ROS 1, description says ROS 2

**Severity:** Warning
**Files:** `pyproject.toml` (line 8), `component.py` (module docstring, line 1), `docs/lucid-component-ros-bridge/README.md`
`pyproject.toml` describes the package as "ROS 2 ↔ LUCID MQTT bridge", but all implementation code imports `rospy` (ROS 1 library) and uses ROS 1 message conventions (`package/MessageType`). There is no `rclpy` usage anywhere. The workspace `CLAUDE.md` also refers to this package as "ROS 1 ↔ LUCID MQTT bridge" in the architecture table, and "ROS 2" in the package map. The discrepancy risks confusion for installers and Notion documentation.

---

## WARN-002 — `auto_discover` creates publishers for every discovered ROS topic by default

**Severity:** Warning
**File:** `component.py`, `_discover_ros_topics()` (lines 101–147)
When `auto_discover: true`, the bridge registers a LUCID command (`cmd/{topic}`) for every topic it finds on the ROS master, not just topics the operator intends to publish to. On a busy ROS graph this creates a very large number of commands, most of which will never be used, and causes the `capabilities` array in the MQTT `metadata` topic to be excessively large. There is no opt-out mechanism separate from `exclude_topics`.

---

## WARN-003 — Binary / high-bandwidth topics produce oversized MQTT payloads

**Severity:** Warning
**File:** `component.py`, `_msg_to_dict()` (lines 180–206) and `_setup_ros_subscriptions()` (lines 379–409)
Raw binary fields (e.g. `sensor_msgs/Image` data arrays, `sensor_msgs/PointCloud2` data) are serialised verbatim into JSON and published as LUCID telemetry. This can produce MQTT payloads in the megabyte range, which may exceed EMQX broker limits (default max payload: 1 MB) and cause publish failures with no error propagation back to the user. No size guard or truncation is implemented.

---

## WARN-004 — `on_cmd_cfg_set` only supports updating `node_name`

**Severity:** Warning
**File:** `component.py`, `on_cmd_cfg_set()` (lines 479–513)
The `cmd/cfg/set` handler only modifies `node_name`. The `get_cfg_payload()` exposes several other mutable fields (`auto_discover`, `exclude_topics`, `ros_subscriptions`, `ros_publishers`) but changes to these are not supported at runtime without restarting the component. The handler silently ignores unknown keys in `set_dict` without reporting them as applied or rejected, which violates the principle of observable state.

---

## WARN-005 — ROS node is not re-initialisable after stop

**Severity:** Warning
**File:** `component.py`, `_stop()` / `_start()` (lines 310–375)
`rospy.init_node()` can only be called once per Python process. If the component is stopped and then started again (e.g. via `cmd/reset` at the agent level), the second call to `rospy.init_node()` will raise a `rospy.exceptions.ROSException`. The `_start()` method does not guard against this, so a restart will leave the component in `FAILED` status. A process restart is required to reinitialise the ROS bridge.

---

## INFO-001 — Missing `rospy_message_converter` listed as optional but undocumented

**Severity:** Info
**File:** `pyproject.toml`, `component.py` (lines 187–189, 212–216)
`rospy_message_converter` is silently used when available (for richer ROS ↔ dict conversion) but is not listed in `[project.optional-dependencies]` and is not mentioned in the README. Users who want the richer serialization have no guidance on how to install it.

---

## INFO-002 — No `pytest-cov` or coverage measurement configured

**Severity:** Info
**File:** `pyproject.toml`
The `[project.optional-dependencies].dev` section only lists `pytest`. There is no `pytest-cov` dependency and no coverage threshold enforced in `pyproject.toml` or a `setup.cfg`/`.coveragerc`. This makes it easy for coverage to regress unnoticed.

---

## INFO-003 — `_spin_loop` polling at 10 Hz introduces up to 100 ms latency on stop

**Severity:** Info
**File:** `component.py`, `_spin_loop()` (lines 435–444)
The spin loop polls at 10 Hz using `rospy.Rate(10).sleep()`. When `_stop()` is called, the thread can take up to 100 ms to wake up and exit. The `_stop()` method then waits up to 5 seconds for the join. This is acceptable but worth noting if sub-100 ms shutdown is required.

---

## INFO-004 — `tests/` directory has no `conftest.py`

**Severity:** Info
**File:** `tests/`
There is no `conftest.py` with shared fixtures. The `_fake_context()` factory and `_write_yaml()` helper are duplicated inline in the single test file. As more test files are added, these should be extracted into a `conftest.py`.
