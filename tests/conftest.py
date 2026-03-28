from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

from lucid_component_base import ComponentContext


class FakeMqtt:
    def __init__(self) -> None:
        self.published: list[tuple] = []

    def publish(self, topic: str, payload: Any, *, qos: int = 0, retain: bool = False) -> None:
        self.published.append((topic, payload, qos, retain))


def make_context(config: dict | None = None) -> ComponentContext:
    return ComponentContext.create(
        agent_id="test-agent",
        base_topic="lucid/agents/test-agent",
        component_id="ros_bridge",
        mqtt=FakeMqtt(),
        config=config or {},
    )


def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "ros_bridge.yaml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p
