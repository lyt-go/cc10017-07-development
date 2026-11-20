"""流式补数 时间线服务：生成作业时间序列和示例数据。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.common import build_timeline

MODULE = "backfill_stream"
REQUIRED_FIELDS = ["任务编号", "目标表名", "起始偏移"]
STATUS_ORDER = ["草稿", "运行中", "已完成", "已取消"]
ACTION_RULES = {"启动补数": "运行中", "确认完成": "已完成", "取消补数": "已取消"}
NEGATIVE_ACTIONS = []


class BackfillStreamService:
    def get_timeline(self) -> list[dict[str, Any]]:
        return build_timeline(store.rows(MODULE), STATUS_ORDER)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"补数任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式补数可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"补数任务已{action}"
