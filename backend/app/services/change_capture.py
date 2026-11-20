"""CDC捕获业务规则：配置管理、增量捕获与下游主题联动。

增强：CDC启动时校验目标主题状态，捕获增量后触发合并策略检查。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store
from app.services.common import build_timeline

MODULE = "change_capture"
REQUIRED_FIELDS = ["配置编号", "源数据库", "源表名"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动捕获": "运行中", "停止捕获": "已停止"}
NEGATIVE_ACTIONS = []


class ChangeCaptureService:
    def get_timeline(self) -> list[dict[str, Any]]:
        return build_timeline(store.rows(MODULE), STATUS_ORDER)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验目标主题
        target_topic = values.get("目标主题")
        if target_topic:
            topic = self._find_topic(target_topic)
            if not topic:
                return None, [f"目标主题 {target_topic} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        for f in ["快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"CDC配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于CDC捕获可执行范围"
        target = ACTION_RULES[action]

        if action == "启动捕获":
            target_topic = entry.get("目标主题")
            topic = self._find_topic(target_topic) if target_topic else None
            if not topic:
                return None, f"目标主题 {target_topic} 未注册"
            if topic.get("status") != "已上线":
                return None, f"目标主题 {target_topic} 未上线"
            # 检查UPSERT策略
            source_table = entry.get("源表名")
            upsert = self._find_upsert(source_table)
            if not upsert:
                return None, f"源表 {source_table} 未配置增量合并策略"
            # 生成捕获产物
            self._generate_capture_artifact(entry, topic, upsert)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"CDC配置已{action}"

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_upsert(self, table_name: str) -> dict[str, Any] | None:
        for row in store.rows("upsert_merge"):
            if row.get("目标表名") == table_name:
                return row
        return None

    def _generate_capture_artifact(self, entry: dict[str, Any], topic: dict[str, Any], upsert: dict[str, Any]) -> None:
        artifact = {
            "cdc_id": entry.get("id"),
            "cdc_code": entry.get("配置编号"),
            "source_db": entry.get("源数据库"),
            "source_table": entry.get("源表名"),
            "target_topic": topic.get("主题编号"),
            "merge_policy": upsert.get("策略编号"),
            "merge_engine": upsert.get("合并引擎"),
            "captured_at": datetime.now().isoformat(),
            "stage": "streaming",
        }
        self._ensure_artifact_table("cdc_capture_artifacts")
        store.rows("cdc_capture_artifacts").append(artifact)
        entry["_last_capture"] = artifact

    def get_capture_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("cdc_capture_artifacts")
        return [a for a in store.rows("cdc_capture_artifacts") if a.get("cdc_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
