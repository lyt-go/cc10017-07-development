"""任务状态迁移数据计划：流式计算与Schema演进之间的交接对象。

订单聚合任务准备迁移时，只有当前检查点路径并不足以说明输出表版本是否兼容；
一旦中断，也无法从计划中判断该从哪个检查点继续。本模块把两部分信息一起写入
迁移数据计划：

* 「检查点版本」与「可恢复输出表」必须同时写入计划；
* 同一恢复窗口内新检查点与输出表历史版本相遇时，按固定口径只声明一个可恢复点；
* 中断后重新生成计划只续做未完成批次，不会产生第二个可恢复点；
* Schema演进模块读取计划，核对检查点字段版本与主题 Schema 版本。

计划只描述恢复口径，不改变流式计算模块自身的状态流转。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

PLAN_TABLE = "task_migration_plans"
CHECKPOINT_TABLE = "flink_checkpoints"

# 计划状态
PLAN_READY = "可恢复"
PLAN_RESUMED = "已续做"
PLAN_CONFLICT = "存在冲突"
PLAN_COMPLETED = "已完成"

# 冲突收敛口径：以新检查点为准
RESOLUTION_KEEP_CHECKPOINT = "以新检查点为准"


class TaskMigrationPlanService:
    """生成、续做并核对任务状态迁移数据计划。"""

    def _plans(self) -> list[dict[str, Any]]:
        return store.rows(PLAN_TABLE)

    def _checkpoints(self) -> list[dict[str, Any]]:
        return store.rows(CHECKPOINT_TABLE)

    # ───────────────────────────────
    # 读取订单聚合任务数据配置
    # ───────────────────────────────

    def resolve_job_source(self, job_id: int) -> dict[str, Any] | None:
        """读取任务数据配置：流式任务、消费主题、Schema演进规则与输出表元数据。"""
        job = store.find("flink_job", job_id)
        if job is None:
            return None

        topic_code = str(job.get("消费主题") or "")
        topic = self._find("topic_registry", "主题编号", topic_code)
        schema_rows = [
            row for row in store.rows("schema_evolve")
            if row.get("关联主题") == topic_code
        ]
        output_table = str(job.get("输出表名") or "")
        iceberg = self._find("iceberg_table", "表名称", output_table) if output_table else None
        checkpoints = [
            row for row in self._checkpoints()
            if int(row.get("job_id", 0) or 0) == job_id
        ]

        return {
            "job": job,
            "topic": topic,
            "schema": self._pick_schema(schema_rows),
            "schema_rules": schema_rows,
            "output_table": output_table,
            "iceberg": iceberg,
            "checkpoints": checkpoints,
            "checkpoint": self._latest_checkpoint(checkpoints),
        }

    # ───────────────────────────────
    # 生成迁移数据计划
    # ───────────────────────────────

    def generate_plan(
        self,
        job_id: int,
        *,
        resume: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        """生成任务状态迁移数据计划；resume=True 表示中断后续做未完成批次。"""
        source = self.resolve_job_source(job_id)
        if source is None:
            return None, f"流式任务 {job_id} 不存在或已归档"

        job = source["job"]
        checkpoint = source["checkpoint"]
        output_table = source["output_table"]
        schema = source["schema"]

        if checkpoint is None:
            return None, f"任务 {job.get('任务编号')} 缺少检查点记录，无法生成迁移数据计划"
        if not output_table:
            return None, f"任务 {job.get('任务编号')} 未配置输出表，无法生成迁移数据计划"

        previous = self._find_plan(job_id)
        checkpoint_version = str(checkpoint.get("checkpoint_version", ""))
        checkpoint_output_version = str(checkpoint.get("output_table_version", ""))
        output_version = self._current_output_version(source)
        history_versions = self._history_output_versions(source)
        schema_version = str(schema.get("生效版本", "")) if schema else ""

        conflict = self._detect_conflict(
            checkpoint_version=checkpoint_version,
            checkpoint_output_version=checkpoint_output_version,
            output_version=output_version,
            history_versions=history_versions,
        )
        batches = self._build_batches(checkpoint, previous, resume=resume)

        plan: dict[str, Any] = {
            "plan_code": f"PLAN-{job.get('任务编号') or job_id}",
            "job_id": job_id,
            "job_code": job.get("任务编号"),
            "topic_code": source["topic"].get("主题编号") if source["topic"] else job.get("消费主题"),
            "output_table": output_table,
            # 检查点版本与可恢复输出表一起写入计划
            "checkpoint_version": checkpoint_version,
            "checkpoint_path": str(checkpoint.get("checkpoint_path", "")),
            "checkpoint_offset": str(checkpoint.get("checkpoint_offset", "")),
            "checkpoint_schema_version": str(
                checkpoint.get("task_schema_version") or checkpoint_version
            ),
            "schema_version": schema_version,
            "output_table_version": output_version,
            "checkpoint_output_version": checkpoint_output_version,
            "recoverable_outputs": self._recoverable_outputs(
                output_table, output_version, history_versions
            ),
            "batches": batches,
            "conflict": conflict,
            "generated_at": datetime.now().isoformat(),
        }
        plan["recoverable_point"] = self._recoverable_point(plan, checkpoint_output_version, conflict)
        plan["status"] = self._plan_status(plan, conflict, batches, resumed=resume)
        plan["message"] = self._plan_message(plan, conflict)

        self._upsert_plan(plan)
        return dict(plan), plan["message"]

    # ───────────────────────────────
    # Schema演进核对检查点字段版本
    # ───────────────────────────────

    def check_schema_compatibility(self, plan_id: int) -> dict[str, Any] | None:
        """Schema演进读取迁移数据计划，核对检查点字段版本。

        核对对象是「计划里记录的检查点字段版本」与「主题当前生效的 Schema 版本」，
        因此主题 Schema 继续演进后，旧的迁移计划会被识别为不再兼容。
        """
        plan = store.find(PLAN_TABLE, plan_id)
        if plan is None:
            return None

        topic_code = str(plan.get("topic_code") or "")
        schema_rows = [
            row for row in store.rows("schema_evolve")
            if row.get("关联主题") == topic_code
        ]
        schema = self._pick_schema(schema_rows)
        current_version = str(schema.get("生效版本", "")) if schema else ""
        checkpoint_version = str(plan.get("checkpoint_schema_version", ""))

        compatible = bool(current_version) and current_version == checkpoint_version
        return {
            "plan_id": plan.get("id"),
            "plan_code": plan.get("plan_code"),
            "topic_code": topic_code,
            "checkpoint_version": checkpoint_version,
            "schema_version": current_version,
            "compatible": compatible,
            "schema_rule_id": schema.get("id") if schema else None,
            "message": (
                "检查点字段版本与主题 Schema 版本一致，可继续迁移"
                if compatible
                else f"检查点字段版本 {checkpoint_version or '缺失'} 与主题 Schema 版本 "
                     f"{current_version or '缺失'} 不一致"
            ),
        }

    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def list_plans(self) -> list[dict[str, Any]]:
        return [dict(plan) for plan in self._plans()]

    def get_plan(self, plan_id: int) -> dict[str, Any] | None:
        plan = store.find(PLAN_TABLE, plan_id)
        return dict(plan) if plan is not None else None

    def get_plan_by_job(self, job_id: int) -> dict[str, Any] | None:
        plan = self._find_plan(job_id)
        return dict(plan) if plan is not None else None

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _find(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        for row in store.rows(module):
            if str(row.get(field, "")) == str(value):
                return row
        return None

    def _pick_schema(self, schema_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """主题当前生效的演进规则；没有生效规则时退回最后登记的一条。"""
        for row in schema_rows:
            if row.get("status") == "已生效":
                return row
        return schema_rows[-1] if schema_rows else None

    def _latest_checkpoint(self, checkpoints: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not checkpoints:
            return None
        return max(checkpoints, key=lambda row: int(row.get("checkpoint_version", 0) or 0))

    def _current_output_version(self, source: dict[str, Any]) -> str:
        """输出表当前版本：优先取 Iceberg 元数据，其次取检查点记录的当前版本。"""
        iceberg = source.get("iceberg")
        if iceberg is not None and iceberg.get("元数据版本"):
            return str(iceberg.get("元数据版本"))
        checkpoint = source.get("checkpoint") or {}
        return str(checkpoint.get("current_output_table_version", ""))

    def _history_output_versions(self, source: dict[str, Any]) -> list[str]:
        """输出表仍可恢复的历史版本（Iceberg 历史元数据或检查点记录的历史版本）。"""
        versions: list[str] = []
        iceberg = source.get("iceberg")
        if iceberg is not None:
            for version in iceberg.get("历史元数据版本", []) or []:
                text = str(version)
                if text and text not in versions:
                    versions.append(text)
        checkpoint = source.get("checkpoint") or {}
        for version in checkpoint.get("output_table_history", []) or []:
            text = str(version)
            if text and text not in versions:
                versions.append(text)
        return versions

    def _detect_conflict(
        self,
        *,
        checkpoint_version: str,
        checkpoint_output_version: str,
        output_version: str,
        history_versions: list[str],
    ) -> dict[str, Any]:
        """判断新检查点与输出表历史版本是否在同一恢复窗口相遇。

        检查点写在旧的输出表结构上，而输出表已经切换到新版本、旧版本仍可恢复时，
        「先采用新检查点」和「先恢复历史输出表」会形成不同的重放范围。
        """
        detected = bool(
            checkpoint_output_version
            and output_version
            and checkpoint_output_version != output_version
            and checkpoint_output_version in history_versions
        )
        return {
            "detected": detected,
            "resolution": RESOLUTION_KEEP_CHECKPOINT if detected else "",
            "checkpoint_version": checkpoint_version,
            "checkpoint_output_version": checkpoint_output_version,
            "output_version": output_version,
            "history_versions": history_versions,
            "replay_scope": (
                f"从检查点 v{checkpoint_version} 的位点开始，"
                f"输出表按历史版本 {checkpoint_output_version} 对齐后重放"
                if detected else ""
            ),
        }

    def _build_batches(
        self,
        checkpoint: dict[str, Any],
        previous: dict[str, Any] | None,
        *,
        resume: bool,
    ) -> list[dict[str, Any]]:
        """按检查点分批；中断续做时沿用已完成批次，只续做未完成批次。"""
        total = int(checkpoint.get("batch_total", 0) or 0)
        if total <= 0:
            total = 1
        completed = int(checkpoint.get("batch_completed", 0) or 0)

        done: set[int] = set(range(1, completed + 1))
        if resume and previous is not None:
            done |= {
                int(batch.get("batch_no", 0))
                for batch in previous.get("batches", [])
                if batch.get("state") == "已完成"
            }

        return [
            {
                "batch_no": batch_no,
                "state": "已完成" if batch_no in done else "待续做",
            }
            for batch_no in range(1, total + 1)
        ]

    def _recoverable_outputs(
        self,
        output_table: str,
        output_version: str,
        history_versions: list[str],
    ) -> list[dict[str, Any]]:
        """可恢复输出表清单：当前版本在前，历史版本随后，版本不重复。"""
        versions: list[str] = []
        for version in [output_version, *history_versions]:
            if version and version not in versions:
                versions.append(version)
        if not versions:
            versions = [""]
        return [
            {"table": output_table, "version": version, "recoverable": True}
            for version in versions
        ]

    def _recoverable_point(
        self,
        plan: dict[str, Any],
        checkpoint_output_version: str,
        conflict: dict[str, Any],
    ) -> dict[str, Any]:
        """计划里只声明一个可恢复点：检查点版本与与之匹配的输出表版本。"""
        if conflict["detected"]:
            # 冲突时固定以新检查点为准，输出表取检查点写入时的历史版本
            output_version = checkpoint_output_version
        else:
            output_version = plan["output_table_version"]
        return {
            "checkpoint_version": plan["checkpoint_version"],
            "checkpoint_path": plan["checkpoint_path"],
            "checkpoint_offset": plan["checkpoint_offset"],
            "output_table": plan["output_table"],
            "output_table_version": output_version,
            "resolution": conflict["resolution"],
            "replay_scope": conflict["replay_scope"],
        }

    def _plan_status(
        self,
        plan: dict[str, Any],
        conflict: dict[str, Any],
        batches: list[dict[str, Any]],
        *,
        resumed: bool,
    ) -> str:
        if all(batch["state"] == "已完成" for batch in batches):
            return PLAN_COMPLETED
        if conflict["detected"]:
            return PLAN_CONFLICT
        if resumed:
            return PLAN_RESUMED
        return PLAN_READY

    def _plan_message(self, plan: dict[str, Any], conflict: dict[str, Any]) -> str:
        if conflict["detected"]:
            return (
                f"新检查点 v{plan['checkpoint_version']} 与输出表历史版本 "
                f"{plan['checkpoint_output_version']} 在同一恢复窗口相遇，"
                f"计划按「{RESOLUTION_KEEP_CHECKPOINT}」只保留一个可恢复点"
            )
        if plan["status"] == PLAN_COMPLETED:
            return "全部批次已完成，迁移数据计划无需续做"
        return "迁移数据计划已生成，检查点版本与可恢复输出表已一并记录"

    def _find_plan(self, job_id: int) -> dict[str, Any] | None:
        for plan in self._plans():
            if int(plan.get("job_id", 0) or 0) == job_id:
                return plan
        return None

    def _upsert_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        """同一任务只保留一份计划，重新生成即覆盖，避免出现第二个可恢复点。"""
        existing = self._find_plan(int(plan["job_id"]))
        if existing is not None:
            plan["id"] = existing.get("id")
            existing.clear()
            existing.update(plan)
            return existing
        rows = self._plans()
        plan["id"] = max((int(row.get("id", 0) or 0) for row in rows), default=0) + 1
        rows.append(plan)
        return plan
