"""任务状态迁移数据计划测试：检查点版本与可恢复输出表必须一起写入计划。

覆盖：
1. 流式计算页生成迁移计划，计划保留检查点版本与可恢复输出表；
2. Schema演进页读取计划核对检查点字段版本；
3. 新检查点与输出表历史版本在同一恢复窗口相遇时只声明一个可恢复点；
4. 中断后重新生成只续做未完成批次，不出现第二个可恢复点；
5. 共用列表/时间线实现与原先口径一致。
"""
from __future__ import annotations

import unittest
from typing import Any

from app.pipeline_engine import pipeline_engine
from app.services.common import build_timeline, filter_page
from app.services.flink_job import FlinkJobService
from app.services.schema_evolve import SchemaEvolveService
from app.services.task_migration_plan import (
    PLAN_COMPLETED,
    PLAN_CONFLICT,
    PLAN_RESUMED,
    TaskMigrationPlanService,
)
from app.store import store


class MigrationTestBase(unittest.TestCase):
    """每个用例前后恢复检查点与计划数据，避免用例间互相影响。"""

    def setUp(self) -> None:
        self.service = TaskMigrationPlanService()
        self._checkpoints = [dict(row) for row in store.rows("flink_checkpoints")]
        self._plans = [dict(row) for row in store.rows("task_migration_plans")]

    def tearDown(self) -> None:
        for name, snapshot in (
            ("flink_checkpoints", self._checkpoints),
            ("task_migration_plans", self._plans),
        ):
            rows = store.rows(name)
            rows.clear()
            rows.extend(dict(row) for row in snapshot)


class TestMigrationPlanCreation(MigrationTestBase):
    """生成迁移数据计划：检查点版本与可恢复输出表一起写入。"""

    def test_plan_keeps_checkpoint_version_and_recoverable_outputs(self) -> None:
        plan, message = self.service.generate_plan(1)
        self.assertIsNotNone(plan)
        self.assertIn("检查点", message)

        # 检查点版本与可恢复输出表必须同时出现在计划里
        self.assertEqual(plan["checkpoint_version"], "3")
        self.assertTrue(plan["checkpoint_path"])
        self.assertEqual(plan["output_table"], "order_agg_hourly")
        self.assertTrue(plan["recoverable_outputs"])
        versions = [item["version"] for item in plan["recoverable_outputs"]]
        self.assertIn("v2", versions)
        self.assertEqual(len(versions), len(set(versions)), "可恢复输出表版本不应重复")

        # 只有一个可恢复点，且带上检查点版本与匹配的输出表版本
        point = plan["recoverable_point"]
        self.assertEqual(point["checkpoint_version"], plan["checkpoint_version"])
        self.assertEqual(point["output_table"], plan["output_table"])
        self.assertEqual(point["output_table_version"], "v2")

    def test_plan_requires_checkpoint(self) -> None:
        job, _ = FlinkJobService().create_entry({
            "任务编号": "FLIN-NOCHK", "消费主题": "TOPI-0001", "转换逻辑": "无检查点任务",
            "输出表名": "order_agg_hourly",
        })
        plan, message = self.service.generate_plan(job["id"])
        self.assertIsNone(plan)
        self.assertIn("检查点", message)

    def test_plan_for_missing_job(self) -> None:
        plan, message = self.service.generate_plan(99999)
        self.assertIsNone(plan)
        self.assertIn("不存在", message)


class TestSchemaCompatibility(MigrationTestBase):
    """Schema演进页读取计划并核对检查点字段版本。"""

    def test_schema_check_matches_topic_schema(self) -> None:
        plan, _ = self.service.generate_plan(2)
        check = self.service.check_schema_compatibility(plan["id"])
        self.assertIsNotNone(check)
        self.assertEqual(check["topic_code"], "TOPI-0002")
        self.assertEqual(check["checkpoint_version"], "v2")
        self.assertEqual(check["schema_version"], "v2")
        self.assertTrue(check["compatible"])

    def test_schema_check_reports_mismatch(self) -> None:
        plan, _ = self.service.generate_plan(1)
        # 模拟检查点写在旧 Schema 结构上
        store.find("task_migration_plans", plan["id"])["checkpoint_schema_version"] = "v2"
        check = self.service.check_schema_compatibility(plan["id"])
        self.assertFalse(check["compatible"])
        self.assertIn("不一致", check["message"])

    def test_schema_check_for_missing_plan(self) -> None:
        self.assertIsNone(self.service.check_schema_compatibility(99999))

    def test_schema_router_lists_checks(self) -> None:
        self.service.generate_plan(2)
        from app.routers.schema_evolve import list_migration_checks

        checks = list_migration_checks()
        self.assertTrue(any(check["plan_code"] == "PLAN-FLIN-0002" for check in checks))


class TestMigrationConflict(MigrationTestBase):
    """新检查点与输出表历史版本在同一恢复窗口相遇的处理。"""

    def test_conflict_declares_single_recoverable_point(self) -> None:
        plan, message = self.service.generate_plan(1)
        self.assertEqual(plan["status"], PLAN_CONFLICT)
        self.assertTrue(plan["conflict"]["detected"])
        self.assertIn("以新检查点为准", message)

        # 冲突时只有一个可恢复点，检查点与输出表版本成对锁定
        point = plan["recoverable_point"]
        self.assertEqual(point["checkpoint_version"], "3")
        self.assertEqual(point["output_table_version"], "v2")
        self.assertEqual(point["resolution"], "以新检查点为准")
        self.assertTrue(point["replay_scope"])

    def test_no_conflict_when_versions_align(self) -> None:
        plan, _ = self.service.generate_plan(3)
        self.assertFalse(plan["conflict"]["detected"])
        self.assertEqual(plan["status"], PLAN_COMPLETED)
        self.assertEqual(plan["recoverable_point"]["output_table_version"], plan["output_table_version"])


class TestMigrationResume(MigrationTestBase):
    """中断后重新生成：只续做未完成批次，不出现第二个可恢复点。"""

    def test_resume_keeps_completed_batches(self) -> None:
        first, _ = self.service.generate_plan(1)
        completed_before = {
            batch["batch_no"] for batch in first["batches"] if batch["state"] == "已完成"
        }
        self.assertTrue(completed_before)

        resumed, message = self.service.generate_plan(1, resume=True)
        self.assertEqual(resumed["id"], first["id"], "重新生成应覆盖原计划")
        self.assertIn("只保留一个可恢复点", message)
        completed_after = {
            batch["batch_no"] for batch in resumed["batches"] if batch["state"] == "已完成"
        }
        self.assertTrue(completed_before <= completed_after)
        self.assertTrue(any(batch["state"] == "待续做" for batch in resumed["batches"]))

    def test_resume_without_conflict_is_marked_resumed(self) -> None:
        first, _ = self.service.generate_plan(2)
        self.assertTrue(any(batch["state"] == "待续做" for batch in first["batches"]))
        resumed, _ = self.service.generate_plan(2, resume=True)
        self.assertEqual(resumed["status"], PLAN_RESUMED)
        self.assertFalse(resumed["conflict"]["detected"])

    def test_regenerate_does_not_create_second_plan(self) -> None:
        self.service.generate_plan(1)
        self.service.generate_plan(1, resume=True)
        self.service.generate_plan(1, resume=True)
        plans = [plan for plan in self.service.list_plans() if plan["job_id"] == 1]
        self.assertEqual(len(plans), 1)
        self.assertEqual(len(self.service.list_plans()), 1)

    def test_resume_only_unfinished_batches(self) -> None:
        """已完成批次不再回到待续做，未完成批次保持待续做。"""
        first, _ = self.service.generate_plan(1)
        checkpoint = store.find("flink_checkpoints", 1)
        original_completed = checkpoint["batch_completed"]
        try:
            # 模拟中断：检查点只完成 2 个批次，但上一版计划已推进到 4 个
            checkpoint["batch_completed"] = 2
            resumed, _ = self.service.generate_plan(1, resume=True)
            states = {batch["batch_no"]: batch["state"] for batch in resumed["batches"]}
            self.assertEqual(states[3], "已完成")
            self.assertEqual(states[4], "已完成")
            self.assertEqual(states[5], "待续做")
            self.assertEqual(states[6], "待续做")
        finally:
            checkpoint["batch_completed"] = original_completed

    def test_resume_without_new_checkpoint_keeps_plan_identity(self) -> None:
        first, _ = self.service.generate_plan(1)
        resumed, _ = self.service.generate_plan(1, resume=True)
        self.assertEqual(first["plan_code"], resumed["plan_code"])
        self.assertEqual(first["id"], resumed["id"])


class TestMigrationChain(MigrationTestBase):
    """跨模块链路：以迁移数据计划为交接对象连接流式计算与Schema演进。"""

    def test_chain_status_picks_up_existing_plan(self) -> None:
        before = pipeline_engine.migration_chain_status(1)
        self.assertTrue(before["checkpoint"]["exists"])
        self.assertFalse(before["migration_plan"]["exists"])
        self.assertFalse(before["chain_ready"])

        self.service.generate_plan(1)
        after = pipeline_engine.migration_chain_status(1)
        self.assertTrue(after["migration_plan"]["exists"])
        self.assertTrue(after["chain_ready"])
        self.assertEqual(after["migration_plan"]["entry"]["checkpoint_version"], "3")
        self.assertEqual(after["schema_evolve"]["entry"]["关联主题"], "TOPI-0001")

    def test_chain_status_for_missing_job(self) -> None:
        status = pipeline_engine.migration_chain_status(99999)
        self.assertFalse(status["exists"])


class TestSharedHelpers(unittest.TestCase):
    """共用列表/时间线实现与各模块原先口径一致。"""

    def test_filter_page_matches_module_filters(self) -> None:
        service = SchemaEvolveService()
        rows, total = service.list_entries(filters={"关联主题": "TOPI-0001"})
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["规则编号"], "SCHE-0001")

        rows, total = service.list_entries(keyword="sche-0003")
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["关联主题"], "TOPI-0003")

        rows, total = service.list_entries(status="已生效")
        self.assertEqual(total, 2)

    def test_filter_page_pagination(self) -> None:
        rows = [{"id": i, "编号": f"C-{i}", "status": "草稿"} for i in range(5)]
        page, total = filter_page(rows, code_field="编号", page=2, size=2)
        self.assertEqual(total, 5)
        self.assertEqual([row["id"] for row in page], [2, 3])

    def test_timeline_structure_preserved(self) -> None:
        items = FlinkJobService().get_timeline()
        self.assertEqual(len(items), len(store.rows("flink_job")))
        first = items[0]
        for key in ("id", "name", "start", "end", "status", "logs"):
            self.assertIn(key, first)
        self.assertIn("作业启动", first["logs"])

    def test_timeline_status_fallback(self) -> None:
        items = build_timeline([{"id": 1, "name": "无状态作业"}], ["草稿", "运行中", "已停止"])
        self.assertEqual(items[0]["status"], "草稿")
        self.assertIn("进行中", items[0]["logs"])


if __name__ == "__main__":
    unittest.main()
