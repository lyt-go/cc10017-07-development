"""任务状态迁移数据计划接口：流式计算与Schema演进之间的交接对象。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult
from app.services.task_migration_plan import TaskMigrationPlanService

router = APIRouter(prefix="/api/task_migration_plan", tags=["任务状态迁移"])
service = TaskMigrationPlanService()


@router.get("")
def list_plans() -> dict[str, Any]:
    """列出已有的任务状态迁移数据计划。"""
    items = service.list_plans()
    return {"items": items, "total": len(items)}


@router.post("/job/{job_id}/generate")
def generate_plan(job_id: int, resume: bool = Query(default=False, description="中断后只续做未完成批次")) -> ActionResult:
    """为订单聚合任务生成迁移数据计划；重新生成会覆盖旧计划，不产生第二个可恢复点。"""
    plan, message = service.generate_plan(job_id, resume=resume)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)


@router.get("/job/{job_id}")
def get_plan_by_job(job_id: int) -> dict[str, Any]:
    """按流式任务读取迁移数据计划，供流式计算页展示当前恢复口径。

    任务尚未生成计划时返回 ``exists=false`` 而不是 404，页面照常渲染即可。
    """
    plan = service.get_plan_by_job(job_id)
    return {"exists": plan is not None, "plan": plan}


@router.get("/{plan_id}")
def get_plan(plan_id: int) -> dict:
    """读取单份迁移数据计划；计划里同时保留检查点版本与可恢复输出表。"""
    plan = service.get_plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"迁移数据计划 {plan_id} 不存在")
    return plan


@router.get("/{plan_id}/schema-check")
def check_schema(plan_id: int) -> dict:
    """Schema演进读取迁移数据计划并核对检查点字段版本。"""
    result = service.check_schema_compatibility(plan_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"迁移数据计划 {plan_id} 不存在")
    return result
