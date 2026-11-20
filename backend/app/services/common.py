"""业务服务共用实现：列表筛选分页与作业时间线的统一写法。

各业务模块此前各自复制了一份完全相同的筛选/分页与时间线生成逻辑，这里抽成
一份共用实现，只保留各模块不同的字段名与状态序列作为参数，行为与原先一致。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any


def filter_page(
    rows: list[dict[str, Any]],
    *,
    code_field: str,
    filters: dict[str, str] | None = None,
    keyword: str | None = None,
    status: str | None = None,
    page: int = 1,
    size: int = 20,
) -> tuple[list[dict[str, Any]], int]:
    """按页面字段、编号关键字与状态过滤，再按页切片。

    与各模块原先的实现保持一致：字段筛选是大小写不敏感的子串匹配，
    ``keyword`` 只匹配编号字段，状态为精确匹配。
    """
    result = list(rows)
    for field, expected in (filters or {}).items():
        result = [
            row for row in result
            if expected.casefold() in str(row.get(field, "")).casefold()
        ]
    if keyword:
        result = [
            row for row in result
            if keyword.casefold() in str(row.get(code_field, "")).casefold()
        ]
    if status:
        result = [row for row in result if row.get("status") == status]
    total = len(result)
    start = (page - 1) * size
    return result[start:start + size], total


def build_timeline(rows: list[dict[str, Any]], status_order: list[str]) -> list[dict[str, Any]]:
    """把作业记录展开成甘特图使用的时间线结构。

    起止日期和日志文案沿用各时间线模块原本的口径，不改变页面展示。
    """
    today = date.today()
    items: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        start = (today - timedelta(days=7 - i * 2)).isoformat()
        end = (today + timedelta(days=3 - i)).isoformat()
        items.append({
            "id": int(row.get("id", i + 1)),
            "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
            "start": start,
            "end": end,
            "status": str(row.get("status", status_order[i % len(status_order)])),
            "logs": (
                f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] "
                f"任务{'完成' if row.get('status') == status_order[-1] else '进行中'}"
            ),
        })
    return items
