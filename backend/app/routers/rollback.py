"""删列回滚演练 API：传递影响评估 → 回滚依赖校验 → 演练输入 → 演练/恢复。

路由只做参数解析和 HTTP 状态映射，业务判断全部在领域服务/引擎内完成，
与项目「状态流转只允许在 services 里改」的约定保持一致。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services.config_migration import ConfigMigrationService, rollback_engine
from app.services.schema_evolution import SchemaEvolutionService

router = APIRouter(prefix="/api/rollback", tags=["删列回滚演练"])

_migration_service = ConfigMigrationService()
_schema_service = SchemaEvolutionService()


def _require_result(result: dict[str, Any]) -> dict[str, Any]:
    if isinstance(result, dict) and result.get("error"):
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.post("/impact/evaluate")
def evaluate_drop_impact(body: dict[str, Any]) -> dict[str, Any]:
    """评估删列的传递影响。

    body: {"source_table", "dropped_column"}
    沿血缘实际消费方向 BFS，返回全部传递消费者（共享目标只一次）及路径版本。
    """
    source_table = body.get("source_table", "").strip()
    column = body.get("dropped_column", "").strip()
    if not source_table or not column:
        raise HTTPException(status_code=400, detail="source_table 和 dropped_column 必填")

    impact = rollback_engine.evaluate_drop_impact(source_table, column)
    # 同步产出 Schema 领域的字段影响详情（修复一跳漏算后的口径）
    field_impact = _schema_service.analyze_field_impact(source_table, column)
    return {
        "impact": impact,
        "field_impact_detail": field_impact,
        "consistent": impact["affected_table_count"]
        == field_impact["affected_table_count"],
    }


@router.get("/impact/{source_table}/{dropped_column}")
def get_drop_impact(source_table: str, dropped_column: str) -> dict[str, Any]:
    """读取最近一轮删列影响评估（含路径版本）。"""
    impact = rollback_engine.get_impact(source_table, dropped_column)
    if impact is None:
        raise HTTPException(status_code=404, detail="尚未对该删列执行影响评估")
    return impact


@router.post("/dependencies/build")
def build_dependencies(body: dict[str, Any]) -> dict[str, Any]:
    """配置迁移读取回滚依赖数据并校验每个真实下游的恢复条件。"""
    source_table = body.get("source_table", "").strip()
    column = body.get("dropped_column", "").strip()
    if not source_table or not column:
        raise HTTPException(status_code=400, detail="source_table 和 dropped_column 必填")
    return _migration_service.load_rollback_dependencies(source_table, column)


@router.get("/dependencies/{source_table}/{dropped_column}")
def get_dependencies(source_table: str, dropped_column: str) -> dict[str, Any]:
    """读取回滚依赖清单与各下游验证状态。"""
    return rollback_engine.get_dependency_summary(source_table, dropped_column)


@router.post("/dependencies/validate")
def validate_downstream(body: dict[str, Any]) -> dict[str, Any]:
    """校验单个真实下游能否按旧列和有效分区恢复。

    body: {"source_table", "dropped_column", "target_table", "passed": true/false}
    """
    try:
        result = _migration_service.validate_downstream_recovery(
            source_table=body["source_table"],
            dropped_column=body["dropped_column"],
            target_table=body["target_table"],
            passed=bool(body.get("passed", False)),
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {exc}")
    return _require_result(result)


@router.post("/drill/prepare")
def prepare_drill(body: dict[str, Any]) -> dict[str, Any]:
    """据回滚依赖数据建立回滚演练输入，判断是否可执行及可回滚分区。"""
    try:
        return _migration_service.build_rollback_drill_input(
            body["source_table"], body["dropped_column"]
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {exc}")


@router.post("/drill/run")
def run_drill(body: dict[str, Any]) -> dict[str, Any]:
    """执行回滚演练；simulate_failure=true 时模拟演练失败。"""
    try:
        result = _migration_service.execute_rollback_drill(
            source_table=body["source_table"],
            dropped_column=body["dropped_column"],
            simulate_failure=bool(body.get("simulate_failure", False)),
            failure_reason=str(body.get("failure_reason", "")),
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {exc}")
    return _require_result(result)


@router.post("/drill/recover")
def recover_drill(body: dict[str, Any]) -> dict[str, Any]:
    """演练失败后恢复，证据仍锁定该轮路径版本。"""
    try:
        result = _migration_service.recover_failed_rollback_drill(
            body["source_table"], body["dropped_column"]
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"缺少必填参数: {exc}")
    return _require_result(result)


@router.get("/drill/{source_table}/{dropped_column}")
def get_drill_status(source_table: str, dropped_column: str) -> dict[str, Any]:
    """查询演练状态、可回滚分区与路径版本。"""
    result = _migration_service.get_rollback_drill_status(
        source_table, dropped_column
    )
    return _require_result(result)
