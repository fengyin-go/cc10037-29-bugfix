"""配置迁移领域服务：管理跨环境管线配置的导出、导入和差异对比。

上游：pipeline_def + task_node 产出配置快照 → config_migration 迁移
消费：backfill_tracker 消费迁移进度用于回填状态追踪
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.engine.rollback_recovery import RollbackRecoveryEngine


# 模块级单例：回滚依赖与演练状态需要在多次请求间保持，且与血缘/证据同生命周期。
rollback_engine = RollbackRecoveryEngine(store)


class ConfigMigrationService(BaseModuleService):
    MODULE = "config_migration"
    ENTITY = "迁移任务"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["任务名称", "源环境", "目标环境", "迁移范围", "依赖检查", "回滚方案", "执行窗口"]
    REQUIRED_FIELDS = ["任务名称", "源环境", "目标环境"]
    STATUS_ORDER = ["待迁移", "迁移中", "已完成", "已回滚", "迁移失败"]
    ACTION_RULES = {"开始迁移": "待迁移", "完成迁移": "迁移中", "回滚迁移": "迁移中", "标记失败": "迁移中"}
    NEGATIVE_ACTIONS = ["回滚迁移"]

    def query_by_environment(self, env: str) -> list[dict[str, Any]]:
        """按环境查询迁移任务。

        筛选指定源环境或目标环境的迁移记录，
        返回任务列表供环境运维面板消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if env.casefold() in str(row.get("源环境", "")).casefold() or \
               env.casefold() in str(row.get("目标环境", "")).casefold():
                result.append(dict(row))
        return result

    def check_dependencies(self, task_name: str) -> dict[str, Any]:
        """检查迁移任务的依赖是否满足。

        根据依赖检查字段中的配置项，在 store 中查找对应的管线配置
        判断依赖是否准备就绪。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        dep_str = str(task.get("依赖检查", ""))
        deps = [d.strip() for d in dep_str.split(",") if d.strip()]
        dep_status: dict[str, bool] = {}

        for dep in deps:
            found = False
            for pipeline_row in store.rows("pipeline_def"):
                if dep.casefold() in str(pipeline_row.get("流水线名称", "")).casefold():
                    dep_status[dep] = pipeline_row.get("status") == "已上线"
                    found = True
                    break
            if not found:
                dep_status[dep] = False

        all_met = all(dep_status.values()) if dep_status else True
        return {
            "task": task_name,
            "source_env": task.get("源环境"),
            "target_env": task.get("目标环境"),
            "dependencies": dep_status,
            "all_dependencies_met": all_met,
            "can_migrate": all_met and task.get("status") == "待迁移",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_diff_summary(self, task_name: str) -> dict[str, Any]:
        """计算迁移范围的差异摘要。

        对比源和目标环境中的管线配置数量差异，
        产出迁移影响评估数据供审批参考。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        source_env = str(task.get("源环境", ""))
        target_env = str(task.get("目标环境", ""))

        pipeline_rows = store.rows("pipeline_def")
        task_rows = store.rows("task_node")
        rule_rows = store.rows("quality_rule")

        source_pipelines = len(pipeline_rows)
        source_nodes = len(task_rows)
        source_rules = len(rule_rows)

        return {
            "task": task_name,
            "source_env": source_env,
            "target_env": target_env,
            "items_to_migrate": {
                "pipelines": source_pipelines,
                "task_nodes": source_nodes,
                "quality_rules": source_rules,
            },
            "total_items": source_pipelines + source_nodes + source_rules,
            "rollback_plan": task.get("回滚方案", "未配置"),
            "execution_window": task.get("执行窗口", "未设定"),
            "summary_at": datetime.now().isoformat(),
        }

    def validate_rollback_readiness(self, task_name: str) -> dict[str, Any]:
        """验证回滚准备状态。

        检查回滚方案是否已配置和最近迁移历史，
        确保迁移失败时能安全回滚。
        """
        task = None
        for row in store.rows(self.MODULE):
            if row.get("任务名称") == task_name:
                task = row
                break
        if task is None:
            return {"error": f"迁移任务 {task_name} 不存在"}

        rollback = str(task.get("回滚方案", ""))
        has_rollback = len(rollback) > 0 and rollback != "未配置"
        recent_rollbacks = [
            r for r in store.rows(self.MODULE)
            if r.get("status") == "已回滚" and r.get("任务名称") != task_name
        ]

        return {
            "task": task_name,
            "rollback_configured": has_rollback,
            "rollback_plan": rollback,
            "recent_rollback_count": len(recent_rollbacks),
            "safe_to_migrate": has_rollback,
            "validated_at": datetime.now().isoformat(),
        }

    # ---- 删列迁移：读取回滚依赖数据并驱动回滚演练 ----

    def load_rollback_dependencies(
        self, source_table: str, dropped_column: str
    ) -> dict[str, Any]:
        """配置迁移读取回滚依赖数据。

        先沿实际消费方向固化删列传递影响（路径版本），再为每个真实下游
        校验能否按旧列和有效分区恢复，返回可核查的依赖清单。迁移清单表
        数量与影响表数量在同一口径下保持一致，共享目标只纳入一次。
        """
        impact = rollback_engine.get_impact(source_table, dropped_column)
        if impact is None:
            impact = rollback_engine.evaluate_drop_impact(source_table, dropped_column)
        summary = rollback_engine.build_rollback_dependencies(
            source_table, dropped_column
        )
        summary["impact_table_count"] = impact["affected_table_count"]
        summary["migration_list_count"] = summary["total_consumers"]
        summary["count_consistent"] = (
            summary["migration_list_count"] == summary["impact_table_count"]
        )
        return summary

    def validate_downstream_recovery(
        self,
        source_table: str,
        dropped_column: str,
        target_table: str,
        passed: bool,
    ) -> dict[str, Any]:
        """校验单个真实下游能否按旧列和有效分区恢复。

        月汇总等传递消费者验证未结束时保持待验证状态，不允许提前判定就绪。
        """
        return rollback_engine.validate_consumer(
            source_table, dropped_column, target_table, passed
        )

    def build_rollback_drill_input(
        self, source_table: str, dropped_column: str
    ) -> dict[str, Any]:
        """据回滚依赖数据建立回滚演练输入。

        可执行演练由全部下游验证通过决定；可回滚分区取各消费者有效分区
        交集，即这些旧分区可供下游继续取数。
        """
        return rollback_engine.prepare_drill_input(source_table, dropped_column)

    def execute_rollback_drill(
        self,
        source_table: str,
        dropped_column: str,
        *,
        simulate_failure: bool = False,
        failure_reason: str = "",
    ) -> dict[str, Any]:
        """执行回滚演练（可注入失败以模拟演练失败场景）。"""
        return rollback_engine.run_drill(
            source_table,
            dropped_column,
            simulate_failure=simulate_failure,
            failure_reason=failure_reason,
        )

    def recover_failed_rollback_drill(
        self, source_table: str, dropped_column: str
    ) -> dict[str, Any]:
        """演练失败后恢复：旧列映射与分区证据仍采用该轮路径版本。"""
        return rollback_engine.recover_failed_drill(source_table, dropped_column)

    def get_rollback_drill_status(
        self, source_table: str, dropped_column: str
    ) -> dict[str, Any]:
        """查询当前演练状态与锁定的路径版本。"""
        drill = rollback_engine.get_drill(source_table, dropped_column)
        if drill is None:
            return {"error": "尚未建立回滚演练输入"}
        return drill

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg