"""Schema变更领域服务：管理表字段变更、兼容性评估和影响分析。

上游：异常捕获字段级异常 → schema_evolution 记录变更
消费：quality_rule 根据变更结果调整校验规则
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchemaEvolutionService(BaseModuleService):
    MODULE = "schema_evolution"
    ENTITY = "变更记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["变更时间", "目标表", "变更类型", "变更详情", "兼容性评估", "影响管线", "审批状态"]
    REQUIRED_FIELDS = ["变更时间", "目标表", "变更类型"]
    STATUS_ORDER = ["待评估", "兼容", "不兼容", "已执行", "已回滚"]
    ACTION_RULES = {"评估兼容": "待评估", "执行变更": "兼容", "回滚变更": "不兼容"}
    NEGATIVE_ACTIONS = ["回滚变更"]

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按目标表查询 Schema 变更记录。

        从 store 中筛选指定表的字段变更历史，
        返回变更列表供数据治理和审计消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if table_name.casefold() in str(row.get("目标表", "")).casefold():
                result.append(dict(row))
        return result

    def evaluate_compatibility(self, change_id: int) -> dict[str, Any]:
        """评估 Schema 变更的兼容性。

        分析变更类型和影响范围，根据变更类型判断
        是向前兼容还是向后不兼容的变更。
        """
        entry = store.find(self.MODULE, change_id)
        if entry is None:
            return {"error": f"变更记录 {change_id} 不存在"}

        change_type = str(entry.get("变更类型", ""))
        change_detail = str(entry.get("变更详情", ""))
        table_name = str(entry.get("目标表", ""))

        is_compatible = True
        risk_level = "低"

        compatible_ops = {"增加字段", "新增列", "添加字段", "ADD COLUMN"}
        incompatible_ops = {"删除字段", "重命名字段", "修改类型", "删除列",
                             "DROP COLUMN", "MODIFY COLUMN", "CHANGE COLUMN"}

        if change_type in compatible_ops:
            is_compatible = True
            risk_level = "低"
        elif change_type in incompatible_ops:
            is_compatible = False
            risk_level = "高"
        else:
            is_compatible = True
            risk_level = "中"

        affected_pipelines = str(entry.get("影响管线", ""))
        pipeline_list = [
            p.strip() for p in affected_pipelines.split(",") if p.strip()
        ] if affected_pipelines else []

        entry["兼容性评估"] = "兼容" if is_compatible else "不兼容"

        return {
            "change_id": change_id,
            "table": table_name,
            "change_type": change_type,
            "change_detail": change_detail,
            "is_compatible": is_compatible,
            "risk_level": risk_level,
            "affected_pipelines": pipeline_list,
            "recommendation": (
                "变更可安全执行" if is_compatible
                else "变更不兼容，需制定回滚方案并通知下游管线"
            ),
            "evaluated_at": datetime.now().isoformat(),
        }

    def analyze_field_impact(self, table_name: str, field_name: str) -> dict[str, Any]:
        """分析字段变更的影响范围。

        修复点：旧实现只扫描血缘边里直接包含该字段的一跳目标，经日汇总再
        传递到月汇总这类多跳消费者会从详情目标表中丢失（关系遍历能看到月
        汇总，影响详情却漏了它）。现改为沿血缘边实际消费方向（源节点 →
        目标节点）逐跳 BFS，输出全部传递消费者；共享目标只纳入一次，并保留
        每个目标的消费路径与跳数供核查。
        """
        changes = self.query_by_table(table_name)
        related_changes = [
            c for c in changes
            if field_name.casefold() in str(c.get("变更详情", "")).casefold()
        ]

        lineage_edges = store.rows("lineage_graph") if "lineage_graph" in store._tables else []
        col = field_name.strip().casefold()

        def _edge_carries_field(edge: dict[str, Any]) -> bool:
            fields = [
                f.strip().casefold()
                for f in str(edge.get("传递字段", "")).split(",") if f.strip()
            ]
            return col in fields

        def _edge_active(edge: dict[str, Any]) -> bool:
            return str(edge.get("是否活跃", "是")) == "是" and \
                str(edge.get("status", "活跃")) == "活跃"

        # 邻接表：仅保留活跃且传递该字段的边
        adjacency: dict[str, list[dict[str, Any]]] = {}
        for edge in lineage_edges:
            if _edge_active(edge) and _edge_carries_field(edge):
                adjacency.setdefault(str(edge.get("源节点", "")), []).append(edge)

        # 沿实际消费方向 BFS，visited 保证共享目标只纳入一次
        consumers: list[dict[str, Any]] = []
        visited: set[str] = {table_name}
        queue: list[tuple[str, int, list[dict[str, str]]]] = [(table_name, 0, [])]
        while queue:
            current, depth, path = queue.pop(0)
            for edge in adjacency.get(current, []):
                target = str(edge.get("目标节点", ""))
                hop = {
                    "source": current,
                    "target": target,
                    "transform": str(edge.get("转换类型", "identity")),
                    "transfer_field": field_name,
                }
                next_path = [*path, hop]
                if target not in visited:
                    visited.add(target)
                    consumers.append({
                        "target_table": target,
                        "depth": depth + 1,
                        "direct_consumer": depth == 0,
                        "lineage_path": next_path,
                    })
                    queue.append((target, depth + 1, next_path))

        consumers.sort(key=lambda item: (item["depth"], item["target_table"]))
        affected_targets = [item["target_table"] for item in consumers]
        direct_targets = [
            item["target_table"] for item in consumers if item["direct_consumer"]
        ]
        transitive_targets = [
            item["target_table"] for item in consumers if not item["direct_consumer"]
        ]

        quality_rules = store.rows("quality_rule") if "quality_rule" in store._tables else []
        affected_rules = []
        for rule in quality_rules:
            if field_name.casefold() in str(rule.get("校验字段", "")).casefold():
                affected_rules.append(rule.get("规则名称"))

        return {
            "table": table_name,
            "field": field_name,
            "change_history_count": len(related_changes),
            "affected_downstream_nodes": affected_targets,
            "direct_consumers": direct_targets,
            "transitive_consumers": transitive_targets,
            "consumers": consumers,
            "affected_quality_rules": affected_rules,
            "total_impact_items": len(affected_targets) + len(affected_rules),
            "affected_table_count": len(affected_targets),
            "max_impact_depth": max((item["depth"] for item in consumers), default=0),
            "one_hop_only": False,
            "risk_summary": (
                "高影响：涉及多个下游节点和质量规则"
                if len(affected_targets) + len(affected_rules) > 3
                else "影响范围可控"
            ),
            "analyzed_at": datetime.now().isoformat(),
        }

    def compute_evolution_stats(self, table_name: str) -> dict[str, Any]:
        """统计表 Schema 变更的演化趋势。

        汇总指定表的字段变更频率和类型分布，
        产出 Schema 稳定性评估数据。
        """
        changes = self.query_by_table(table_name)
        by_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        compatible = 0
        incompatible = 0

        for c in changes:
            ctype = str(c.get("变更类型", "未知"))
            cstatus = str(c.get("status", "未知"))
            by_type[ctype] = by_type.get(ctype, 0) + 1
            by_status[cstatus] = by_status.get(cstatus, 0) + 1
            if c.get("兼容性评估") == "兼容":
                compatible += 1
            elif c.get("兼容性评估") == "不兼容":
                incompatible += 1

        return {
            "table": table_name,
            "total_changes": len(changes),
            "compatible_changes": compatible,
            "incompatible_changes": incompatible,
            "by_type": by_type,
            "by_status": by_status,
            "stability_score": round(compatible / max(len(changes), 1) * 100, 1),
            "snapshot_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg