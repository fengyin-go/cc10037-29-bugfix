"""数据血缘领域服务：管理表间字段级血缘图、影响分析和追溯查询。

上游：pipeline_def + task_node 产出数据流转 → lineage_graph 建立血缘
消费：schema_evolution 消费血缘信息评估变更影响
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘边"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["源节点", "目标节点", "传递字段", "转换类型", "数据量级", "血缘深度", "是否活跃"]
    REQUIRED_FIELDS = ["源节点", "目标节点", "传递字段"]
    STATUS_ORDER = ["活跃", "已失效", "已归档"]
    ACTION_RULES = {"标记失效": "活跃", "归档边": "已失效"}
    NEGATIVE_ACTIONS = []

    def query_upstream(self, target_node: str) -> list[dict[str, Any]]:
        """查询指定节点的上游血缘。

        遍历 store 中的血缘边记录，查找以该节点为目标的边，
        返回上游源节点信息供影响分析消费。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if target_node.casefold() in str(row.get("目标节点", "")).casefold():
                result.append(dict(row))
        return result

    def query_downstream(self, source_node: str) -> list[dict[str, Any]]:
        """查询指定节点的下游血缘。

        遍历 store 中的血缘边记录，查找以该节点为源的边，
        返回下游目标节点信息供变更影响范围评估。
        """
        result: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if source_node.casefold() in str(row.get("源节点", "")).casefold():
                result.append(dict(row))
        return result

    def trace_field_lineage(self, field_name: str, source_node: str | None = None) -> dict[str, Any]:
        """追溯字段的端到端血缘链路。

        从源节点（未指定时从所有包含该字段的活跃边起点）出发，沿血缘边
        实际消费方向（源节点 → 目标节点）逐跳 BFS，构建字段级数据流向图。
        共享目标只纳入一次，传递消费者不会因为多跳而丢失。
        """
        col = field_name.strip().casefold()
        edges = store.rows(self.MODULE)
        field_edges: list[dict[str, Any]] = []
        visited: set[str] = set()

        def _carries_field(edge: dict[str, Any]) -> bool:
            return col in [
                f.strip().casefold()
                for f in str(edge.get("传递字段", "")).split(",") if f.strip()
            ]

        def _is_active(edge: dict[str, Any]) -> bool:
            return str(edge.get("是否活跃", "是")) == "是" and \
                str(edge.get("status", "活跃")) == "活跃"

        adjacency: dict[str, list[dict[str, Any]]] = {}
        roots: set[str] = set()
        for edge in edges:
            if not _is_active(edge) or not _carries_field(edge):
                continue
            src = str(edge.get("源节点", ""))
            adjacency.setdefault(src, []).append(edge)
            roots.add(src)

        if source_node:
            roots = {source_node}

        queue: list[tuple[str, int]] = [(root, 0) for root in sorted(roots)]
        enqueued: set[str] = set(roots)
        while queue:
            current, depth = queue.pop(0)
            for edge in adjacency.get(current, []):
                target = str(edge.get("目标节点", ""))
                dedup_key = f"{current}->{target}"
                if dedup_key in visited:
                    continue
                visited.add(dedup_key)
                field_edges.append({
                    "source": current,
                    "target": target,
                    "transform": edge.get("转换类型"),
                    "data_volume": edge.get("数据量级"),
                    "depth": depth + 1,
                    "direct": depth == 0,
                    "transfer_field": field_name,
                })
                if target not in enqueued:
                    enqueued.add(target)
                    queue.append((target, depth + 1))

        affected_tables = list(dict.fromkeys(item["target"] for item in field_edges))
        return {
            "field": field_name,
            "source_node": source_node,
            "edge_count": len(field_edges),
            "lineage_chain": field_edges,
            "affected_tables": affected_tables,
            "affected_table_count": len(affected_tables),
            "traced_at": datetime.now().isoformat(),
        }

    def compute_impact_scope(self, source_node: str, max_depth: int = 5) -> dict[str, Any]:
        """计算源节点变更的影响范围。

        沿活跃血缘边的实际消费方向（源节点 → 目标节点）递归遍历，统计全部
        受影响的表和处理节点数量，用于变更审批和风险评估。共享目标只纳入
        一次，多跳传递消费者（如日汇总 → 月汇总）不会被漏算。
        """
        direct_edges = [
            edge for edge in self.query_downstream(source_node)
            if str(edge.get("是否活跃", "是")) == "是"
            and str(edge.get("status", "活跃")) == "活跃"
        ]
        affected_tables: set[str] = set()
        max_reached_depth = 0
        # BFS：visited 记录已入队节点，保证共享目标只纳入一次，
        # 同时所有可达的多跳消费者都会被继续展开。
        visited: set[str] = {source_node}
        queue: list[tuple[str, int]] = []
        for edge in direct_edges:
            tgt = str(edge.get("目标节点", ""))
            if tgt not in visited:
                visited.add(tgt)
                queue.append((tgt, 1))

        while queue:
            node, depth = queue.pop(0)
            if depth > max_depth:
                continue
            affected_tables.add(node)
            max_reached_depth = max(max_reached_depth, depth)
            for edge in store.rows(self.MODULE):
                if str(edge.get("是否活跃", "是")) != "是" or \
                        str(edge.get("status", "活跃")) != "活跃":
                    continue
                if str(edge.get("源节点", "")) != node:
                    continue
                tgt = str(edge.get("目标节点", ""))
                if tgt not in visited:
                    visited.add(tgt)
                    queue.append((tgt, depth + 1))

        return {
            "source": source_node,
            "direct_downstream": len(direct_edges),
            "total_affected": len(affected_tables),
            "max_depth": max_reached_depth,
            "affected_tables": sorted(affected_tables),
            "computed_at": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry:
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg