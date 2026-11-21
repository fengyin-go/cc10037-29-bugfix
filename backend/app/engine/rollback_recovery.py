"""删列回滚依赖与回滚演练引擎。

围绕 Schema 演进中 DROP_COLUMN 场景，回答三个问题：
- 字段删除的真实传递影响：沿血缘边的实际消费方向（源节点 → 目标节点）逐跳遍历，
  得到全部传递消费者，共享目标只纳入一次，不残留只算一跳的旧路径；
- 每个真实下游能否按旧列 + 有效分区恢复：基于分区证据做回滚就绪校验；
- 回滚演练可执行范围：下游验证全部完成才允许建演练输入，演练可回滚分区
  由各消费者的有效分区交集决定。

所有结论都带 ``path_version``：一次影响评估对应一轮血缘路径版本，演练失败
恢复后，旧列映射与数据分区证据仍引用该轮路径版本，不会静默漂移到新路径。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class FieldLineagePath:
    """一条字段消费路径（沿实际消费方向的有序节点链）。"""
    target_table: str
    hops: list[dict[str, str]]
    depth: int
    direct: bool
    transform_chain: list[str]


@dataclass
class RollbackDependency:
    """一个真实下游消费者的可核查回滚依赖。"""
    target_table: str
    old_column: str
    direct_consumer: bool
    depth: int
    validation_status: str
    rollback_ready: bool
    blocking_reasons: list[str]
    lineage_path: list[dict[str, str]]
    partition_evidence: dict[str, Any]
    path_version: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_table": self.target_table,
            "old_column": self.old_column,
            "direct_consumer": self.direct_consumer,
            "depth": self.depth,
            "validation_status": self.validation_status,
            "rollback_ready": self.rollback_ready,
            "blocking_reasons": list(self.blocking_reasons),
            "lineage_path": self.lineage_path,
            "partition_evidence": self.partition_evidence,
            "path_version": self.path_version,
        }


@dataclass
class RollbackDrill:
    """一次回滚演练及其输入/结果。"""
    drill_id: str
    source_table: str
    dropped_column: str
    path_version: str
    status: str
    round_no: int
    input_tables: list[str]
    recoverable_partitions: list[str]
    blocked_tables: list[dict[str, str]]
    pending_validation_tables: list[str]
    started_at: str | None = None
    finished_at: str | None = None
    failure_reason: str = ""
    history: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "drill_id": self.drill_id,
            "source_table": self.source_table,
            "dropped_column": self.dropped_column,
            "path_version": self.path_version,
            "status": self.status,
            "round_no": self.round_no,
            "input_tables": list(self.input_tables),
            "recoverable_partitions": list(self.recoverable_partitions),
            "recoverable_partition_count": len(self.recoverable_partitions),
            "blocked_tables": list(self.blocked_tables),
            "pending_validation_tables": list(self.pending_validation_tables),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "failure_reason": self.failure_reason,
            "history": list(self.history),
        }


class RollbackRecoveryEngine:
    """删列影响评估、回滚依赖校验与演练编排引擎。

    数据来源是 store 中的 ``lineage_graph`` 血缘边与分区证据。引擎自身只维护
    评估轮次、校验状态和演练状态，不修改血缘边本身。
    """

    def __init__(self, store: Any | None = None) -> None:
        # 延迟导入，避免引擎层与存储层产生导入环
        if store is None:
            from app.store import store as default_store
            store = default_store
        self._store = store
        # source_table -> column -> 影响评估结果（每轮一个 path_version）
        self._impacts: dict[str, dict[str, dict[str, Any]]] = {}
        # (source_table, column, target_table) -> 回滚依赖
        self._deps: dict[tuple[str, str, str], RollbackDependency] = {}
        # (source_table, column) -> 演练
        self._drills: dict[tuple[str, str], RollbackDrill] = {}
        self._round_counter: dict[tuple[str, str], int] = {}

    # ============================================================
    # 一、字段级传递影响：沿实际消费方向遍历
    # ============================================================

    @staticmethod
    def _edge_fields(edge: dict[str, Any]) -> list[str]:
        return [
            f.strip()
            for f in str(edge.get("传递字段", "")).split(",")
            if f.strip()
        ]

    @staticmethod
    def _edge_active(edge: dict[str, Any]) -> bool:
        return str(edge.get("是否活跃", "是")) == "是" and \
            str(edge.get("status", "活跃")) == "活跃"

    def trace_field_consumers(
        self, source_table: str, column: str
    ) -> list[FieldLineagePath]:
        """沿血缘边实际消费方向（源→目标）BFS 遍历字段的全部消费者。

        边必须活跃且传递字段包含目标列；共享目标通过 visited 去重，
        每个目标保留其最短（最先到达）消费路径。
        """
        col = column.strip().casefold()
        edges = self._store.rows("lineage_graph")
        # 邻接表：source -> [active edges]
        adjacency: dict[str, list[dict[str, Any]]] = {}
        for edge in edges:
            if not self._edge_active(edge):
                continue
            fields = [f.casefold() for f in self._edge_fields(edge)]
            if col not in fields:
                continue
            adjacency.setdefault(str(edge["源节点"]), []).append(edge)

        paths: list[FieldLineagePath] = []
        visited: set[str] = {source_table}
        # 队列元素：(当前节点, 已走跳数, 路径上的边)
        queue: list[tuple[str, int, list[dict[str, str]]]] = [
            (source_table, 0, [])
        ]

        while queue:
            current, depth, path_edges = queue.pop(0)
            for edge in adjacency.get(current, []):
                target = str(edge["目标节点"])
                hop = {
                    "source": str(edge["源节点"]),
                    "target": target,
                    "transform": str(edge.get("转换类型", "identity")),
                    "transfer_field": column,
                }
                next_edges = [*path_edges, hop]
                if target not in visited:
                    visited.add(target)
                    paths.append(FieldLineagePath(
                        target_table=target,
                        hops=next_edges,
                        depth=depth + 1,
                        direct=(depth == 0),
                        transform_chain=[h["transform"] for h in next_edges],
                    ))
                    queue.append((target, depth + 1, next_edges))
                # 已访问目标不再入队，但边仍属于图的一部分（共享目标只纳入一次）
        paths.sort(key=lambda p: (p.depth, p.target_table))
        return paths

    def evaluate_drop_impact(
        self, source_table: str, column: str
    ) -> dict[str, Any]:
        """评估删列影响并固化一轮可核查的路径版本。

        修复点：旧实现只取直接边的目标节点（一跳），经日汇总再到月汇总的
        传递消费者会从详情里丢失。这里改为沿实际消费方向完整 BFS。
        """
        round_no = self._round_counter.get((source_table, column), 0) + 1
        self._round_counter[(source_table, column)] = round_no
        path_version = f"PATH-{source_table.split('.')[-1]}-{column}-R{round_no:02d}"

        paths = self.trace_field_consumers(source_table, column)

        # 共享目标只纳入一次（trace 已去重，这里再以表名兜底）
        seen: set[str] = set()
        consumers: list[dict[str, Any]] = []
        for p in paths:
            if p.target_table in seen:
                continue
            seen.add(p.target_table)
            consumers.append({
                "target_table": p.target_table,
                "depth": p.depth,
                "direct_consumer": p.direct,
                "lineage_path": p.hops,
                "transform_chain": p.transform_chain,
            })

        direct_tables = [c["target_table"] for c in consumers if c["direct_consumer"]]
        transitive_tables = [
            c["target_table"] for c in consumers if not c["direct_consumer"]
        ]

        # 受影响质量规则（字段级）
        affected_rules = [
            {
                "rule_name": str(r.get("规则名称")),
                "target_table": str(r.get("目标表")),
                "check_field": str(r.get("校验字段")),
            }
            for r in self._store.rows("quality_rule")
            if column.casefold() in str(r.get("校验字段", "")).casefold()
            and str(r.get("目标表")) in seen
        ]

        impact = {
            "source_table": source_table,
            "dropped_column": column,
            "path_version": path_version,
            "round_no": round_no,
            "evaluated_at": _now(),
            "consumers": consumers,
            "affected_tables": [c["target_table"] for c in consumers],
            "affected_table_count": len(consumers),
            "direct_consumers": direct_tables,
            "transitive_consumers": transitive_tables,
            "max_depth": max((c["depth"] for c in consumers), default=0),
            "affected_quality_rules": affected_rules,
            "one_hop_only": False,
        }
        self._impacts.setdefault(source_table, {})[column] = impact
        return impact

    def get_impact(self, source_table: str, column: str) -> dict[str, Any] | None:
        return self._impacts.get(source_table, {}).get(column)

    # ============================================================
    # 二、回滚依赖数据：旧列映射 + 有效分区证据
    # ============================================================

    def _load_partition_evidence_rows(self) -> list[dict[str, Any]]:
        module = "rollback_partition_evidence"
        if module not in self._store._tables:  # noqa: SLF001 - 模块表由 seed 注册
            return []
        return self._store.rows(module)

    def _is_drill_locked(self, source_table: str, column: str) -> bool:
        """演练一旦建立，路径版本即锁定；失败恢复期间证据不得漂移到新版本。"""
        drill = self._drills.get((source_table, column))
        return drill is not None

    def build_rollback_dependencies(
        self, source_table: str, column: str
    ) -> dict[str, Any]:
        """读取影响评估的路径版本，为每个真实下游建立回滚依赖记录。

        校验每个真实下游能否按旧列和有效分区恢复：
        - 分区证据中必须存在该表、旧列名一致；
        - 证据必须带有效分区且标记已核验；
        - 演练建立前，证据跟随最新评估轮次重新打标；演练建立（含失败恢复）
          后，证据路径版本被锁定，与当前评估轮次不一致即判定不可恢复，
          保证恢复时旧列映射和分区证据仍采用该轮路径版本。
        """
        impact = self.get_impact(source_table, column)
        if impact is None:
            impact = self.evaluate_drop_impact(source_table, column)
        drill_locked = self._is_drill_locked(source_table, column)
        latest_version = impact["path_version"]
        if drill_locked:
            # 演练已建立（含失败恢复）：依赖清单口径锁定在该轮路径版本，
            # 重新评估产生的新轮次不改变回滚证据。
            locked_drill = self._drills[(source_table, column)]
            path_version = locked_drill.path_version
        else:
            path_version = latest_version

        evidence_rows = self._load_partition_evidence_rows()
        evidence_by_table = {
            str(row["目标表"]): row for row in evidence_rows
        }

        dependencies: list[RollbackDependency] = []
        # 演练锁定后若又出现新一轮评估，所有下游都标注跨轮次阻断，
        # 明确恢复只能使用锁定轮次证据，不能静默采用新路径。
        cross_round_blocked = drill_locked and latest_version != path_version
        for consumer in impact["consumers"]:
            table = consumer["target_table"]
            reasons: list[str] = []
            if cross_round_blocked:
                reasons.append(
                    f"路径版本已锁定为 {path_version}，存在更新评估轮次 "
                    f"{latest_version}，恢复仍须采用该轮路径版本"
                )
            raw = evidence_by_table.get(table)

            if raw is None:
                reasons.append("缺少该下游的旧列分区证据")
                evidence_dict: dict[str, Any] = {}
                verified = False
                valid_partitions: list[str] = []
            else:
                old_column = str(raw.get("旧列名", ""))
                if old_column.casefold() != column.casefold():
                    reasons.append(
                        f"证据旧列 {old_column} 与删除列 {column} 不一致"
                    )
                valid_partitions = [
                    p.strip()
                    for p in str(raw.get("有效分区清单", "")).split(",")
                    if p.strip()
                ]
                if not valid_partitions:
                    reasons.append("证据中没有有效分区可用于恢复")
                if str(raw.get("证据核验", "否")) != "是":
                    reasons.append("分区证据尚未核验")
                evidence_version = str(raw.get("path_version", ""))
                if evidence_version and evidence_version != path_version:
                    if drill_locked:
                        # 演练已建立（含失败恢复）：锁定版本，拒绝跨轮混用
                        reasons.append(
                            f"证据路径版本 {evidence_version} 已锁定，与评估轮次 "
                            f"{path_version} 不一致，恢复仍须采用该轮路径版本"
                        )
                    # 演练建立前：证据随最新评估轮次重新打标，不阻断
                if not drill_locked:
                    raw["path_version"] = path_version
                    evidence_version = path_version
                verified = not reasons
                evidence_dict = {
                    "table": table,
                    "old_column": old_column,
                    "partition_field": str(raw.get("分区字段", "dt")),
                    "earliest_partition": str(raw.get("最早可用分区", "")),
                    "latest_partition": str(raw.get("当前水位分区", "")),
                    "valid_partitions": valid_partitions,
                    "valid_partition_count": len(valid_partitions),
                    "rows": int(raw.get("存量行数", 0)),
                    "evidence_location": str(raw.get("证据位置", "")),
                    "verified": verified,
                    "path_version": evidence_version,
                }

            validation_status = "待验证"
            dep = RollbackDependency(
                target_table=table,
                old_column=column,
                direct_consumer=consumer["direct_consumer"],
                depth=consumer["depth"],
                validation_status=validation_status,
                rollback_ready=False,
                blocking_reasons=reasons,
                lineage_path=consumer["lineage_path"],
                partition_evidence=evidence_dict,
                path_version=path_version,
            )
            dependencies.append(dep)
            self._deps[(source_table, column, table)] = dep

        return self._dependency_summary(source_table, column, path_version)

    def validate_consumer(
        self,
        source_table: str,
        column: str,
        target_table: str,
        passed: bool,
    ) -> dict[str, Any]:
        """配置迁移侧回滚就绪校验：更新单个真实下游的验证结论。"""
        key = (source_table, column, target_table)
        dep = self._deps.get(key)
        if dep is None:
            # 尚未建立依赖时先补建
            self.build_rollback_dependencies(source_table, column)
            dep = self._deps.get(key)
        if dep is None:
            return {"error": f"下游 {target_table} 不在 {source_table}.{column} 的传递影响范围内"}

        if passed:
            if dep.blocking_reasons:
                dep.validation_status = "验证未通过"
                dep.rollback_ready = False
                return {
                    "error": "校验失败：回滚依赖未就绪",
                    "target_table": target_table,
                    "blocking_reasons": dep.blocking_reasons,
                }
            dep.validation_status = "验证通过"
            dep.rollback_ready = True
            if dep.partition_evidence:
                dep.partition_evidence["verified"] = True
        else:
            dep.validation_status = "验证未通过"
            dep.rollback_ready = False
            if "迁移校验未通过" not in dep.blocking_reasons:
                dep.blocking_reasons.append("迁移校验未通过")
        return dep.to_dict()

    def _dependency_summary(
        self, source_table: str, column: str, path_version: str
    ) -> dict[str, Any]:
        deps = [
            d for (s, c, _), d in self._deps.items()
            if s == source_table and c == column
        ]
        deps.sort(key=lambda d: (d.depth, d.target_table))
        ready = [d.target_table for d in deps if d.rollback_ready]
        pending = [
            d.target_table for d in deps
            if d.validation_status in ("待验证", "验证中")
        ]
        failed = [
            {"target_table": d.target_table, "reasons": d.blocking_reasons}
            for d in deps if d.validation_status == "验证未通过"
        ]
        return {
            "source_table": source_table,
            "dropped_column": column,
            "path_version": path_version,
            "total_consumers": len(deps),
            "rollback_ready_tables": ready,
            "rollback_ready_count": len(ready),
            "pending_validation_tables": pending,
            "validation_failed": failed,
            "all_validated": all(
                d.validation_status == "验证通过" for d in deps
            ) and bool(deps),
            "dependencies": [d.to_dict() for d in deps],
            "generated_at": _now(),
        }

    def get_dependency_summary(
        self, source_table: str, column: str
    ) -> dict[str, Any]:
        impact = self.get_impact(source_table, column)
        drill = self._drills.get((source_table, column))
        if drill is not None:
            # 演练已建立：对外口径始终是锁定轮次，不随重新评估漂移
            path_version = drill.path_version
        else:
            path_version = impact["path_version"] if impact else ""
        return self._dependency_summary(source_table, column, path_version)

    # ============================================================
    # 三、回滚演练输入与执行
    # ============================================================

    def prepare_drill_input(
        self, source_table: str, column: str
    ) -> dict[str, Any]:
        """据回滚依赖数据建立回滚演练输入。

        月汇总这类传递消费者验证未结束时，演练不可执行；可执行演练的
        可回滚分区由全部就绪消费者有效分区的交集决定——这些旧分区才
        可供下游继续取数。
        """
        impact = self.get_impact(source_table, column)
        if impact is None:
            self.evaluate_drop_impact(source_table, column)
        if not any(
            (s, c) == (source_table, column) for (s, c, _) in self._deps
        ):
            self.build_rollback_dependencies(source_table, column)

        summary = self.get_dependency_summary(source_table, column)
        path_version = summary["path_version"]
        deps = [
            d for (s, c, _), d in self._deps.items()
            if s == source_table and c == column
        ]
        deps.sort(key=lambda d: (d.depth, d.target_table))

        pending = [
            d.target_table for d in deps
            if d.validation_status in ("待验证", "验证中")
        ]
        not_ready = [
            {
                "target_table": d.target_table,
                "validation_status": d.validation_status,
                "blocking_reasons": d.blocking_reasons,
            }
            for d in deps if not d.rollback_ready
        ]

        if pending or not_ready:
            executable = False
            recoverable: list[str] = []
        else:
            executable = True
            partition_sets = [
                set(d.partition_evidence.get("valid_partitions", []))
                for d in deps
                if d.partition_evidence.get("valid_partitions")
            ]
            recoverable = sorted(
                set.intersection(*partition_sets) if partition_sets else set()
            )

        existing = self._drills.get((source_table, column))
        if existing is not None and existing.status == "演练中":
            return {
                "error": "当前演练正在执行中，不能重建演练输入",
                "drill": existing.to_dict(),
                "executable": False,
                "reason": "演练中",
                "migration_list_count": len(existing.input_tables),
                "impact_table_count": impact["affected_table_count"] if impact else len(deps),
                "count_consistent": len(existing.input_tables)
                == (impact["affected_table_count"] if impact else len(deps)),
                "prepared_at": _now(),
            }
        # 演练一旦建立，路径版本即锁定：即使期间重新评估过影响，
        # 后续演练（含失败恢复后的重跑）仍采用该轮路径版本。
        if existing is not None:
            path_version = existing.path_version
        # 已存在演练而当前依赖未就绪时，保留原演练输入与锁定版本，
        # 不用不可执行的新输入覆盖可重跑的历史演练。
        if existing is not None and (pending or not_ready):
            existing.history.append({
                "event": "演练输入重建被拒（依赖未就绪，保留锁定轮次）",
                "at": _now(),
                "path_version": existing.path_version,
            })
            return {
                "error": "当前回滚依赖未就绪，保留既有演练输入与锁定路径版本",
                "drill": existing.to_dict(),
                "executable": existing.status in ("可演练", "演练成功", "已恢复"),
                "reason": "仍有下游验证未结束或回滚依赖未就绪，演练暂不可执行",
                "migration_list_count": len(existing.input_tables),
                "impact_table_count": impact["affected_table_count"] if impact else len(deps),
                "count_consistent": len(existing.input_tables)
                == (impact["affected_table_count"] if impact else len(deps)),
                "prepared_at": _now(),
            }
        round_no = (existing.round_no + 1) if existing else 1
        drill = RollbackDrill(
            drill_id=f"DRILL-{source_table.split('.')[-1]}-{column}-{round_no:02d}",
            source_table=source_table,
            dropped_column=column,
            path_version=path_version,
            status="可演练" if executable else "待演练",
            round_no=round_no,
            input_tables=[d.target_table for d in deps],
            recoverable_partitions=recoverable,
            blocked_tables=not_ready,
            pending_validation_tables=pending,
            history=[
                {
                    "event": "演练输入已建立",
                    "at": _now(),
                    "path_version": path_version,
                    "executable": executable,
                }
            ],
        )
        self._drills[(source_table, column)] = drill

        return {
            "drill": drill.to_dict(),
            "executable": executable,
            "reason": (
                "全部真实下游验证通过，演练可执行"
                if executable
                else "仍有下游验证未结束或回滚依赖未就绪，演练暂不可执行"
            ),
            "migration_list_count": len(drill.input_tables),
            "impact_table_count": impact["affected_table_count"] if impact else len(deps),
            "count_consistent": len(drill.input_tables)
            == (impact["affected_table_count"] if impact else len(deps)),
            "prepared_at": _now(),
        }

    def run_drill(
        self,
        source_table: str,
        column: str,
        *,
        simulate_failure: bool = False,
        failure_reason: str = "",
    ) -> dict[str, Any]:
        """执行回滚演练。

        演练失败后进入恢复流程：恢复完成后旧列映射与数据分区证据仍采用
        该轮路径版本（依赖记录上的 path_version 不变），仅演练状态回退。
        """
        drill = self._drills.get((source_table, column))
        if drill is None:
            prepared = self.prepare_drill_input(source_table, column)
            drill = self._drills[(source_table, column)]
            if not prepared["executable"]:
                return {
                    "error": "演练不可执行：存在未完成验证的下游",
                    "drill": drill.to_dict(),
                }

        if drill.status not in ("可演练", "已恢复", "演练失败", "演练成功"):
            return {
                "error": f"当前演练状态 {drill.status} 不允许执行",
                "drill": drill.to_dict(),
            }
        if drill.status == "可演练" and (
            drill.pending_validation_tables or drill.blocked_tables
        ):
            return {
                "error": "演练不可执行：存在未完成验证或未就绪的下游",
                "drill": drill.to_dict(),
            }

        drill.started_at = _now()
        drill.status = "演练中"
        drill.history.append({
            "event": "演练开始",
            "at": drill.started_at,
            "path_version": drill.path_version,
        })

        if simulate_failure:
            drill.status = "演练失败"
            drill.failure_reason = failure_reason or "模拟回滚取数失败"
            drill.finished_at = _now()
            drill.history.append({
                "event": "演练失败",
                "at": drill.finished_at,
                "reason": drill.failure_reason,
                "path_version": drill.path_version,
            })
            return {
                "drill": drill.to_dict(),
                "result": "failed",
                "message": "演练失败，等待恢复；证据仍锁定在 "
                           f"{drill.path_version}",
            }

        drill.status = "演练成功"
        drill.failure_reason = ""
        drill.finished_at = _now()
        drill.history.append({
            "event": "演练成功",
            "at": drill.finished_at,
            "recoverable_partitions": list(drill.recoverable_partitions),
            "path_version": drill.path_version,
        })
        return {
            "drill": drill.to_dict(),
            "result": "success",
            "message": (
                f"演练成功，{len(drill.recoverable_partitions)} 个旧分区可供下游继续取数"
            ),
        }

    def recover_failed_drill(
        self, source_table: str, column: str
    ) -> dict[str, Any]:
        """演练失败后的恢复：重置演练状态，保留该轮路径版本的证据。"""
        drill = self._drills.get((source_table, column))
        if drill is None or drill.status != "演练失败":
            return {"error": "没有处于演练失败状态、需要恢复的演练"}

        # 关键约束：恢复后旧列映射与分区证据仍采用该轮路径版本
        dep_versions = {
            d.path_version
            for (s, c, _), d in self._deps.items()
            if s == source_table and c == column
        }
        evidence_versions = {
            d.partition_evidence.get("path_version")
            for (s, c, _), d in self._deps.items()
            if s == source_table and c == column and d.partition_evidence
        }
        versions_intact = dep_versions == {drill.path_version} and (
            not evidence_versions or evidence_versions == {drill.path_version}
        )

        drill.status = "已恢复"
        drill.history.append({
            "event": "失败恢复完成",
            "at": _now(),
            "path_version": drill.path_version,
            "evidence_path_version_preserved": versions_intact,
        })
        return {
            "drill": drill.to_dict(),
            "recovered": True,
            "path_version": drill.path_version,
            "evidence_path_version_preserved": versions_intact,
            "message": (
                "恢复完成，旧列映射与数据分区证据仍采用 "
                f"{drill.path_version}，可重新发起演练"
            ),
        }

    def get_drill(self, source_table: str, column: str) -> dict[str, Any] | None:
        drill = self._drills.get((source_table, column))
        return drill.to_dict() if drill else None
