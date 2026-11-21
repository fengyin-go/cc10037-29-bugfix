"""端到端核查脚本：模拟事实表删列的完整回滚演练业务链路。

运行：backend/.venv/bin/python -I /workspace/verify/e2e_rollback_drill.py
（使用 -I 隔离模式，脚本目录与仓库目录隔离）
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/backend")

from app.engine.rollback_recovery import RollbackRecoveryEngine  # noqa: E402
from app.services.config_migration import (  # noqa: E402
    ConfigMigrationService,
    rollback_engine,
)
from app.services.lineage_graph import LineageGraphService  # noqa: E402
from app.services.schema_evolution import SchemaEvolutionService  # noqa: E402
from app.store import store  # noqa: E402

SOURCE = "dw.fact_trade_order"
COLUMN = "legacy_order_amount"
DAILY = "dw.dws_trade_amount_daily"
MONTHLY = "dw.ads_trade_amount_monthly"
ACCOUNTING = "dw.ads_accounting_check"

failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


# ---------- 0. 基线数据 ----------
print("=== 0. 基线：血缘与分区证据 ===")
edges = [
    (r["源节点"], r["目标节点"])
    for r in store.rows("lineage_graph")
    if COLUMN in r["传递字段"] and r["status"] == "活跃"
]
print("携带旧金额列的活跃边:", edges)
check("血缘含事实->日汇总", (SOURCE, DAILY) in edges)
check("血缘含日汇总->月汇总", (DAILY, MONTHLY) in edges)
check("血缘含事实->独立核算", (SOURCE, ACCOUNTING) in edges)

evidence_tables = {r["目标表"] for r in store.rows("rollback_partition_evidence")}
check("分区证据覆盖三个真实下游", evidence_tables == {DAILY, MONTHLY, ACCOUNTING},
      str(sorted(evidence_tables)))

# ---------- 1. 修复后的字段影响分析（不再只算一跳） ----------
print("\n=== 1. Schema 评估入口：字段传递影响 ===")
schema_svc = SchemaEvolutionService()
field_impact = schema_svc.analyze_field_impact(SOURCE, COLUMN)
print("受影响目标表:", field_impact["affected_downstream_nodes"])
print("直接消费者:", field_impact["direct_consumers"])
print("传递消费者:", field_impact["transitive_consumers"])
print("最大影响深度:", field_impact["max_impact_depth"])
check("影响表共3张（共享目标只一次）", field_impact["affected_table_count"] == 3)
check("月汇总未从影响详情丢失", MONTHLY in field_impact["affected_downstream_nodes"])
check("月汇总被识别为二跳传递消费者",
      MONTHLY in field_impact["transitive_consumers"])
check("日汇总/核算为一跳直接消费者",
      set(field_impact["direct_consumers"]) == {DAILY, ACCOUNTING})
check("结果明确非一跳路径", field_impact["one_hop_only"] is False)
monthly_consumer = next(c for c in field_impact["consumers"]
                        if c["target_table"] == MONTHLY)
check("月汇总消费路径可核查（经日汇总）",
      [h["target"] for h in monthly_consumer["lineage_path"]] == [DAILY, MONTHLY])

# 血缘服务同口径
lineage_svc = LineageGraphService()
scope = lineage_svc.compute_impact_scope(SOURCE)
print("血缘影响范围:", scope)
check("血缘影响范围同样含全部3张下游",
      set(scope["affected_tables"]) == {DAILY, MONTHLY, ACCOUNTING},
      str(scope["affected_tables"]))
trace = lineage_svc.trace_field_lineage(COLUMN, source_node=SOURCE)
check("字段血缘链覆盖4条边（含两跳）", trace["edge_count"] == 3,
      f"edge_count={trace['edge_count']}")

# ---------- 2. 影响评估固化路径版本 ----------
print("\n=== 2. 传递影响评估与路径版本 ===")
impact = rollback_engine.evaluate_drop_impact(SOURCE, COLUMN)
print("path_version:", impact["path_version"])
print("affected_tables:", impact["affected_tables"])
check("评估影响表数=3", impact["affected_table_count"] == 3)
check("评估消费者去重", len(impact["affected_tables"]) == len(set(impact["affected_tables"])))
path_version_r1 = impact["path_version"]

# ---------- 3. 配置迁移读取回滚依赖数据 ----------
print("\n=== 3. 回滚依赖数据（旧列 + 有效分区校验） ===")
migration_svc = ConfigMigrationService()
deps_summary = migration_svc.load_rollback_dependencies(SOURCE, COLUMN)
print("就绪:", deps_summary["rollback_ready_tables"])
print("待验证:", deps_summary["pending_validation_tables"])
print("清单表数 vs 影响表数:",
      deps_summary["migration_list_count"], deps_summary["impact_table_count"])
check("迁移清单与影响表数量一致", deps_summary["count_consistent"] is True)
check("依赖覆盖全部3个真实下游", deps_summary["total_consumers"] == 3)

dep_by_table = {d["target_table"]: d for d in deps_summary["dependencies"]}
check("日汇总证据已核验、初始待验证",
      dep_by_table[DAILY]["validation_status"] == "待验证")
check("月汇总因证据未核验存在阻断原因",
      any("分区证据尚未核验" in r for r in dep_by_table[MONTHLY]["blocking_reasons"]))
check("所有依赖锁定首轮路径版本",
      all(d["path_version"] == path_version_r1 for d in deps_summary["dependencies"]))

# 未核验的月汇总不能直接判通过
bad = migration_svc.validate_downstream_recovery(SOURCE, COLUMN, MONTHLY, True)
check("月汇总证据未核验时校验被拒", bad.get("error") is not None, str(bad.get("error")))

# ---------- 4. 月汇总验证未结束 -> 演练不可执行 ----------
print("\n=== 4. 演练输入：月汇总验证未结束 ===")
prepared = migration_svc.build_rollback_drill_input(SOURCE, COLUMN)
print("可执行:", prepared["executable"], "| 原因:", prepared["reason"])
print("待验证表:", prepared["drill"]["pending_validation_tables"])
check("月汇总未验证时演练不可执行", prepared["executable"] is False)
check("不可执行时无可回滚分区",
        prepared["drill"]["recoverable_partitions"] == [])
check("演练清单仍含全部3张表",
        set(prepared["drill"]["input_tables"]) == {DAILY, MONTHLY, ACCOUNTING})
check("清单/影响数量一致", prepared["count_consistent"] is True)

# 直接 run 应被拒绝
run_rejected = migration_svc.execute_rollback_drill(SOURCE, COLUMN)
check("未就绪时执行演练被拒", run_rejected.get("error") is not None)

# ---------- 5. 完成两个已核验下游的迁移校验 ----------
print("\n=== 5. 完成日汇总/核算分支校验，月汇总补核验后校验 ===")
r1 = migration_svc.validate_downstream_recovery(SOURCE, COLUMN, DAILY, True)
r2 = migration_svc.validate_downstream_recovery(SOURCE, COLUMN, ACCOUNTING, True)
check("日汇总校验通过", r1["validation_status"] == "验证通过" and r1["rollback_ready"])
check("核算分支校验通过", r2["validation_status"] == "验证通过" and r2["rollback_ready"])

# 月汇总证据核验完成（模拟分区扫描核验结束），随后通过校验
for row in store.rows("rollback_partition_evidence"):
    if row["目标表"] == MONTHLY:
        row["证据核验"] = "是"
        row["status"] = "已核验"
        row["pending"] = False
# 重新构建依赖以读取核验后的证据（同一影响轮次，路径版本保持）
deps_summary2 = rollback_engine.build_rollback_dependencies(SOURCE, COLUMN)
version_after_rebuild = deps_summary2["path_version"]
print("重建依赖后路径版本:", version_after_rebuild, "（首轮:", path_version_r1, "）")
check("未重新评估影响时路径版本保持首轮",
      version_after_rebuild == path_version_r1)

# 重建会把验证状态重置为待验证；三个下游重新校验
for table in (DAILY, ACCOUNTING, MONTHLY):
    res = migration_svc.validate_downstream_recovery(SOURCE, COLUMN, table, True)
    check(f"{table} 校验通过", res.get("validation_status") == "验证通过", str(res)[:120])

final_deps = rollback_engine.get_dependency_summary(SOURCE, COLUMN)
check("全部下游验证完成", final_deps["all_validated"] is True)

# ---------- 6. 演练可执行：可回滚分区 = 有效分区交集 ----------
print("\n=== 6. 可执行演练与可回滚分区 ===")
prepared3 = migration_svc.build_rollback_drill_input(SOURCE, COLUMN)
recoverable = prepared3["drill"]["recoverable_partitions"]
print("可执行:", prepared3["executable"], "| 可回滚分区:", recoverable)
# 日/核算有效分区 11-18..20；月汇总有效分区 11-19..20 -> 交集 11-19,11-20
check("演练可执行", prepared3["executable"] is True)
check("可回滚分区取三方交集", recoverable == ["2026-11-19", "2026-11-20"], str(recoverable))
check("可回滚分区数=2", prepared3["drill"]["recoverable_partition_count"] == 2)
check("这些旧分区可供全部下游继续取数",
        set(prepared3["drill"]["input_tables"]) == {DAILY, MONTHLY, ACCOUNTING})
locked_version = prepared3["drill"]["path_version"]

# ---------- 7. 演练失败 -> 恢复，证据仍锁定该轮路径版本 ----------
print("\n=== 7. 演练失败与恢复（路径版本不漂移） ===")
failed = migration_svc.execute_rollback_drill(
    SOURCE, COLUMN, simulate_failure=True,
    failure_reason="月汇总旧分区 2026-11-19 取数校验行数不符",
)
check("演练进入失败状态", failed["drill"]["status"] == "演练失败")
check("失败演练保留路径版本", failed["drill"]["path_version"] == locked_version)

recovered = migration_svc.recover_failed_rollback_drill(SOURCE, COLUMN)
check("恢复完成", recovered["recovered"] is True)
check("恢复后证据路径版本保持不变",
      recovered["evidence_path_version_preserved"] is True)
check("恢复后状态为已恢复", recovered["drill"]["status"] == "已恢复")

dep_versions = {
    d.path_version
    for (s, c, _t), d in rollback_engine._deps.items()
    if s == SOURCE and c == COLUMN
}
check("旧列映射依赖仍采用该轮路径版本", dep_versions == {locked_version}, str(dep_versions))
evidence_versions = {
    d.partition_evidence.get("path_version")
    for (s, c, _t), d in rollback_engine._deps.items()
    if s == SOURCE and c == COLUMN
}
check("数据分区证据仍采用该轮路径版本", evidence_versions == {locked_version},
      str(evidence_versions))

# 恢复后可重新演练并成功
rerun = migration_svc.execute_rollback_drill(SOURCE, COLUMN)
check("恢复后重跑演练成功", rerun["drill"]["status"] == "演练成功", str(rerun.get("error")))
check("成功演练沿用同轮证据版本",
      rerun["drill"]["path_version"] == locked_version)

# ---------- 8. 汇总 ----------
print("\n=== 汇总 ===")
if failures:
    print(f"共 {len(failures)} 项失败: {failures}")
    sys.exit(1)
print("全部核查通过 ✓")
