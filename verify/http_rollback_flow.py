"""HTTP 端到端核查：通过真实 FastAPI 路由走完整删列回滚演练流程。"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:8011"
SOURCE = "dw.fact_trade_order"
COLUMN = "legacy_order_amount"
DAILY = "dw.dws_trade_amount_daily"
MONTHLY = "dw.ads_trade_amount_monthly"
ACCOUNTING = "dw.ads_accounting_check"

failures: list[str] = []


def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return {"_http_error": exc.code, "_detail": json.loads(exc.read().decode())}


def check(name: str, condition: bool, detail: str = "") -> None:
    print(f"[{'PASS' if condition else 'FAIL'}] {name}" + (f" -> {detail}" if detail else ""))
    if not condition:
        failures.append(name)


# 1. 影响评估
r = call("POST", "/api/rollback/impact/evaluate",
         {"source_table": SOURCE, "dropped_column": COLUMN})
impact = r["impact"]
print("影响表:", impact["affected_tables"], "| 版本:", impact["path_version"])
check("HTTP 影响表含月汇总", MONTHLY in impact["affected_tables"])
check("HTTP 影响表数=3", impact["affected_table_count"] == 3)
check("两个入口口径一致", r["consistent"] is True)
check("字段详情含消费路径",
      any(c["target_table"] == MONTHLY for c in r["field_impact_detail"]["consumers"]))
version = impact["path_version"]

# 2. 回滚依赖
r = call("POST", "/api/rollback/dependencies/build",
         {"source_table": SOURCE, "dropped_column": COLUMN})
print("待验证:", r["pending_validation_tables"], "| 数量一致:", r["count_consistent"])
check("依赖清单3个下游", r["total_consumers"] == 3)
check("清单与影响数量一致", r["count_consistent"] is True)
check("路径版本未漂移", r["path_version"] == version)

# 3. 月汇总验证未结束 -> 演练不可执行
r = call("POST", "/api/rollback/drill/prepare",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("月汇总未验证演练不可执行", r["executable"] is False)
r = call("POST", "/api/rollback/drill/run",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("未就绪执行被 HTTP 拒绝(409)", r.get("_http_error") == 409, str(r.get("_detail")))

# 4. 日汇总/核算通过；月汇总直接通过被拒
for table in (DAILY, ACCOUNTING):
    r = call("POST", "/api/rollback/dependencies/validate",
             {"source_table": SOURCE, "dropped_column": COLUMN,
              "target_table": table, "passed": True})
    check(f"{table} 校验通过", r.get("validation_status") == "验证通过")
r = call("POST", "/api/rollback/dependencies/validate",
         {"source_table": SOURCE, "dropped_column": COLUMN,
          "target_table": MONTHLY, "passed": True})
check("月汇总证据未核验被拒(409)", r.get("_http_error") == 409)

# 5. 通用资源路由仍可读取证据模块（新增模块已在 store 注册）
r = call("GET", "/api/rollback_partition_evidence?size=10")
check("证据模块可经通用路由查询", r.get("total") == 3, str(r.get("total")))
# 依赖构建后证据行已固化路径版本
stamped = [it for it in r["items"] if it["目标表"] == DAILY][0]
check("证据行固化该轮路径版本", stamped.get("path_version") == version,
      str(stamped.get("path_version")))

# 5b. 通过通用动作接口核验月汇总证据（模拟月汇总验证结束）
r = call("POST", "/api/rollback_partition_evidence/2/actions",
         {"action": "确认核验"})
check("月汇总证据动作核验成功", r.get("ok") is True and r["entry"]["status"] == "已核验",
      str(r.get("entry", {}).get("status")))

# 5c. 重建依赖（不重新评估影响）；证据变更后验证状态重置，三个下游重新校验
r = call("POST", "/api/rollback/dependencies/build",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("重建依赖路径版本仍为首轮", r["path_version"] == version)
for table in (DAILY, ACCOUNTING, MONTHLY):
    r = call("POST", "/api/rollback/dependencies/validate",
             {"source_table": SOURCE, "dropped_column": COLUMN,
              "target_table": table, "passed": True})
    check(f"{table} 校验通过", r.get("validation_status") == "验证通过", str(r)[:120])
r = call("GET", f"/api/rollback/dependencies/{SOURCE}/{COLUMN}")
check("全部下游验证完成", r["all_validated"] is True)

# 6. 不存在的影响评估返回 404
r = call("GET", "/api/rollback/impact/dw.unknown/missing_col")
check("未评估的删列返回404", r.get("_http_error") == 404)

# 7. 完整演练生命周期：准备 -> 失败 -> 恢复 -> 重跑成功
r = call("POST", "/api/rollback/drill/prepare",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("全部验证后演练可执行", r["executable"] is True)
check("可回滚分区为三方交集",
      r["drill"]["recoverable_partitions"] == ["2026-11-19", "2026-11-20"])
drill_id = r["drill"]["drill_id"]

r = call("POST", "/api/rollback/drill/run",
         {"source_table": SOURCE, "dropped_column": COLUMN,
          "simulate_failure": True, "failure_reason": "模拟月汇总取数失败"})
check("演练失败", r["drill"]["status"] == "演练失败")
check("失败演练锁定路径版本", r["drill"]["path_version"] == version)

r = call("POST", "/api/rollback/drill/recover",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("失败恢复成功且证据版本保持",
      r.get("recovered") is True and r["evidence_path_version_preserved"] is True)

r = call("POST", "/api/rollback/drill/run",
         {"source_table": SOURCE, "dropped_column": COLUMN})
check("恢复后重跑演练成功", r["drill"]["status"] == "演练成功")
check("重跑沿用同轮路径版本", r["drill"]["path_version"] == version)

r = call("GET", f"/api/rollback/drill/{SOURCE}/{COLUMN}")
check("演练状态查询一致", r["drill_id"] == drill_id and r["status"] == "演练成功")
check("演练历史可核查", len(r["history"]) >= 4, f"history={len(r['history'])}")

print()
if failures:
    print(f"HTTP 核查失败 {len(failures)} 项: {failures}")
    raise SystemExit(1)
print("HTTP 核查全部通过 ✓")
