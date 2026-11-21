"""路径版本锁定语义核查（需在 http_rollback_flow.py 成功后执行）。

验证题设关键约束：演练失败恢复后，旧列映射和数据分区证据仍采用该轮
路径版本；即使之后重新评估了删列影响（产生新轮次），已建立演练的
证据也不允许漂移到新版本。
"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:8011"
SOURCE = "dw.fact_trade_order"
COLUMN = "legacy_order_amount"

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


# 前置：上一轮流程已把演练锁定在某个路径版本
drill_before = call("GET", f"/api/rollback/drill/{SOURCE}/{COLUMN}")
if drill_before.get("_http_error") == 404:
    raise SystemExit("请先在干净服务上运行 http_rollback_flow.py 建立演练")
locked_version = drill_before["path_version"]
print("已锁定路径版本:", locked_version, "| 演练状态:", drill_before["status"])
check("前置演练成功", drill_before["status"] == "演练成功")

# 1. 重新评估删列影响 -> 产生新轮次
new_impact = call("POST", "/api/rollback/impact/evaluate",
                  {"source_table": SOURCE, "dropped_column": COLUMN})
new_version = new_impact["impact"]["path_version"]
print("重新评估后新版本:", new_version)
check("重新评估产生新轮次", new_version != locked_version)

# 2. 已建立演练的证据仍锁定旧版本，不允许漂移
evidence = call("GET", "/api/rollback_partition_evidence?size=10")
versions = {it["path_version"] for it in evidence["items"]}
check("证据行仍保留锁定轮次版本", versions == {locked_version}, str(versions))

# 3. 重建依赖时显式报跨轮次锁定阻断
deps = call("POST", "/api/rollback/dependencies/build",
            {"source_table": SOURCE, "dropped_column": COLUMN})
check("依赖清单仍标注锁定轮次", deps["path_version"] == locked_version)
lock_blocks = [
    reason for dep in deps["dependencies"] for reason in dep["blocking_reasons"]
    if "已锁定" in reason
]
check("每个下游都有跨轮次锁定阻断", len(lock_blocks) >= 3, f"{len(lock_blocks)} 条")
check("新版本轮次下演练不可执行（证据不可漂移）", deps["all_validated"] is False)

# 4. 依赖未就绪时演练输入保留锁定轮次，不采用新版本也不覆盖历史演练
prepared = call("POST", "/api/rollback/drill/prepare",
                {"source_table": SOURCE, "dropped_column": COLUMN})
check("演练输入沿用锁定路径版本",
      prepared["drill"]["path_version"] == locked_version,
      prepared["drill"]["path_version"])
check("演练输入未漂移到新版本",
      prepared["drill"]["path_version"] != new_version)
check("未就绪重建被明确拒绝并保留历史演练", prepared.get("error") is not None,
      prepared.get("error", ""))

# 5. 演练失败 -> 恢复，证据版本全程不漂移
run = call("POST", "/api/rollback/drill/run",
           {"source_table": SOURCE, "dropped_column": COLUMN,
            "simulate_failure": True, "failure_reason": "锁定语义演练失败"})
check("锁定轮次演练可失败", run["drill"]["status"] == "演练失败")
recovered = call("POST", "/api/rollback/drill/recover",
                 {"source_table": SOURCE, "dropped_column": COLUMN})
check("恢复成功且证据版本保持",
      recovered["recovered"] is True
      and recovered["evidence_path_version_preserved"] is True
      and recovered["path_version"] == locked_version)

drill_after = call("GET", f"/api/rollback/drill/{SOURCE}/{COLUMN}")
check("恢复后演练仍锁定同一轮次", drill_after["path_version"] == locked_version)
check("恢复后状态为已恢复", drill_after["status"] == "已恢复")

print()
if failures:
    print(f"锁定语义核查失败 {len(failures)} 项: {failures}")
    raise SystemExit(1)
print("路径版本锁定语义核查全部通过 ✓")
