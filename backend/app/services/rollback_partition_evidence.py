"""回滚分区证据领域服务：管理删列回滚所需的旧列映射与有效分区证据。

上游：partition_scan 分区扫描核验 → rollback_partition_evidence 登记证据
消费：config_migration 的回滚演练读取证据，校验每个真实下游能否按旧列
和有效分区恢复。证据一旦核验会带上演练轮次的 path_version，演练失败
恢复后仍沿用该轮版本。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RollbackPartitionEvidenceService(BaseModuleService):
    MODULE = "rollback_partition_evidence"
    ENTITY = "回滚分区证据"
    OPERATION_TERM = "核验"
    LIST_FIELDS = [
        "目标表", "旧列名", "分区字段", "最早可用分区", "当前水位分区",
        "有效分区清单", "存量行数", "证据位置", "证据核验", "path_version",
    ]
    REQUIRED_FIELDS = ["目标表", "旧列名", "有效分区清单"]
    STATUS_ORDER = ["核验中", "已核验"]
    ACTION_RULES = {"确认核验": "已核验"}
    NEGATIVE_ACTIONS = []

    def query_by_table(self, table_name: str) -> list[dict[str, Any]]:
        """按下游目标表查询回滚分区证据。"""
        return [
            dict(row)
            for row in store.rows(self.MODULE)
            if table_name.casefold() in str(row.get("目标表", "")).casefold()
        ]

    def verified_evidence(self) -> list[dict[str, Any]]:
        """返回已核验证据，供回滚演练就绪判断消费。"""
        return [
            dict(row)
            for row in store.rows(self.MODULE)
            if str(row.get("证据核验", "否")) == "是"
        ]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "确认核验":
            entry["证据核验"] = "是"
            entry["pending"] = False
            entry["last_operated_at"] = datetime.now().isoformat()
        return entry, msg
