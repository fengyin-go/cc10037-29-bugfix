"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.pipeline_def import PipelineDefService
from app.services.task_node import TaskNodeService
from app.services.quality_rule import QualityRuleService
from app.services.validation_run import ValidationRunService
from app.services.depend_scheduler import DependSchedulerService
from app.services.sliding_window import SlidingWindowService
from app.services.rerun_compensation import RerunCompensationService
from app.services.watermark_tracker import WatermarkTrackerService
from app.services.resource_pool import ResourcePoolService
from app.services.lineage_graph import LineageGraphService
from app.services.exception_capture import ExceptionCaptureService
from app.services.template_store import TemplateStoreService
from app.services.partition_scan import PartitionScanService
from app.services.config_migration import ConfigMigrationService
from app.services.schema_evolution import SchemaEvolutionService
from app.services.backfill_tracker import BackfillTrackerService
from app.services.cost_analysis import CostAnalysisService
from app.services.alert_policy import AlertPolicyService
from app.services.rollback_partition_evidence import RollbackPartitionEvidenceService

SERVICES: dict[str, BaseModuleService] = {
    "pipeline_def": PipelineDefService(),
    "task_node": TaskNodeService(),
    "quality_rule": QualityRuleService(),
    "validation_run": ValidationRunService(),
    "depend_scheduler": DependSchedulerService(),
    "sliding_window": SlidingWindowService(),
    "rerun_compensation": RerunCompensationService(),
    "watermark_tracker": WatermarkTrackerService(),
    "resource_pool": ResourcePoolService(),
    "lineage_graph": LineageGraphService(),
    "exception_capture": ExceptionCaptureService(),
    "template_store": TemplateStoreService(),
    "partition_scan": PartitionScanService(),
    "config_migration": ConfigMigrationService(),
    "schema_evolution": SchemaEvolutionService(),
    "backfill_tracker": BackfillTrackerService(),
    "cost_analysis": CostAnalysisService(),
    "alert_policy": AlertPolicyService(),
    "rollback_partition_evidence": RollbackPartitionEvidenceService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
