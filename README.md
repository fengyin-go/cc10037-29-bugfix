# ETL编排与数据质量中心

面向数据仓库ETL作业场景，提供流水线编排、质量规则、依赖调度和回刷补偿的ETL治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              19个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流水线定义 | `pipeline_def` | ETL流水线 | 流水线名称、处理阶段、输入源表 |
| 任务节点 | `task_node` | 处理节点 | 节点名称、所属管线、节点类型 |
| 质量规则 | `quality_rule` | 校验规则 | 规则名称、目标表、校验字段 |
| 质量执行 | `validation_run` | 校验批次 | 批次标识、目标表、校验分区 |
| 依赖调度 | `depend_scheduler` | 调度配置 | 配置名称、目标管线、触发方式 |
| 滑动窗口 | `sliding_window` | 窗口配置 | 窗口名称、目标表、窗口字段 |
| 回刷补偿 | `rerun_compensation` | 重跑任务 | 任务名称、目标管线、重跑范围 |
| 水位追踪 | `watermark_tracker` | 水位记录 | 管线标识、分区字段、当前水位 |
| 资源池 | `resource_pool` | 计算资源 | 池名称、总核数、总内存 |
| 血缘图谱 | `lineage_graph` | 血缘边 | 源节点、目标节点、传递字段 |
| 异常捕获 | `exception_capture` | 异常记录 | 发生时间、所属管线、异常类型 |
| 模板仓库 | `template_store` | 处理模板 | 模板名称、处理类型、输入Schema |
| 分区扫描 | `partition_scan` | 扫描报告 | 报告时间、目标表、分区总数 |
| 配置迁移 | `config_migration` | 迁移任务 | 任务名称、源环境、目标环境 |
| Schema演进 | `schema_evolution` | 变更记录 | 变更时间、目标表、变更类型 |
| 回填追踪 | `backfill_tracker` | 回填进度 | 追踪ID、回填任务、当前分区 |
| 成本分析 | `cost_analysis` | 成本快照 | 快照日期、管线标识、计算时长 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控对象、告警条件 |
| 回滚分区证据 | `rollback_partition_evidence` | 回滚分区证据 | 目标表、旧列名、有效分区清单、证据核验、path_version |

## 删列回滚演练

事实表删除旧金额列时，影响评估沿血缘边的实际消费方向（源节点 → 目标节点）
逐跳遍历，覆盖事实表 → 日汇总 → 月汇总的传递链路与独立消费分支，共享目标只
纳入一次；每个真实下游的回滚依赖都带旧列映射、有效分区证据和路径版本。

流程接口（前缀 `/api/rollback`）：

1. `POST /impact/evaluate`：评估删列传递影响，固化 `path_version`
2. `POST /dependencies/build`：配置迁移读取回滚依赖，校验每个真实下游能否按旧列和有效分区恢复
3. `POST /dependencies/validate`：逐下游确认回滚就绪（月汇总验证未结束时演练不可执行）
4. `POST /drill/prepare`：建立演练输入，可回滚分区取各消费者有效分区的交集
5. `POST /drill/run`：执行演练（`simulate_failure` 可模拟失败）
6. `POST /drill/recover`：失败恢复，旧列映射与分区证据仍锁定该轮 `path_version`

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
