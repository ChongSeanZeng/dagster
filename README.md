# FSC 现代数据栈（Dagster + dlt + dbt + DuckLake）

基于 **DuckLake** 湖仓的 medallion（Bronze / Silver / Gold）分层数据平台：

- **编排**：[Dagster](https://dagster.io/) —— 目前全部为**手动触发**资产，暂无定时调度。
- **抽取（Bronze）**：**Salesforce** 直接用 `duckdb` 写入 DuckLake（无 dlt，见 `sf_bronze.py`）；**Dataverse** 仍用 [dlt](https://dlthub.com/)（见 `dataverse_bronze.py`）。两者都增量加载**全部列**到同一个 DuckLake `bronze`，按主键 `merge`。
- **运行要求**：`duckdb >= 1.5`（catalog 已是 DuckLake 1.0 格式，1.4.x 的 ducklake 扩展只支持到 0.3，attach 会报 `Only DuckLake versions ...0.3 are supported`）。
- **转换（Silver / Gold）**：[dbt](https://www.getdbt.com/) —— Silver 只保留**指定列**、去重、去删除；Gold 定期**快照**常见统计指标。
- **对外发布**：Silver / Gold 以 **Parquet** 发布到 Azure Blob `duck` 容器，供其他应用直接消费。

> ✅ **路径全部为相对路径**：各 Python 模块以「自身所在目录」为项目根（`os.path.dirname(os.path.abspath(__file__))`），dbt 用相对路径（相对 `dbt/` 目录）。因此**同一份代码在 Linux VM 与本地 Windows 上都能直接运行、无需改任何路径**。生产部署在 Linux VM，本地开发/测试可直接在 Windows 上跑（见 [本地测试（Windows）](#本地测试windows)）。

---

## 目录

- [架构与数据流](#架构与数据流)
- [目录结构](#目录结构)
- [部署到 VM](#部署到-vm)
- [本地测试（Windows）](#本地测试windows)
- [配置](#配置)
- [使用方式](#使用方式)
  - [方式一：Dagster UI（推荐）](#方式一dagster-ui推荐)
  - [方式二：命令行直接运行](#方式二命令行直接运行)
- [硬删除的「id 拉齐」流程](#硬删除的id-拉齐流程)
- [从外部查询 DuckLake](#从外部查询-ducklake)
- [新增实体 / 新增列](#新增实体--新增列)
- [注意事项与排错](#注意事项与排错)

---

## 架构与数据流

```
     Salesforce ─┐                          ┌── silver（dbt，指定列/去重/去删除）
                 ├─ dlt ─▶ DuckLake bronze ─┤
      Dataverse ─┘         （全部列、merge） └── gold（dbt，快照统计）
                                │                        │
                    *_id_sync（拉全部存活 Id，          │
                     供 silver 反连接删除硬删记录）     │
                                                         ▼
                                       publish.py ─▶ Azure Blob 容器 `duck`
                                         duck/silver/*.parquet
                                         duck/gold/*.parquet
```

| 层 | 工具 | 位置 | 说明 |
|----|------|------|------|
| Bronze | Salesforce: `duckdb` 直写；Dataverse: dlt | DuckLake schema `bronze` | 拉取全部列，按主键 merge。**首次全量走 Bulk 2.0（快）**，**增量走 Bulk 1.0 `queryAll`**（捕获软删除 `IsDeleted`）。两条路径统一 schema：非游标列存文本、游标存 datetime。SF 的增量水位 = bronze 表里已有的 `max(游标)`（无单独状态文件、无 dlt 游标）。 |
| Silver | dbt | DuckLake schema `silver` | 只保留 `silver_config.py` 指定的业务列；按游标去重取最新；剔除软删除；反连接 `__live_ids` 剔除硬删除；keep-all 分支会去掉 dlt 系统列（仅 Dataverse 有）。 |
| Gold | dbt | DuckLake schema `gold` | 从 silver 聚合常见统计（`daily.py` / `monthly_test.py` 的移植），每次运行**追加**一个带 `snapshot_date` 的快照。 |
| 发布 | duckdb + azure | Blob 容器 `duck` | 把 silver / gold 每张表导出为 Parquet 上传。 |

**关键事实：** DuckLake 的元数据 catalog 现在存在**本机 Postgres**（数据库 `fsc_lake`，由 `catalog.py` 从 `local.setting.json` 的 `catalog_pg` 块读取连接）；实际数据是 `lake_pg/` 下的 Parquet。三个写入/读取方（`sf_bronze.py` 的 duckdb 直写、`dataverse_bronze.py` 的 dlt、dbt 的 `attach`）**必须指向同一个 Postgres catalog**，否则 silver 读不到 bronze。Postgres 后端支持**并发写入**（这正是启用 Dagster 并发管道的前提）。

---

## 目录结构

```
dagster/
├── definitions.py           # Dagster 资产定义（bronze / id_sync / silver / gold）
├── workspace.yaml           # Dagster 加载入口（指向 definitions.py）
├── sf_bronze.py             # Salesforce → bronze（duckdb 直写，无 dlt）+ 硬删除 id 拉齐
├── dataverse_bronze.py      # Dataverse → bronze（dlt）+ 硬删除 id 拉齐
├── silver_config.py         # 每个实体的 silver「指定列」清单
├── gen_silver.py            # 由 silver_config 生成 silver dbt 模型 + _sources.yml
├── publish.py               # 导出 silver/gold 表为 Parquet 并上传到 Blob `duck`
├── daily.py / monthly_test.py  # 旧版参考脚本（Gold 已用 dbt 重写，仅供对照）
├── sf.py                    # 旧版全量脚本（silver 列清单的来源，仅供参考）
├── local.setting.json       # 凭据（不入库，见 .gitignore；含 catalog_pg 块）
├── catalog.py               # DuckLake catalog（Postgres）连接构建器
├── lake_pg/                 # DuckLake 数据文件（Parquet，Postgres catalog，不入库）
├── publish/                 # 发布前的本地 Parquet 暂存（不入库）
├── home/                    # DAGSTER_HOME（运行历史/日志，含 dagster.yaml，不入库）
├── dagster.service          # systemd 服务单元（dagster dev）
├── requirements.txt
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml         # 连接 DuckLake（ducklake:postgres:...，经 DUCKLAKE_CATALOG）
    ├── seeds/regions.csv    # ISO3 → 区域映射（members 模型用）
    ├── macros/
    │   ├── silver_dedupe.sql    # 通用 silver 变换（去重/去删/选列）
    │   ├── gold_helpers.sql     # snapshot_date、账户类型等公共宏
    │   └── generate_schema_name.sql
    └── models/
        ├── silver/*.sql         # 由 gen_silver.py 生成
        └── gold/*.sql           # 快照统计模型
```

---

## 部署到 VM

以下命令均在 **Linux VM** 上执行，工作目录约定为 `/home/ubuntu/dagster`。

### 1. 获取代码

```bash
cd /home/ubuntu
git clone <your-repo-url> dagster
cd dagster
```

> 项目根目录可以放在任意位置——`BASE` 由各模块的 `__file__` 自动推断，dbt 用相对路径，无需改任何路径常量。

### 2. 创建虚拟环境并安装依赖

> ⚠️ 用 **Python 3.11 / 3.12**（dbt-core 不支持 3.13/3.14）。

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python --version                 # 应显示 3.12.x
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. 准备凭据

创建 `local.setting.json`（**不要入库**，已在 `.gitignore` 中）：

```json
{
  "password": "<Salesforce 密码>",
  "security_token": "<Salesforce security token>",
  "key1": "<fscdatastore 账户 key，旧版脚本用>",
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=synapsedeltalake2022;AccountKey=<key>;EndpointSuffix=core.windows.net;",
  "dataverse": {
    "client_id": "<Azure AD 应用 client id>",
    "client_secret": "<Azure AD 应用 client secret>"
  },
  "catalog_pg": {
    "host": "localhost",
    "port": 5432,
    "dbname": "fsc_lake",
    "user": "fsc",
    "password": "<catalog 角色密码>"
  }
}
```

- `catalog_pg` 是 **DuckLake catalog 的 Postgres 连接**（元数据后端）。用超级用户建一次专用库/角色即可：
  `CREATE ROLE fsc LOGIN PASSWORD '<pw>'; CREATE DATABASE fsc_lake OWNER fsc;`（`catalog.py` 从这里读取，构建 duckdb/dlt/dbt 三种连接形式）。数据文件仍是本地 Parquet（`./lake_pg`）。
- `connection_string` 用于把 Parquet 发布到 `synapsedeltalake2022` 账户的 `duck` 容器。
- `dataverse.client_id` / `dataverse.client_secret` 是 Dataverse 的 Azure AD 应用凭据；**必须放在这里**（本文件已被 `.gitignore` 排除），不要写进 `dataverse_bronze.py`——GitHub 的 secret 扫描会拦截含明文密钥的提交。租户 `authority_url` 与 `resource` 有非机密默认值，如需可在此 `dataverse` 块中覆盖。

> 该文件是一个 **Python dict 字面量**（用 `eval` 解析，`connection_string` 用单引号），不是严格 JSON——照抄格式即可。

### 4. 设置 Dagster 主目录

```bash
export DAGSTER_HOME=$(pwd)/home        # 指向项目内的 home/ 目录
```

建议把这行加入 `~/.bashrc` 或 systemd 服务的环境变量。`DAGSTER_HOME` 必须是**绝对路径**（Dagster 要求）；用 `$(pwd)/home` 即可从当前项目根动态得到，不必写死。若不设置，Dagster 会用临时目录（运行历史不持久，本地测试可接受）。

### 5. 首次校验 catalog 连通（重要）

先只跑一个小实体，确认 bronze 写入与 dbt 读取共用同一个 Postgres catalog：

```bash
python -c "import sf_bronze; print(sf_bronze.run_bronze(entities=['RecordType']))"
cd dbt && dbt run --select silver.recordtype --profiles-dir . && cd ..
```

若 `lake.silver.recordtype` 有数据、且只有 `Id, Name` 两列，说明打通成功。

> 连接由 `catalog.py` 从 `local.setting.json` 的 `catalog_pg` 块（host/port/dbname/user/password）构建，三方指向同一个 Postgres 库 `fsc_lake`。若 attach 报 `Only DuckLake versions ...0.3 are supported`，是 `duckdb < 1.5`：`pip install "duckdb>=1.5"`。手动跑 dbt 前需 `export DUCKLAKE_CATALOG="$(python -c 'import catalog;print(catalog.dbt_env()["DUCKLAKE_CATALOG"])')"`。

### 6. 启动 Dagster

前台快速启动：

```bash
dagster dev -h 0.0.0.0 -p 3000
```

浏览器打开 `http://<vm-ip>:3000`。因为目前是手动触发、无定时调度，`dagster dev` 已足够。

#### 作为服务长期运行（systemd）

项目根目录提供了现成的服务单元 **`dagster.service`**（启动 `dagster dev`，含 webserver + daemon）。安装一次：

```bash
sudo cp dagster.service /etc/systemd/system/dagster.service
sudo systemctl daemon-reload
sudo systemctl enable --now dagster.service     # 开机自启 + 立即启动
```

常用管理：

```bash
systemctl status dagster          # 健康状态
journalctl -u dagster -f          # 实时日志
sudo systemctl restart dagster    # 改了 definitions.py / 配置后重启
```

> 单元文件里写死了 `WorkingDirectory=/home/ubuntu/dagster`、`DAGSTER_HOME`、`PATH`（含 venv/bin，保证 silver/gold 资产里 `dbt` 子进程能被找到）。路径不同请自行修改。

#### 实例配置 `home/dagster.yaml`（DAGSTER_HOME）

Dagster 的实例配置在 `home/dagster.yaml`（即 `DAGSTER_HOME`）。除自动生成的存储路径外，已加入：

- `run_coordinator: QueuedRunCoordinator` + `max_concurrent_runs: 4` —— catalog 迁到 Postgres（支持并发写）后，跨 run 并发上限设为 4（run 内并行由 `definitions.py` 的 `multiprocess_executor` 控制）。由 `dagster dev` 启动的 daemon 负责出队。机器吃紧就调小。
- `telemetry.enabled: false` —— 关闭匿名遥测。

---

## 本地测试（Windows）

代码路径全部相对/动态，Windows 上可跑一套**独立的本地湖仓**（数据文件 `lake_pg/`、`publish/` 都生成在项目目录内）用于调试。**除了首次装环境，其余全部在 Dagster UI 里点按钮完成，无需命令行。**

> ⚠️ **catalog 现在是 Postgres**：本地也需要一个可连的 Postgres，并在 `local.setting.json` 的 `catalog_pg` 块填好连接（本机装一个 Postgres，建 `fsc_lake` 库/`fsc` 角色即可）。不再有 SQLite 单文件 catalog。

> ⚠️ **Python 版本**：用 **3.11 / 3.12**。dbt-core 不支持 3.13/3.14——在 3.14 上 `import dbt` 会因 `mashumaro` 直接报错（`UnserializableField: Field "schema" ...`）。dlt/bronze 在 3.14 能跑，dbt 不行。

**① 一次性装环境并启动 UI（命令行）**

```powershell
# 在项目根目录（PowerShell），用 Python 3.12
py -3.12 -m venv .venv          # 或 python3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version                # 应显示 3.12.x
pip install -r requirements.txt

# 准备 local.setting.json（同 VM，见「配置」）
# DAGSTER_HOME 必须绝对路径；用当前目录动态生成
$env:DAGSTER_HOME = "$PWD\home"

# 启动 UI（从项目根运行），浏览器打开 http://localhost:3000
dagster dev
```

**② 之后全部在 UI 里点按钮**（Assets 页面，每个资产右上角 **Materialize**；带 ▸ 的资产可在 Launchpad 里改 Config）：

1. **Bronze**：点 `salesforce_bronze` → **Materialize**。想只测一个实体，在 Launchpad 里填：
   ```yaml
   ops:
     salesforce_bronze:
       config:
         entities: ["RecordType"]   # 留空=全部 15 个实体
         # seed_since_days: 7        # 可选：只回填最近 N 天，测大表时很有用
   ```
2. **Silver**：点 `silver` → **Materialize**。想只测一个模型、且不上传 blob：
   ```yaml
   ops:
     silver:
       config:
         select: "silver.recordtype"  # 留空/默认=whole silver 层
         publish: false               # 本地测试关掉上传；默认 true
   ```
3. **Gold**：点 `gold` → **Materialize**（`config` 可设 `snapshot_date`、`publish` 等，见「使用方式」）。

要点：

- **Bronze 必须先于 dbt(silver/gold) 运行**。DuckLake **新建** catalog 时需要 `DATA_PATH`；Dataverse 的 dlt 由 `STORAGE` 提供，能建新 catalog。`sf_bronze.py` 与 dbt 都只「附加已存在的 catalog」（不带 `DATA_PATH`，路径从 catalog 元数据读），所以**全新机器上先跑一次 `dataverse_bronze`（或已存在 catalog）**，再跑 SF/silver。UI 里 `silver` 对 bronze 有依赖连线，先点 bronze 再点 silver 即可。
- **catalog（Postgres）与 `lake_pg/` 必须在同一台机器**：数据文件的绝对路径记录在 catalog 元数据里（`file://.../lake_pg`）。换机器不能直接搬数据——重新跑一遍 bronze 即可。
- 已在 **Windows 3.12** 上端到端验证：`salesforce_bronze`（config `entities:["RecordType"]`）→ `silver`（`select:"silver.recordtype"`）全部 `RUN_SUCCESS`，silver 产物只有 `Id,Name`、46 行、无 dlt 列。
- 需要联网让 DuckDB 首次 `INSTALL ducklake/postgres/azure` 下载扩展；catalog 需本机 Postgres 可达；Bronze 抽取仍需 Salesforce/Dataverse 凭据。纯 dbt/publish/查询逻辑可离线在本地湖仓上调试。

---

## 配置

### `dbt/profiles.yml`

```yaml
fsc_lake:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dbt.duckdb                     # 相对 dbt/ 目录；仅作 DuckDB 会话锚点
      threads: 4                            # Postgres catalog 支持并发写，dbt 可并行建模
      extensions: [postgres, ducklake]
      attach:
        # Postgres catalog；完整连接串（含密码）由 DUCKLAKE_CATALOG 环境变量注入，
        # 不落进本文件。Dagster 自动注入；手动 dbt 前先 export（见上文 catalog 说明）。
        - path: "ducklake:{{ env_var('DUCKLAKE_CATALOG') }}"
          alias: lake                       # 模型里通过 lake.silver.* / lake.gold.* 访问
```

> 路径是**相对 dbt/ 目录**的，因此 dbt 必须从 `dbt/` 目录运行（Dagster 的 `silver`/`gold` 资产已用 `cwd=dbt/`；手动运行请先 `cd dbt`）。Linux/Windows 通用，无需改路径。

### 实体清单

- **Salesforce**：编辑 `sf_bronze.py` 的 `ENTITIES`（约 15 个 API 名称）。
- **Dataverse**：编辑 `dataverse_bronze.py` 的 `ENTITIES`（`entity_set: primary_key`）。

### Silver 列清单

编辑 `silver_config.py` 的 `SILVER_COLUMNS`（来源于旧 `sf.py` 的 `QUERIES`，并为 Gold 需要的字段做了扩展）。改完后需重新生成模型（见下）。

---

## 使用方式

### 方式一：Dagster UI（推荐，全程点按钮）

在 Dagster UI 的 **Assets** 页面手动 **Materialize**；每个资产都可在 Launchpad 里改 **Config**：

| 资产 | 分组 | 作用 | Launchpad 可配置项（Config） |
|------|------|------|------|
| `salesforce_bronze` | bronze | 增量加载 Salesforce → bronze | `entities`（子集，留空=全部）、`seed_since_days`（首次回填天数） |
| `dataverse_bronze` | bronze | 增量加载 Dataverse → bronze | 同上 |
| `salesforce_id_sync` | bronze | **手动**：Salesforce 存活 Id（硬删除拉齐） | `entities`（子集，留空=全部） |
| `dataverse_id_sync` | bronze | **手动**：Dataverse 存活 Id（硬删除拉齐） | `entities` |
| `silver` | silver | 重新生成模型 → `dbt seed` → `dbt run` silver → 发布 Parquet | `select`（默认 `silver`，可 `silver.<模型>`）、`publish`（默认 true） |
| `gold` | gold | `dbt run` gold（带 `snapshot_date`）→ 发布 Parquet | `select`（默认 `gold`）、`snapshot_date`（留空=今天）、`publish` |

Config 示例（Launchpad 里粘贴 YAML）：

```yaml
ops:
  salesforce_bronze:
    config:
      entities: ["Account", "Certificate_c__c"]   # 只加载这两个；留空=全部
      seed_since_days: 30                          # 首次只回填最近 30 天
```

典型流程：先 `salesforce_bronze` / `dataverse_bronze`，再 `silver`，最后 `gold`。硬删除拉齐按需单独触发（见下节）。本地测试可在 `silver` 的 Config 里设 `publish: false` 关掉上传。

### 方式二：命令行直接运行（可选，用于脚本 / VM 自动化）

日常操作用上面的 UI 即可。如需脚本化，在项目根目录、已激活虚拟环境、已设 `DAGSTER_HOME` 的前提下：

```bash
# Bronze —— 增量加载（不带参数=全部实体；也可指定单个实体）
python -c "import sf_bronze; print(sf_bronze.run_bronze())"
python -c "import sf_bronze; print(sf_bronze.run_bronze(entities=['Account']))"
python -c "import dataverse_bronze as d; print(d.run_bronze())"

# 硬删除「id 拉齐」（手动，按需）
python -c "import sf_bronze; print(sf_bronze.run_id_reconcile())"
python -c "import dataverse_bronze as d; print(d.run_id_reconcile())"

# Silver —— 生成模型 + 构建
python gen_silver.py
cd dbt
dbt seed --profiles-dir .
dbt run --select silver --profiles-dir .
cd ..

# Gold —— 指定快照日期
cd dbt
dbt run --select gold --vars 'snapshot_date: 2026-07-14' --profiles-dir .
cd ..

# 发布 Parquet 到 Blob 容器 duck/
python -c "import publish; print(publish.publish_silver()); print(publish.publish_gold())"
```

---

## 硬删除的「id 拉齐」流程

- **软删除**（Salesforce `IsDeleted=true`）：bronze 用 `queryAll` 已捕获，silver 自动过滤，无需额外操作。
- **硬删除**（记录被物理删除）：增量拉不到，需要「id 拉齐」——拉取数据源当前**全部存活 Id** 写入 `bronze.<entity>__live_ids`，silver 通过反连接 `Id in (select Id from ..._live_ids)` 剔除。

推荐顺序：

```bash
# 1) 正常增量
python -c "import sf_bronze; sf_bronze.run_bronze()"
# 2) 拉齐存活 Id（快照）
python -c "import sf_bronze; sf_bronze.run_id_reconcile()"
# 3) 重建 silver，硬删记录被剔除
cd dbt && dbt run --select silver --profiles-dir . && cd ..
```

> 注意：`__live_ids` 只在手动拉齐时刷新。若在「拉齐」与「重建 silver」之间源端**新增**了记录，这些新记录可能被暂时剔除，直到下次拉齐。因此请把拉齐与 silver 重建**连续执行**。首次尚未拉齐时，silver 会自动跳过反连接（不会报错）。

---

## 从外部查询 DuckLake

有两种消费方式，**推荐第一种**（对外应用无需接触 VM 上的 catalog / 文件）。

### 方式 A：直接读 Blob 上发布的 Parquet（推荐）

发布后，容器 `duck` 中有：

```
duck/silver/<表名>.parquet      # 如 account、certificate_c、country_data ...
duck/gold/<表名>.parquet        # 如 members、coc_per_country、baseline_certification ...
```

用 DuckDB 直接读（任意机器，装好 DuckDB 即可）：

```sql
INSTALL azure; LOAD azure;

-- 用与 local.setting.json 相同的连接串创建 secret
CREATE SECRET az (
    TYPE azure,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=synapsedeltalake2022;AccountKey=<key>;EndpointSuffix=core.windows.net;'
);

-- 查询单表
SELECT * FROM read_parquet('az://duck/gold/members.parquet') LIMIT 100;

-- 查看某统计的历史快照
SELECT snapshot_date, country, certificates
FROM read_parquet('az://duck/gold/coc_per_country.parquet')
ORDER BY snapshot_date DESC, certificates DESC;
```

Python（polars / pandas）也可以直接读：

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")
con.execute("CREATE SECRET az (TYPE azure, CONNECTION_STRING '<connection_string>')")
df = con.execute("SELECT * FROM read_parquet('az://duck/silver/account.parquet')").pl()
```

> 也可以先把 blob 里的 parquet 下载到本地再用任意工具读取；这些 Parquet 是自包含的，不依赖 catalog。

### 方式 B：直接连 DuckLake catalog（需能访问 VM 上的 Postgres 与 lake_pg/ 目录）

适合在 VM 上直接查询。DuckLake 元数据在 Postgres 库 `fsc_lake`，数据文件路径记录在元数据里（`file://.../lake_pg`），因此 **Postgres 可达且 lake_pg/ 路径一致**。用 `catalog.py` 生成连接串免得手写密码：

```bash
python -c "import duckdb, catalog as k; c=duckdb.connect(); \
  c.execute('INSTALL postgres;LOAD postgres;INSTALL ducklake;LOAD ducklake;'); \
  c.execute(f\"ATTACH '{k.duckdb_attach_target()}' AS lake\"); \
  print(c.execute('SELECT * FROM lake.gold.fm_per_country LIMIT 20').fetchall())"
```

或在 SQL 里手动 attach（把 `<...>` 换成 `local.setting.json` 的 `catalog_pg`）：

```sql
INSTALL postgres; LOAD postgres;
INSTALL ducklake; LOAD ducklake;

ATTACH 'ducklake:postgres:dbname=fsc_lake host=localhost port=5432 user=fsc password=<pw>' AS lake;

-- 列出所有表
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'lake'
ORDER BY 1, 2;

-- 查询 silver / gold
SELECT * FROM lake.silver.account LIMIT 100;
SELECT * FROM lake.gold.members WHERE snapshot_date = (SELECT max(snapshot_date) FROM lake.gold.members);
```

命令行：`duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres; ATTACH 'ducklake:postgres:dbname=fsc_lake host=localhost user=fsc password=<pw>' AS lake; SELECT * FROM lake.gold.fm_per_country LIMIT 20;"`

---

## 新增实体 / 新增列

**新增一个 Salesforce 实体：**

1. 在 `sf_bronze.py` 的 `ENTITIES` 追加其 API 名称（自动拉全部列）。
2. 在 `silver_config.py` 的 `SILVER_COLUMNS` 增加该实体（bronze 表名=API 名小写）的列清单。
3. `python gen_silver.py` 重新生成 silver 模型与 `_sources.yml`。
4. 跑 `salesforce_bronze` → `silver`（Dagster 的 `silver` 资产已内置第 3 步）。

**给已有实体新增列：** 在 `silver_config.py` 里加列名 → `python gen_silver.py` → 重建 silver。（bronze 本就拉了全部列，无需改抽取。）

**自定义某个 silver 模型：** 把模型名加入 `gen_silver.py` 的 `SKIP` 集合，然后手写该 `.sql`，生成器不再覆盖它。

---

## 注意事项与排错

- **路径可移植**：项目根由各模块 `__file__` 自动推断，dbt 用相对路径，Linux VM 与本地 Windows 通用，无需改路径。注意 catalog（Postgres）与 `lake_pg/` 是同机绑定的（元数据里是绝对路径），跨机器不能直接搬——换机器重跑 bronze 即可。
- **catalog 是 Postgres**：元数据在本机 Postgres 库 `fsc_lake`，连接由 `catalog.py` 从 `local.setting.json` 的 `catalog_pg` 块构建。三方各自的连接形式不同但指向同一个库：SF/publish 用 duckdb `ATTACH 'ducklake:postgres:...'`（`catalog.duckdb_attach_target()`）、Dataverse 的 dlt 用 URL `postgresql://...`（`catalog.dlt_catalog()`）、dbt 用 `profiles.yml` 里的 `{{ env_var('DUCKLAKE_CATALOG') }}`（Dagster 由 `catalog.dbt_env()` 注入；手动 `dbt` 前需 `export DUCKLAKE_CATALOG=...`）。**需 `duckdb >= 1.5`**（DuckLake 1.0 格式）。
- **列名大小写**：silver 用带引号的精确列名投影。若某列在 bronze 中的大小写与 `silver_config.py` 不一致（例如 `policy_omit_trademark_symbols__c`），silver 构建会报「列不存在」。排查：`DESCRIBE lake.bronze.<表名>;` 核对真实列名并同步到配置。
- **DuckLake 并发（Postgres 后端）**：catalog 迁到 Postgres 后支持并发写入，因此已开启并发：
  - `definitions.py` 用 `multiprocess_executor.configured({"max_concurrent": 4})`——一个 run 内的资产可并行（如 `salesforce_bronze` 与 `dataverse_bronze` 同时写不同表）。
  - `home/dagster.yaml` 的 `QueuedRunCoordinator max_concurrent_runs: 4`——跨 run 并发上限。
  - dbt `profiles.yml` `threads: 4`——dbt 并行构建独立模型。
  - 注意：**不同表**并发写安全；**同一张表**并发提交走乐观并发，冲突会重试（dbt/dlt 的 DAG 一般不会让两个进程写同一张表）。机器吃紧就把这几个数调小。
- **Gold 快照幂等**：同一 `snapshot_date` 重复运行 gold 不会重复追加（模型内有去重保护）；换一天则追加新快照。
- **Gold 数值字段**：源自 Salesforce 的数值/日期在 bronze 里可能是字符串，gold 用 `try_cast` 容错，无法解析的值会变成 `NULL` 并在过滤中被排除。
- **旧版脚本**：`daily.py` / `monthly_test.py` 仅作为 Gold 逻辑的对照参考，新栈不再依赖它们（`monthly_test.py` 第 169–171 行有历史遗留的语法问题，已在 dbt 移植中规避）。
