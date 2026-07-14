# FSC 现代数据栈（Dagster + dlt + dbt + DuckLake）

基于 **DuckLake** 湖仓的 medallion（Bronze / Silver / Gold）分层数据平台：

- **编排**：[Dagster](https://dagster.io/) —— 目前全部为**手动触发**资产，暂无定时调度。
- **抽取（Bronze）**：[dlt](https://dlthub.com/) —— 从 **Salesforce** / **Dataverse** 增量加载**全部列**到 DuckLake `bronze`，按主键 `merge`。
- **转换（Silver / Gold）**：[dbt](https://www.getdbt.com/) —— Silver 只保留**指定列**、去重、去删除；Gold 定期**快照**常见统计指标。
- **对外发布**：Silver / Gold 以 **Parquet** 发布到 Azure Blob `duck` 容器，供其他应用直接消费。

> ⚠️ **本项目在 Linux VM 上运行**（路径全部写死为 `/home/ubuntu/dagster`）。仓库可能被 clone 到其他系统查看，但 **dlt / dbt / dagster 只在 VM 上运行与验证**。

---

## 目录

- [架构与数据流](#架构与数据流)
- [目录结构](#目录结构)
- [部署到 VM](#部署到-vm)
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
| Bronze | dlt | DuckLake schema `bronze` | 拉取全部列，按 `Id` merge。Salesforce 用 `queryAll` 捕获软删除（`IsDeleted`）。 |
| Silver | dbt | DuckLake schema `silver` | 只保留 `silver_config.py` 指定的业务列；按游标去重取最新；剔除软删除；反连接 `__live_ids` 剔除硬删除；去掉 dlt 系统列。 |
| Gold | dbt | DuckLake schema `gold` | 从 silver 聚合常见统计（`daily.py` / `monthly_test.py` 的移植），每次运行**追加**一个带 `snapshot_date` 的快照。 |
| 发布 | duckdb + azure | Blob 容器 `duck` | 把 silver / gold 每张表导出为 Parquet 上传。 |

**关键事实：** DuckLake 的元数据 catalog 是一个 **SQLite** 文件（`lake_catalog.duckdb`，名字有误导性）；实际数据是 `lake/` 下的 Parquet。dlt 与 dbt **必须指向同一个 SQLite catalog**，否则 silver 读不到 bronze。

---

## 目录结构

```
dagster/
├── definitions.py           # Dagster 资产定义（bronze / id_sync / silver / gold）
├── workspace.yaml           # Dagster 加载入口（指向 definitions.py）
├── sf_bronze.py             # Salesforce → bronze（dlt）+ 硬删除 id 拉齐
├── dataverse_bronze.py      # Dataverse → bronze（dlt）+ 硬删除 id 拉齐
├── silver_config.py         # 每个实体的 silver「指定列」清单
├── gen_silver.py            # 由 silver_config 生成 silver dbt 模型 + _sources.yml
├── publish.py               # 导出 silver/gold 表为 Parquet 并上传到 Blob `duck`
├── daily.py / monthly_test.py  # 旧版参考脚本（Gold 已用 dbt 重写，仅供对照）
├── sf.py                    # 旧版全量脚本（silver 列清单的来源，仅供参考）
├── local.setting.json       # 凭据（不入库，见 .gitignore）
├── lake_catalog.duckdb      # DuckLake catalog（SQLite，不入库）
├── lake/                    # DuckLake 数据文件（Parquet，不入库）
├── publish/                 # 发布前的本地 Parquet 暂存（不入库）
├── home/                    # DAGSTER_HOME（运行历史/日志，不入库）
├── requirements.txt
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml         # 连接 DuckLake（ducklake:sqlite:...）
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

> 若仓库根目录不是 `/home/ubuntu/dagster`，需要同步修改各文件中的 `BASE` 常量与 `dbt/profiles.yml` 里的绝对路径（代码中路径是写死的）。

### 2. 创建虚拟环境并安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
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
  }
}
```

- `connection_string` 用于把 Parquet 发布到 `synapsedeltalake2022` 账户的 `duck` 容器。
- `dataverse.client_id` / `dataverse.client_secret` 是 Dataverse 的 Azure AD 应用凭据；**必须放在这里**（本文件已被 `.gitignore` 排除），不要写进 `dataverse_bronze.py`——GitHub 的 secret 扫描会拦截含明文密钥的提交。租户 `authority_url` 与 `resource` 有非机密默认值，如需可在此 `dataverse` 块中覆盖。

> 该文件是一个 **Python dict 字面量**（用 `eval` 解析，`connection_string` 用单引号），不是严格 JSON——照抄格式即可。

### 4. 设置 Dagster 主目录

```bash
export DAGSTER_HOME=/home/ubuntu/dagster/home
```

建议把这行加入 `~/.bashrc` 或 systemd 服务的环境变量。

### 5. 首次校验 catalog 连通（重要）

先只跑一个小实体，确认 dlt 与 dbt 共用同一个 SQLite catalog：

```bash
python -c "import sf_bronze; print(sf_bronze.run_bronze(entities=['RecordType']))"
cd dbt && dbt run --select silver.recordtype --profiles-dir . && cd ..
```

若 `lake.silver.recordtype` 有数据、且只有 `Id, Name` 两列，说明打通成功。

> 若 dlt 的 catalog 连接串报错，请根据 VM 上安装的 dlt 版本核对 `sf_bronze.py` / `dataverse_bronze.py` 中 `CATALOG` 的 scheme（目标是指向 `lake_catalog.duckdb` 这个 **SQLite** 文件，与 `dbt/profiles.yml` 一致）。

### 6. 启动 Dagster

```bash
dagster dev -h 0.0.0.0 -p 3000
```

浏览器打开 `http://<vm-ip>:3000`。因为目前是手动触发、无定时调度，`dagster dev` 已足够；如需长期后台运行，可用 systemd 或 `nohup` 托管，并（若日后加调度）另跑 `dagster-daemon run`。

---

## 配置

### `dbt/profiles.yml`

```yaml
fsc_lake:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /home/ubuntu/dagster/dbt/dbt.duckdb   # 仅作 DuckDB 会话锚点
      threads: 1                                   # DuckLake 的 SQLite catalog 单写，必须串行
      extensions: [sqlite, ducklake]
      attach:
        - path: "ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb"
          alias: lake                              # 模型里通过 lake.silver.* / lake.gold.* 访问
```

### 实体清单

- **Salesforce**：编辑 `sf_bronze.py` 的 `ENTITIES`（约 15 个 API 名称）。
- **Dataverse**：编辑 `dataverse_bronze.py` 的 `ENTITIES`（`entity_set: primary_key`）。

### Silver 列清单

编辑 `silver_config.py` 的 `SILVER_COLUMNS`（来源于旧 `sf.py` 的 `QUERIES`，并为 Gold 需要的字段做了扩展）。改完后需重新生成模型（见下）。

---

## 使用方式

### 方式一：Dagster UI（推荐）

在 Dagster UI 的 **Assets** 页面，按依赖关系手动 **Materialize**：

| 资产 | 分组 | 作用 |
|------|------|------|
| `salesforce_bronze` | bronze | 增量加载 Salesforce → bronze |
| `dataverse_bronze` | bronze | 增量加载 Dataverse → bronze |
| `salesforce_id_sync` | bronze | **手动**：拉取 Salesforce 全部存活 Id（硬删除拉齐） |
| `dataverse_id_sync` | bronze | **手动**：拉取 Dataverse 全部存活 Id（硬删除拉齐） |
| `silver` | silver | 重新生成模型 → `dbt seed` → `dbt run --select silver` → 发布 Parquet |
| `gold` | gold | `dbt run --select gold`（带当天 `snapshot_date`）→ 发布 Parquet |

典型流程：先 `salesforce_bronze` / `dataverse_bronze`，再 `silver`，最后 `gold`。硬删除拉齐按需单独触发（见下节）。

### 方式二：命令行直接运行

在项目根目录、已激活虚拟环境、已 `export DAGSTER_HOME=...` 的前提下：

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

### 方式 B：直接连 DuckLake catalog（需能访问 VM 上的 catalog 与 lake/ 目录）

适合在 VM 上、或把 `lake_catalog.duckdb` 与 `lake/` 目录一并拷出来的场景。DuckLake 的数据文件路径记录在 catalog 元数据里（`file://.../lake`），因此 **catalog 和 lake/ 必须同时可达且路径一致**。

```sql
INSTALL sqlite;   LOAD sqlite;
INSTALL ducklake; LOAD ducklake;

ATTACH 'ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb' AS lake;

-- 列出所有表
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'lake'
ORDER BY 1, 2;

-- 查询 silver / gold
SELECT * FROM lake.silver.account LIMIT 100;
SELECT * FROM lake.gold.members WHERE snapshot_date = (SELECT max(snapshot_date) FROM lake.gold.members);
```

命令行：`duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite; ATTACH 'ducklake:sqlite:/home/ubuntu/dagster/lake_catalog.duckdb' AS lake; SELECT * FROM lake.gold.fm_per_country LIMIT 20;"`

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

- **只在 VM 运行**：所有路径写死为 `/home/ubuntu/dagster`；在 Windows/Mac 上只能做语法检查，不能跑 dlt/dbt。
- **catalog 是 SQLite**：`lake_catalog.duckdb` 实为 SQLite。dlt（`sf_bronze.py`/`dataverse_bronze.py` 的 `CATALOG`）与 dbt（`profiles.yml` 的 `attach`）必须指向同一个 SQLite catalog，否则 silver 读不到 bronze。
- **列名大小写**：silver 用带引号的精确列名投影。若某列在 bronze 中的大小写与 `silver_config.py` 不一致（例如 `policy_omit_trademark_symbols__c`），silver 构建会报「列不存在」。排查：`DESCRIBE lake.bronze.<表名>;` 核对真实列名并同步到配置。
- **DuckLake 单写**：`profiles.yml` 的 `threads: 1` 不要改大，SQLite catalog 并发提交会冲突。
- **Gold 快照幂等**：同一 `snapshot_date` 重复运行 gold 不会重复追加（模型内有去重保护）；换一天则追加新快照。
- **Gold 数值字段**：源自 Salesforce 的数值/日期在 bronze 里可能是字符串，gold 用 `try_cast` 容错，无法解析的值会变成 `NULL` 并在过滤中被排除。
- **旧版脚本**：`daily.py` / `monthly_test.py` 仅作为 Gold 逻辑的对照参考，新栈不再依赖它们（`monthly_test.py` 第 169–171 行有历史遗留的语法问题，已在 dbt 移植中规避）。
