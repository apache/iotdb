<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# IoTDB 联邦查询插件使用说明

本插件使用 IoTDB **表模型 TableFunction/UDTF** 将外部 JDBC 查询结果作为关系表，支持继续过滤、聚合，以及与 IoTDB 本地表 JOIN。分支为 `ty/federated-query-plugin`，基于 master `87a6d198a9`；当前版本 `2.0.11-SNAPSHOT`。全部改动位于 `library-udf`。

## 1. 支持的连接器

类名均位于 `org.apache.iotdb.library.relational.tablefunction.connector` 包。

| 函数示例名 | 实现类 | JDBC 驱动类 | URL 前缀 |
| --- | --- | --- | --- |
| query_opengauss | OpenGaussConnectorTableFunction | org.opengauss.Driver | jdbc:opengauss: |
| query_gaussdb | GaussDBConnectorTableFunction | com.huawei.gaussdb.jdbc.Driver | jdbc:gaussdb: |
| query_pg | PostgreSqlConnectorTableFunction | org.postgresql.Driver | jdbc:postgresql: |
| query_mysql | MySqlConnectorTableFunction | com.mysql.cj.jdbc.Driver | jdbc:mysql: |
| query_clickhouse | ClickhouseConnectorTableFunction | com.clickhouse.jdbc.ClickHouseDriver | jdbc:ch: |

Cassandra、Doris、MongoDB、Redis、Snowflake 类目前仍是占位类。

本次实测 openGauss 5.0.0：原 PG 连接器在 **MD5 认证**下可用；默认 **SHA-256 认证**使用 openGauss 原生驱动。GaussDB 官方驱动也成功查询了同一 openGauss 后端，但本次没有连接商用 GaussDB 服务端。目标 GaussDB 应使用与服务端版本匹配的官方驱动。

## 2. 打出带依赖的 JAR

在仓库根目录使用 JDK 17 执行：

```bash
mvn clean package -pl library-udf -am \
  -P federated-query-jar -DskipTests
```

生成的单 JAR：

```text
library-udf/target/library-udf-2.0.11-SNAPSHOT-federated-jar-with-dependencies.jar
```

这个 profile 打入插件、第三方运行依赖及五种 JDBC 驱动：MySQL 9.3.0、PostgreSQL 42.7.7、ClickHouse 0.8.2、openGauss 6.0.3-og、GaussDB v2.0-8.218.0。GaussDB 的 Maven 坐标为 `com.huaweicloud:gaussdbjdbc:v2.0-8.218.0`，对应本次目标环境 GaussDB 25.1.32 / V2.0-8.218.0。合并 JDBC ServiceLoader 配置，排除由 IoTDB 提供的 UDF API、TsFile 和 SLF4J API。

普通构建仍生成不含 JDBC 驱动的薄 JAR：

```bash
mvn clean package -pl library-udf -am -DskipTests
```

薄 JAR 需要另行放置所需的驱动及其依赖。旧的 `get-jar-with-dependencies` profile 不会打入本插件声明为 provided 的 JDBC 驱动；联邦查询单包部署请使用 `federated-query-jar`，不要同时启用两种打包 profile。

如需中文错误消息，在同一打包命令末尾加 `-P with-zh-locale`。测试用命令：

```bash
mvn clean verify -pl library-udf -am -P federated-query-jar \
  -Dtest=JDBCConnectorTableFunctionTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false
```

在本机外置盘测试时，将 TMPDIR 和 Java `-Djava.io.tmpdir` 指向 `/Volumes/timecho-yuan/codex-tests` 下的测试目录；Maven 仓库也应使用外置盘上的缓存。

## 3. 安装到 IoTDB 并注册

把带依赖 JAR 放到每个 ConfigNode、DataNode 的 `udf_lib_dir`（默认是各节点安装目录下 `ext/udf/`）。各节点需能够访问 JDBC URL 指向的数据库。单机 Edge 将 JAR 放到同一个安装目录即可。

```bash
# IOTDB_INSTALL_DIR 改成当前节点的安装目录。
IOTDB_INSTALL_DIR=/path/to/iotdb
mkdir -p "$IOTDB_INSTALL_DIR/ext/udf"
cp library-udf/target/library-udf-2.0.11-SNAPSHOT-federated-jar-with-dependencies.jar \
  "$IOTDB_INSTALL_DIR/ext/udf/"
```

目录中只保留本插件的一种部署形式：单 JAR 方案不再需要单独放置薄插件和同版本驱动，避免重复类。升级已有函数时，先结束相关查询并 DROP FUNCTION，再替换文件、重新注册；新安装建议先放好 JAR，再启动节点。

连接 IoTDB 表模型（示例实测端口 16667，普通安装通常为 6667）：

```bash
bash "$IOTDB_INSTALL_DIR/sbin/start-cli.sh" \
  -h 127.0.0.1 -p 16667 -u root -pw root -sql_dialect table
```

注册 SQL（也见 [02-iotdb-register.sql](examples/federated-query/02-iotdb-register.sql)）：

```sql
-- 在 IoTDB 表模型会话中执行；同名函数已注册时跳过对应 CREATE。
CREATE FUNCTION query_opengauss AS
  'org.apache.iotdb.library.relational.tablefunction.connector.OpenGaussConnectorTableFunction';
CREATE FUNCTION query_gaussdb AS
  'org.apache.iotdb.library.relational.tablefunction.connector.GaussDBConnectorTableFunction';
CREATE FUNCTION query_pg AS
  'org.apache.iotdb.library.relational.tablefunction.connector.PostgreSqlConnectorTableFunction';
CREATE FUNCTION query_mysql AS
  'org.apache.iotdb.library.relational.tablefunction.connector.MySqlConnectorTableFunction';
CREATE FUNCTION query_clickhouse AS
  'org.apache.iotdb.library.relational.tablefunction.connector.ClickhouseConnectorTableFunction';
SHOW FUNCTIONS;

```

函数有四个标量参数：SQL、URL、USERNAME、PASSWORD。**PASSWORD 是 IoTDB SQL 保留字，命名参数必须写成 `"PASSWORD"`**。SQL 参数中的字符串引号要写成两个单引号。也可以按 SQL、URL、USERNAME、PASSWORD 的顺序使用四个位置参数。

## 4. 测试地址、权限与认证准备

下面 SQL 使用以下独立测试对象：

| 项目 | 值 |
| --- | --- |
| openGauss 地址 | 127.0.0.1:15432（容器将 5432 映射到该端口） |
| openGauss 数据库 | federated_demo，UTF8，PG 兼容模式 |
| 原生测试账号 | federated_reader / Fq_Native_2026!，SHA-256 |
| 可选 PG 兼容账号 | federated_compat / Fq_Pg_2026!，MD5 |
| IoTDB 地址 | 127.0.0.1:16667，表模型 |
| IoTDB 测试数据库 | federated_demo（与外部数据库是两个独立数据库） |

这些密码仅供独立测试。远程或多节点部署时，将 **所有 SQL 的 URL** 改成对应 ConfigNode/DataNode 可达的地址，127.0.0.1 始终表示执行进程所在机器。为测试账号授予源表 SELECT 权限即可。

以 openGauss 安装用户运行以下命令，`/path/to/opengauss/data` 替换为真实数据目录：

```bash
gs_guc reload -D /path/to/opengauss/data -c 'password_encryption_type=2'
```

`password_encryption_type` 在本次 openGauss 5.0.0 中是 **sighup 参数**，不能用 SQL `SET` 修改。变更后新建连接执行 `SHOW password_encryption_type`，确认结果为 2 再创建原生账号。

在 `pg_hba.conf` 中为 IoTDB 节点的实际出口 IP/网段配置认证。下面是端口仅绑定本机的测试容器配置片段，已有更宽泛的匹配规则时应把这些规则放在前面：

```text
host  federated_demo  federated_reader  0.0.0.0/0  sha256
host  federated_demo  federated_compat  0.0.0.0/0  md5
```

实际部署应将 CIDR 换成节点网段；只有准备做 PG 兼容测试时才需要第二条。修改后执行：

```bash
gs_ctl reload -D /path/to/opengauss/data
```

## 5. openGauss 建库、建表和原始数据 SQL

在 openGauss `gsql` 中用数据库管理员执行以下完整 SQL。也可保存为文件，执行：

```bash
gsql -d postgres -v ON_ERROR_STOP=1 \
  -f library-udf/examples/federated-query/01-opengauss-setup.sql
```

上述文件路径相对于仓库根目录；若 gsql 在远程机器/容器中，应先复制 SQL 文件过去。`\connect` 是 gsql 命令，不能提交给 IoTDB。

```sql
-- 在 openGauss 的 gsql 中执行；运行前确认 password_encryption_type=2。
-- 以下数据库、账号为独立测试对象；首次执行时应不存在。
CREATE DATABASE federated_demo WITH ENCODING='UTF8' TEMPLATE=template0 DBCOMPATIBILITY='PG';
\connect federated_demo
CREATE USER federated_reader PASSWORD 'Fq_Native_2026!';

CREATE TABLE public.fq_devices (
  device_id varchar(20),
  name varchar(100),
  temperature double precision,
  enabled boolean,
  measured_at timestamp,
  installed_on date,
  payload bytea,
  amount numeric(10,2),
  seq bigint
);

INSERT INTO public.fq_devices VALUES
  ('d1', '温度计', 21.5, true, '2025-06-27 12:34:56.123', '2025-06-27',
   decode('00ff10', 'hex'), 123.45, 9000000001),
  ('d2', '压力计', -5.25, false, '2025-06-28 01:02:03', '2025-06-28',
   decode('abcd', 'hex'), -7.50, 9000000002),
  ('d3', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

GRANT SELECT ON public.fq_devices TO federated_reader;
SELECT version();
SELECT device_id, name, temperature, enabled, amount, seq
FROM public.fq_devices ORDER BY device_id;

```

预期有三行：d1/温度计/21.5，d2/压力计/-5.25，d3 的其余字段为 NULL。

## 6. IoTDB 中的完整查询与 JOIN 测试 SQL

注册函数后，在 IoTDB **表模型会话**执行以下 SQL，也见 [03-iotdb-queries.sql](examples/federated-query/03-iotdb-queries.sql)。它包含普通查询、完整类型、别名、NULL、空结果、跨批次、JOIN、GaussDB 驱动及 LIMIT 测试。

```sql
-- 在 IoTDB 表模型会话中执行；已执行 02-iotdb-register.sql。
-- URL 中的地址是 ConfigNode/DataNode 能访问的 openGauss 地址。
CREATE DATABASE IF NOT EXISTS federated_demo;
USE federated_demo;

-- 1. 确认真实外部数据库版本。
SELECT * FROM query_opengauss(
  SQL => 'SELECT version() AS db_version',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 2. 全类型与 NULL：应返回 d1、d2、d3 三行。
SELECT * FROM query_opengauss(
  SQL => 'SELECT * FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 3. 别名与 SQL 字符串转义：应返回 renamed_id=d1。
SELECT renamed_id FROM query_opengauss(
  SQL => 'SELECT device_id AS renamed_id FROM public.fq_devices WHERE device_id = ''d1''',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 4. NULL 过滤：应只返回 d3。
SELECT device_id FROM query_opengauss(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') WHERE name IS NULL;

-- 5. 空结果：应返回 0 行。
SELECT * FROM query_opengauss(
  SQL => 'SELECT device_id FROM public.fq_devices WHERE false',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 6. 跨多个 TsBlock：row_count=5001，id_sum=12507501。
SELECT count(*) AS row_count, sum(id) AS id_sum FROM query_opengauss(
  SQL => 'SELECT generate_series(1,5001) AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 7. IoTDB 本地数据与 openGauss 表 JOIN。
-- measurements 为本测试新建的 IoTDB 表；已有同名表时请改名。
CREATE TABLE measurements (device_id STRING TAG, temperature DOUBLE FIELD);
INSERT INTO measurements(time, device_id, temperature) VALUES
  (1, 'd1', 20.0), (2, 'd2', 30.0), (3, 'd4', 40.0);
SELECT l.device_id, r.name, l.temperature
FROM measurements l JOIN query_opengauss(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') r
ON l.device_id = r.device_id ORDER BY l.device_id;
-- 预期：d1 / 温度计 / 20.0；d2 / 压力计 / 30.0。d4 不匹配。

-- 8. GaussDB 连接器使用 Huawei 驱动读取同一 openGauss 实例，应返回 3 行。
-- 此结果仅验证 openGauss 后端，不能替代商用 GaussDB 实例验证。
SELECT device_id, name FROM query_gaussdb(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:gaussdb://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 9. 提前结束：应只返回 id=1；可重复执行后检查连接释放。
SELECT * FROM query_opengauss(
  SQL => 'SELECT generate_series(1,5001) AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!') LIMIT 1;

```

时间精度使用 IoTDB 默认的毫秒（ms）。测试若统一采用 UTC，d1.measured_at 对应 `1751027696123` 毫秒；客户端显示形式可能受时区影响。d1.installed_on 为 `2025-06-27`，payload 原始字节为 `00 ff 10`。SUM 可能显示为 `1.2507501E7`，与 12507501 数值相同。

## 7. 异常及错误恢复 SQL

以下前三条应报错，请在 CLI 中逐条执行；使用遇错即停的 SQL 执行器时应分开提交。最后一条应恢复正常并返回 3。

```sql
-- 在 IoTDB 表模型会话中逐条执行；前三条报错属于预期结果。
USE federated_demo;

-- 1. 不存在的列：应报外部 SQL/元数据错误。
SELECT * FROM query_opengauss(
  SQL => 'SELECT missing_column FROM public.fq_devices',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 2. 错误密码：应报认证失败。
SELECT * FROM query_opengauss(
  SQL => 'SELECT 1 AS id',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'wrong-password');

-- 3. PostgreSQL 驱动尝试 SHA-256 账号：本次 openGauss 5.0.0 下应认证失败。
SELECT * FROM query_pg(
  SQL => 'SELECT 1 AS id',
  URL => 'jdbc:postgresql://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

-- 4. 错误之后恢复查询：应返回 row_count=3。
SELECT count(*) AS row_count FROM query_opengauss(
  SQL => 'SELECT device_id FROM public.fq_devices',
  URL => 'jdbc:opengauss://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_reader', "PASSWORD" => 'Fq_Native_2026!');

```

## 8. 可选：原 PG 连接器的 MD5 兼容测试

先完成第 4 节的 MD5 账号 pg_hba.conf 规则，然后暂时调整新账号的密码存储方式：

```bash
gs_guc reload -D /path/to/opengauss/data -c 'password_encryption_type=0'
```

用新的 gsql 会话执行，确认 SHOW 结果为 0：

```sql
-- 在 openGauss 的 gsql 中执行。
-- 执行前用 gs_guc reload 设置 password_encryption_type=0；执行后恢复为 2。
\connect federated_demo
SHOW password_encryption_type;
CREATE USER federated_compat PASSWORD 'Fq_Pg_2026!';
GRANT SELECT ON public.fq_devices TO federated_compat;

```

随即恢复默认值；原生账号仍保留此前建立的 SHA-256 密码：

```bash
gs_guc reload -D /path/to/opengauss/data -c 'password_encryption_type=2'
```

在 IoTDB 中执行：

```sql
-- 在 IoTDB 表模型会话中执行；先完成 MD5 账号和 pg_hba.conf 配置。
USE federated_demo;
SELECT device_id, name FROM query_pg(
  SQL => 'SELECT device_id, name FROM public.fq_devices ORDER BY device_id',
  URL => 'jdbc:postgresql://127.0.0.1:15432/federated_demo?connectTimeout=10&socketTimeout=30',
  USERNAME => 'federated_compat', "PASSWORD" => 'Fq_Pg_2026!');
-- 预期 3 行：d1/温度计，d2/压力计，d3/NULL。

```

## 9. 检查数据库配置和连接释放

结束上述 IoTDB 查询后，在 openGauss 管理员 gsql 会话执行：

```sql
-- 在 openGauss 中用数据库管理员执行。
\connect federated_demo
SHOW password_encryption_type;
SELECT current_database(), pg_encoding_to_char(encoding)
FROM pg_database WHERE datname=current_database();
SELECT rolname,
       CASE WHEN rolpassword LIKE 'md5%' THEN 'md5'
            WHEN rolpassword LIKE 'sha256%' THEN 'sha256'
            ELSE 'other' END AS password_type
FROM pg_authid
WHERE rolname IN ('federated_reader', 'federated_compat') ORDER BY rolname;

-- 等 IoTDB 查询结束后执行，预期 0 行。
SELECT usename, state, count(*) FROM pg_stat_activity
WHERE usename IN ('federated_reader', 'federated_compat')
GROUP BY usename, state ORDER BY usename, state;

```

预期：数据库编码 UTF8；默认 password_encryption_type=2；原生账号密码类型 sha256，可选兼容账号 md5；最后一条查询返回 0 行。可以将 LIMIT 1 查询重复执行 20 次后，再检查是否存在残留连接。

## 10. 当前边界

- 当前基线 `87a6d198a9` 实测 `SELECT * FROM query_opengauss(...) ORDER BY device_id` 会触发主干规划器的 `deviceTableScanNode` 空指针。纯外部结果需要排序时，将 `ORDER BY` 写入 SQL 参数中的外部查询；上述脚本已采用此方式。本地表 JOIN 后 `ORDER BY l.device_id` 的示例已通过。外部返回顺序仅适用于当前单来源执行方式，若后续添加会改变顺序的 IoTDB 运算，不能依赖输入顺序。

- 外部 SQL 使用目标数据库方言；过滤条件写入 SQL 参数可减少传输量。当前没有自动谓词/Join 下推或并行分片。
- 分析阶段会打开连接获取元数据，执行阶段另开连接。TsBlock 按大小/行数分批输出，但 JDBC 驱动可能缓存完整结果。
- NUMERIC/DECIMAL 映射为 DOUBLE，可能损失高精度；数组、JSON、UUID 等未支持类型可先在外部 SQL 中转换为受支持类型。
- TIMESTAMP/TIME 按 JDBC 毫秒转换；各节点 JVM 时区应保持一致。非毫秒精度的 IoTDB 部署需要额外适配。
- 本说明中的基本 openGauss 查询不依赖 PostgreSQL 兼容认证；优先使用 query_opengauss 与原生 SHA-256 账号。

## 11. 已有本机测试环境

已安装的环境在外置盘 `/Volumes/timecho-yuan/codex-tests/runs/2026-09-18/iotdb-federated-query-plugin/`，Lima 虚拟机名 `fq0918`，openGauss 容器名 `fq-opengauss`。以前一轮测试的数据库名为 `federated_remote`；本说明使用 `federated_demo`，两组数据独立。本次文档验证也已创建 `federated_demo` 中的对象；直接复用本机环境时，跳过重复的 CREATE/INSERT，执行 SELECT 即可。脚本中的建库、建账号和建表段按首次运行编排。

官方 openGauss 5.0.0 ARM64 镜像的 gosu 工具存在架构错误；当前测试容器已换成校验过 SHA-256 的上游 gosu 1.19 ARM64 版本。本机测试实例平时保持停止，按测试记录中的启动脚本恢复即可。

## 12. 本次文档验证结果

本节端到端结果使用初版 GaussDB 驱动 `com.huaweicloud.gaussdb:gaussdbjdbc:506.0.0.b058-jdk7`。当前默认依赖已升级为 `com.huaweicloud:gaussdbjdbc:v2.0-8.218.0`；原有 openGauss 验证记录不能替代目标商用 GaussDB 实例的验证。

仅在 `ext/udf/` 放置一个 `federated-jar-with-dependencies.jar`，移出原薄插件及三个独立驱动 JAR 后，原有 14 项端到端断言全部通过。随后直接读取本目录提交的 SQL 文件执行，五个函数注册、九组成功查询、预期错误与错误恢复、PG 兼容查询合计 16 项检查通过。openGauss 的初始化与配置检查脚本也已在真实实例执行。

单 JAR 内容已检查：包含五种 JDBC 驱动，JDBC services 已合并，不含 IoTDB UDF API、TsFile、SLF4J API 副本；单元测试 6 项通过。测试完成后，IoTDB 与 openGauss/Lima 环境均停止，文件和数据保留在外置盘。
