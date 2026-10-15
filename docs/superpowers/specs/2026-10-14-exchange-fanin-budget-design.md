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

# Exchange 无序扇入与输入输出预算

基线：Apache master `aec1ad6139cb4dd1450725f53418cf40c26efb4e`。
依据：[技术设计文档](https://timechor.feishu.cn/docx/EpDFdmS94ovMCWxluEOc3WGDn8d)，修订 861。

## 已确认范围

实现仅针对无输入依赖 FI 的输入输出共享账目、Collect/MappingCollect 任意就绪消费，以及由无序汇集引起的计划属性修正。
聚合使用 HashAggregation：只清空实际输入已失去连续性的 preGroupedSymbols；有效分支内的流式聚合保留。
MergeSort、Join、ASOF、CrossJoin 不修改消费算法。FI 内只要存在需要特定输入推进的算子，就整组使用原有按 plan node 限额；Collect 下方或上方出现这些算子也不例外。
TableTopK、CountMerge 的任意就绪消费留到后续；有序输入标志仍需匹配实际输入。

## 额度与账目

共享采用 FI 级显式启用，默认保留原有配额。整个 FI 必须包含 CollectOperator 或 MappingCollectOperator，且所有多输入汇集都能消费任意就绪输入；顺序消费的 IdentitySink/CountMerge、MergeSort、Join、TopK 及未明确支持的多输入算子都禁止共享。检查在 Exchange 处停止，不跨到其他 FI。

在生成全部 pipeline 后再决定预算模式。内部 pipeline 队列具有转发依赖，因此存在多个 pipeline 或调度依赖时整组回退原策略。只含 Collect 的某个子树不会绕过 FI 级否决。

通过上述检查后，F 为配置的单 FI 交换预算。按照实际由本 FI 计费的方向分配：只有 INPUT 时 INPUT=F；只有 OUTPUT 时 OUTPUT=F；同时存在两侧时 INPUT=F/2、OUTPUT=F-INPUT；未使用的方向预算为 0。方向在完整本地计划生成后确定，远端 SourceHandle 表示本 FI 的 INPUT，fragment sink 表示 OUTPUT；LocalSourceHandle 的队列由生产 FI 计费，不重复占用接收 FI 的 INPUT。仅实际启用的方向须容纳配置的最大 TsBlock，否则回退原配额。未启用共享的 FI 恢复原有 exchangeSumNum 配额计算和按 plan node 的申请上限；不能只恢复限额数值却仍按同侧总量进行准入。
远端 SourceHandle 计入接收 FI 的 INPUT；SinkChannel 和 SharedTsBlockQueue 计入生产 FI 的 OUTPUT。
本地共享队列只计费一次，创建先后不改变所有者。

MemoryPool 维护 query→FI→planNode 明细，注册时固定 side。
本地计划生成全部算子和 pipeline 后，一次性检查共享资格并计算两个方向的预算，写入 `FragmentInstanceContext` 的 `inputMaxBytes`、`outputMaxBytes` 字段。`MemorySide` 仅表示方向，不计算预算，也不接受 `hasInput`/`hasOutput` 参数。MemoryPool 的 FI 账目仅保留该上下文的引用，不复制预算字段。共享模式的申请、单块上限检查和等待唤醒统一读取 FI 保存的预算；handle 传入的限额仅用于原有独立配额模式。回退独立配额时上下文的两个共享预算字段为零。
FI 两侧保留 AtomicLong 统计，plan node 保留字节明细。申请和释放在 FI 的记账锁内更新；全局池采用有界原子申请。申请前先检查适用的同侧总量或 plan node 限额，失败不产生临时计费，也不需要回滚，避免并发请求分别撞到不同限额后全部挂起。

释放后在 FI 记账锁外完成等待 Future；保留入队后的重试和每个 Future 的完成/取消同步。稳定快照中的 FI 同侧总量等于同侧明细之和。
保留现有最高优先级查询的零计费兜底与交换组件的收发、等待、释放机制。

## Collect 生命周期

AnyChildBlocked 缓存直接 child.isBlocked 的 Future，每个 pending 周期只有一个 listener。
新轮次重定向现有 listener；发布后检查完成状态，覆盖注册与完成交错。
回调只记录就绪或错误，在锁外完成选择 Future；所有 child 方法由 Driver 线程调用。
重复 isBlocked 与输出背压保留选择；取消对外等待不向 child 传播。

一次 next 消耗一次选择。null/空块刷新等待，不表示结束。
hasNext=false 后检查 isFinished，确认结束才关闭 child 并移除记录。
MappingCollect 按实际 childId 映射。关闭唤醒待完成选择，忽略迟到回调，逐一尝试关闭全部输入。
保留内存 R 为各存活 child 保留内存之和；峰值为 max(R,max(R-r_i+p_i))；返回大小为 max(b_i)。

## 计划属性

多路 Collect/Union 打断分支连续性和排序；单路可透传。
Filter 与符号透传 Project 不恢复属性；有效 Sort/MergeSort 可以重新建立属性。
在 SINGLE/PARTIAL/FINAL 实际 child 组装后校验聚合，包括 DISTINCT/mask、普通分阶段聚合及 Union 下推。
Group 在分区连续性失效时改为完整 Sort。复用 master 的 StreamSort 前缀校验和 Window/Fill 修复。
SortElimination 在各汇集分支独立优化，并阻止跨无序汇集删除恢复排序。

## 验证

覆盖纯 Collect/MappingCollect 显式启用、混合 FI 全组回退、Exchange 边界、内部 pipeline 否决、单块预算边界，以及共享/原配额 FI 共存；
覆盖共享额度并发、INPUT/OUTPUT 隔离、所有者、奇数 F、超大申请、失败准入无计费、释放取消及稳定账目一致性；
覆盖 100/1000 child、A 阻塞 B 就绪、listener 复用、注册竞争、输出背压、空块、结束、异常取消、关闭和映射；
覆盖文档 SQL 对应的聚合、Group/Window、StreamSort、Union 与 Fill 计划及实际结果。
运行 Spotless、英文/中文全 reactor test-compile、针对性单测，以及本分支新增或修改的 IT。

## 实现验证记录（2026-10-14）

上述约定范围的实现已完成。单元测试 86 项通过；IoTDBCollectFanInIT 的 6 项测试在单节点和三 DataNode 集群中分别通过。
集成验证包含普通与 DISTINCT 聚合、Union 分阶段聚合、嵌套窗口、Fill、排序/TopK、Join 依赖、树模型以及树表视图。

构建先执行以下干净构建，生成当前分支的节点包并安装本地依赖：

```sh
mvn -o clean install -DskipTests -pl integration-test,iotdb-core/confignode,iotdb-client/cli -am -P with-integration-tests
```

全 reactor 中文编译与英文编译/回归均通过；英文 test 阶段包含完整 test-compile：

```sh
mvn -o test-compile -DskipTests -P with-zh-locale
mvn -o test -Dtest=CollectOperatorTest,MemoryPoolTest,LocalExecutionPlanContextBudgetTest,CollectInputPropertyTest,SharedTsBlockQueueTest,SourceHandleTest,SinkChannelTest,LocalSourceHandleTest,LocalSinkChannelTest,MPPDataExchangeManagerTest,ParallelizeGroupingTest,TableFunctionSortTest -Dsurefire.failIfNoSpecifiedTests=false
```

两个集成环境均只运行本分支新增的测试类：

```sh
mvn -o verify -DskipUTs -Dit.test=IoTDBCollectFanInIT -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -P TableSimpleIT -P with-integration-tests
mvn -o verify -DskipUTs -Dit.test=IoTDBCollectFanInIT -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -P TableClusterIT -P with-integration-tests
```

使用离线模式是为了避免远端 SNAPSHOT ZIP 下载停滞；节点执行的是当前分支重新打包的代码。
Spotless、Checkstyle、新增文件许可证及中英文消息键名/占位符一致性检查通过。

## 自查与补测（2026-10-14）

自查复现了 MemoryPool 的等待申请漏唤醒：一次申请额度不足后，旧申请先释放全部额度，
新申请才进入等待队列；释放线程看不到新申请，导致空闲额度下仍无限等待。
回归测试用 latch 固定这一时序，在修复前稳定失败。
修复在申请入队后再次检查额度，并与 free/tryCancel 共用该 future 的锁，避免重复计费。

新增 8 项单元测试：等待申请发布竞态、取消不占用共享额度、16 路阻塞申请反复获得额度后账目归零；
空 Collect、全部阻塞时异常唤醒、并发就绪后每路恰好消费一次；
Collect 后 TopK 不误用有序输入，以及有效分支保留流式聚合。
增强关闭异常测试，检查其他输入仍被关闭且后续异常被保留。
新增一项集成测试，验证全局窗口后按字段倒序的 TopK 跨设备结果及完整窗口计数。

补测后的 118 项定向单测通过（14 个测试类，零失败、零跳过）；全 reactor 中英文编译通过。
在前述英文回归命令中加入 `OrderingPropertyPropagationTest,TopKRuntimeFilterDistributedPlanTest`，
同时验证有效排序优化与 TopK runtime filter 不受影响。
重新执行 `mvn -o install -DskipTests -pl integration-test,iotdb-core/confignode,iotdb-client/cli -am -P with-integration-tests`
后运行前述两条 verify 命令；IoTDBCollectFanInIT 的 7 项测试在单节点和三 DataNode 集群中分别全部通过。
Spotless、Checkstyle、许可证与 `git diff --check` 检查通过。

## FI 共享范围修正验证（2026-10-15）

共享改为完整 FI 的显式启用：仅支持 Collect/MappingCollect 的任意就绪汇集；
同一 FI 内的多输入依赖算子、内部 pipeline 队列或调度依赖会使整组回退。
Exchange 边界之外的上游 FI 不参与本 FI 的判断。回退同时恢复 handle 限额和按 plan node 准入。

107 项定向单元测试通过，包括纯 Collect/Union、混合 FI、pipeline 依赖、单块额度边界、
共享与独立配额共存、并发准入，以及真实 TableMergeSort 配合模拟输入和 MemoryPool 的回归。
本地双队列转发回归也通过。Spotless、Checkstyle、许可证和 `git diff --check` 检查通过。

```sh
mvn -o test -pl iotdb-core/datanode -Dtest=LocalExecutionPlanContextBudgetTest,MemoryPoolTest,SharedTsBlockQueueTest,SourceHandleTest,SinkChannelTest,LocalSourceHandleTest,LocalSinkChannelTest,MPPDataExchangeManagerTest,PipelineBuilderTest,CollectInputPropertyTest
mvn -o verify -DskipUTs -Dit.test=IoTDBCollectFanInIT -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -am -P TableSimpleIT -P with-integration-tests
mvn -o failsafe:integration-test@integration-test failsafe:verify@verify -DskipUTs -Dit.test=IoTDBCollectFanInIT -DfailIfNoTests=false -Dfailsafe.failIfNoSpecifiedTests=false -pl integration-test -P TableClusterIT -P with-integration-tests
```

单节点 verify 从当前源码构建依赖和测试节点；集群运行复用同一份节点模板。
IoTDBCollectFanInIT 的 8 项测试在两种环境中均通过，零失败、零跳过。

提交前再次执行完整 reactor 的中英文编译，两者均通过：

```sh
mvn -o test-compile -DskipTests -P with-zh-locale
mvn -o test-compile -DskipTests
```

本次生产代码没有新增未本地化的字符串，且没有修改中英文消息常量。

## 单方向预算修正验证（2026-10-15）

该轮修正取消默认按两侧均分，改为根据本 FI 实际计费方向分配：
单方向使用完整 F，双方向才拆分，未使用方向为 0。本次已将计算统一收回本地计划初始化，
不再保留该轮引入的 `MemorySide.getMaxBytes(F, hasInput, hasOutput)` 接口。
最大 TsBlock 校验只作用于启用的方向；原有 Collect 资格和依赖回退规则保持不变。

新增输入单向、输出单向、B=F、本地输入和本地/远端混合输入用例；
同时验证奇数 F、未使用方向及每个单方向一次申请完整 F 的边界。
113 项定向单测全部通过。首轮中单块边界测试的默认测试池上限为 512、申请为 513，
将该测试池配置为目标 F=513 后重跑 MemoryPoolTest，38 项全部通过，其余 75 项首轮通过。

完整 reactor 中英文 test-compile、Spotless、Checkstyle 和许可证检查通过。
重新从当前代码打包节点后，IoTDBCollectFanInIT 在 TableSimpleIT 和三 DataNode 的
TableClusterIT 中各 8 项全部通过；集群复用同一份节点模板，节点包哈希与本次构建一致。

## FI 预算字段初始化验证（2026-10-15）

`FragmentMemoryReservation` 保存 `inputMaxBytes`、`outputMaxBytes`；
完整本地计划生成后按输入 exchange、fragment sink 和依赖检查结果一次性初始化。
共享申请、单块上限检查与等待重试均读取 FI 字段，独立配额模式仍读取 handle 限额。
`MemorySide` 只保留方向枚举，移除布尔参数预算接口。

115 项定向单测通过，覆盖 handle 参数不能增减共享上限、超额请求不进入等待队列、
未使用方向预算为零、奇数预算拆分和原有独立配额回退。
完整 reactor 中英文编译、Spotless、Checkstyle 和许可证检查通过；生产代码没有新增字符串字面量。

重新打包当前源码后，`IoTDBCollectFanInIT` 在 TableSimpleIT 和三 DataNode 的 TableClusterIT
中各 8 项全部通过，无失败或跳过。集群复用同一份节点模板，其服务端 jar 哈希与本次构建一致。

## FI 预算配置归属修正验证（2026-10-15）

预算字段移至 `FragmentInstanceContext`，由完整本地计划生成后的初始化流程赋值。
MemoryPool 的 FI 账目保存上下文引用，通过 getter 读取限额，不再复制预算或保存共享布尔字段。
该引用随 FI 记账记录注销；预算仍在 driver 启动前初始化。
独立配额回退时上下文的两个共享预算字段为零，不注册共享预算。

123 项定向单测通过，包括真实 FI 上下文的初始化字段断言、申请上限和等待唤醒回归，
以及已有的 FI 上下文生命周期测试。生产代码没有新增字符串字面量。

本次单测在前述 `-Dtest` 列表中加入 `FragmentInstanceContextTest`。完整 reactor 的中英文
`test-compile -DskipTests`、Spotless、Checkstyle、许可证与 `git diff --check` 检查均通过。
重新打包节点后，`IoTDBCollectFanInIT` 在 TableSimpleIT 和三 DataNode 的 TableClusterIT 中各
8 项全部通过，无失败或跳过；测试模板与当前服务端 jar 的哈希一致。
