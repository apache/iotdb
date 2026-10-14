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

实现 FI 输入输出共享账目、Collect/MappingCollect 任意就绪消费，以及由无序汇集引起的计划属性修正。
聚合使用 HashAggregation：只清空实际输入已失去连续性的 preGroupedSymbols；有效分支内的流式聚合保留。
MergeSort、Join、ASOF、CrossJoin 只作依赖验证，不修改其消费算法，不新增进度保障机制。
TableTopK、CountMerge 的任意就绪消费留到后续；有序输入标志仍需匹配实际输入。

## 额度与账目

F 为配置的单 FI 交换预算。INPUT=F/2，OUTPUT=F-INPUT；同 FI 的所有 pipeline 继承相同固定值。
远端 SourceHandle 计入接收 FI 的 INPUT；SinkChannel 和 SharedTsBlockQueue 计入生产 FI 的 OUTPUT。
本地共享队列只计费一次，创建先后不改变所有者。

MemoryPool 维护 query→FI→planNode 明细，注册时固定 side。
FI 两侧使用 AtomicLong；planNode 字节明细通过 compute 增减。
申请、回滚、释放均对应修改全局池、FI 单侧与 planNode 三层账目。
额度判断使用本次增加后的单侧总数；稳定快照中的总数等于同侧明细之和。
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

覆盖共享额度并发、INPUT/OUTPUT 隔离、所有者、奇数 F、超大申请、回滚释放取消及稳定账目一致性；
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
