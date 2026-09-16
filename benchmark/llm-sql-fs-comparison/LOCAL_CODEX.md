<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# 本地 Codex 接入说明

运行入口和参数见 [README.md](README.md)。执行器通过本机 `codex app-server`
的 stdio 接口调用 `gpt-5.6-sol`，继承本地配置的模型供应商和认证。
模型请求发送至供应商；本地运行的是 Codex 客户端。

## 会话与工具

每个任务使用新的临时会话，推理强度为 `low`，注册唯一数据库工具
`iotdb_command`。Codex 的 exec 工具可以转发数据库调用并进行算术，
不提供 OS shell、文件、网络或其他数据库工具。

[restricted-model-catalog.json](restricted-model-catalog.json) 保留所用模型的目录条目，
将 `multi_agent_version` 设为 null。客户端通过 `model_catalog_json` 加载该目录，
禁用额外工具及 skill 加载，并隔离父会话环境。
模型不可见连接密码、fixture 和 oracle。

SQL/fs 两组共用只读账号、数据快照和任务目标，每次数据库调用启动独立 CLI。
完整模型工具交换和数据库原始返回保存在任务目录；不记录模型私有推理内容。

## 预算与重试

- 任务总预算 180 秒，单次 CLI 最多 30 秒。
- 单次模型尝试最多执行 12 次数据库工具；模型响应预算在完成事件边界检查。
- 60 秒配置为流空闲超时，不是单个模型请求的总耗时上限。
- 流在 `response.completed` 前断开时，默认新建会话重试最多 2 次；
  所有尝试与退避共用任务总预算。
- 重试历史记录在 `stream_retry_history`，次数见 `stream_retry_count`。
  错误答案、CLI 错误、任务超时和限流不会触发该流断开重试。

提示词要求每次模型响应最多调用一次工具；运行时数据库回调串行执行。
采样种子、固定输出 token 上限和禁用供应商前缀缓存不是当前适配器的控制项，
token 和缓存用量按服务端实际返回值记录。

## 数据与结果

基础任务准备六个库，对应三个表角色的不同置换；每对共享一个快照。
应用任务按 A1–A3 分别准备快照。连接信息只保存在忽略目录中的
`connections.private.json`，文件权限为 0600。

原始结果保存在 `trials.jsonl` 和 `trials/<id>/result.json`。
基础任务可通过 `evidence_review.json` 记录证据审核，分析器保留原始状态并应用审核。
应用任务直接进行完整答案比较。接口规则和事件过滤由
`test_tool_adapter.py`、`test_codex_client.py` 验证。

时间统计区分任务、工具、CLI 和非工具耗时。非工具部分包含模型、网络、排队与编排，
无法用它单独衡量模型计算时间。不同配置的运行使用独立目录，保留每次尝试记录。

官方协议：[Codex app-server](https://developers.openai.com/codex/app-server/)。
