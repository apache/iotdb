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

# 共同任务说明

你要根据当前任务的公开信息，实际读取 IoTDB 并返回准确答案。自主选择必要的
查询，查看工具返回后决定是否继续查询或纠错。下面的访问模式说明规定了可用能力；
不要假设未提供的表名、schema、行数、数据生成规律或时间范围。

你唯一可用的工具是 `iotdb_command`，参数结构为
`{"command": "一条允许的命令"}`，没有其他参数。连接、认证和访问模式已经配置，
不要把启动 CLI 的脚本、连接选项或密码放进 command。每次模型响应最多调用工具
一次；收到结果后才可发起下一次调用。每次调用启动独立的 CLI，会话状态不保留。

只读当前任务指定数据库。不允许写入、导出、跨库访问、复合语句、管道、输入输出
重定向、通用 shell、Python、网络搜索、本地文件读取或其他数据库客户端。
你可以根据实际观察自行做少量算术，并按任务要求整理最终结果。

工具响应包含 `stdout`、`stderr`、`exit_code`、`error_kind`、`timed_out`、
`truncated`、`stdout_bytes`、`stderr_bytes`。先检查执行状态，再解释输出。
stdout 最多提供 64 KiB，stderr 最多提供 8 KiB；`truncated=true` 表示输出不完整，
不能将保留的前缀当成全部数据。需要时自行缩小查询或分页，并确保得到任务要求的
全部记录或统计范围。表头、说明文字、元数据条目不等于数据记录。

每个任务最多 180 秒、12 次工具尝试和 13 次模型请求。单次工具限时 30 秒，
单次模型请求限时 60 秒，均受任务剩余时间限制。非法命令、拒绝执行、CLI 错误和
超时也占用工具尝试次数；可根据返回的错误在剩余预算内纠正。不要把未知结果
编造成答案。不要请求评分器检查中间答案。

答案必须有真实读取结果作为依据，至少成功读取一次与任务相关的数据库对象。
全表统计须依据覆盖全表的数据或全表统计结果，不能用少量样本外推。已知 schema
任务会公开表名及结构；发现任务仅提供数据库名及业务线索，须自行发现相关对象。

完成时只返回当前任务指定 schema 的一个 JSON 值，不加 Markdown 代码块、说明
文字或额外字段。所有字段、数组长度和排序遵守任务要求。时间按毫秒整数表达，
计数用整数，数值用 JSON 数字，布尔值用 true/false，空值用 null；除非答案 schema
明确要求字符串，不把数值写成字符串。不要返回 NaN 或 Infinity。工具输出格式
不是最终答案结构，需要自行转换。保留足够数值精度，不为美观过早四舍五入。
