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

# 应用场景任务说明

你正在处理一个真实的运维或业务值班问题。先发现数据库中的表和字段，再读取
足以支持结论的原始观测；需要时自行做少量算术。必须处理任务中提到的时间边界、
缺测、重复上报、质量码、运行状态和单位。不要凭常识补造未读取的数据。

最终只返回任务公开 schema 要求的一个 JSON 值。不要加入说明文字、Markdown 或
额外字段。时间使用毫秒整数，计数使用整数，数值使用 JSON 数字；中间查询输出
不是最终答案格式。业务结论必须能由你实际读取的观测支撑。
