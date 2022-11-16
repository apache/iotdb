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

# 状态码

IoTDB 引入了**状态码**这一概念。例如，因为 IoTDB 需要在写入数据之前首先注册时间序列，一种可能的解决方案是：

```
try {
    writeData();
} catch (SQLException e) {
  // the most case is that the time series does not exist
  if (e.getMessage().contains("exist")) {
      //However, using the content of the error message is not so efficient
      registerTimeSeries();
      //write data once again
      writeData();
  }
}

```

利用状态码，我们就可以不必写诸如`if (e.getErrorMessage().contains("exist"))`的代码，
只需要使用`e.getStatusType().getCode() == TSStatusCode.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`。

这里是状态码和相对应信息的列表：

|状态码|状态类型|状态信息|
|:--|:---|:---|
|200|SUCCESS_STATUS|成功状态|
|201|STILL_EXECUTING_STATUS|仍在执行状态|
|203|INCOMPATIBLE_VERSION|版本不兼容|
|298|NODE_DELETE_ERROR|删除节点失败|
|299|ALIAS_ALREADY_EXIST|路径别名已经存在|
|300|PATH_ALREADY_EXIST|路径已经存在|
|303|METADATA_ERROR|处理元数据错误|
|304|PATH_NOT_EXIST|路径不存在|
|305|OUT_OF_TTL|插入时间少于 TTL 时间边界|
|307|COMPACTION_ERROR|合并错误|
|308|SYSTEM_CHECK_ERROR|系统检查错误|
|310|SYNC_CONNECTION_ERROR|回传连接错误|
|311|DATABASE_PROCESS_ERROR|存储组处理器相关错误|
|313|STORAGE_ENGINE_ERROR|存储引擎相关错误|
|314|TSFILE_PROCESSOR_ERROR|TsFile 处理器相关错误|
|315|PATH_ILLEGAL|路径不合法|
|316|LOAD_FILE_ERROR|加载文件错误|
|317|DATABASE_NOT_READY|Database 还在恢复中，还不能接受读写操作|
|318|ILLEGAL_PARAMETER|参数错误|
|319|ALIGNED_TIMESERIES_ERROR|对齐时间序列错误|
|320|DUPLICATED_TEMPLATE|元数据模板重复|
|321|UNDEFINED_TEMPLATE|元数据模板未定义|
|322|DATABASE_NOT_EXIST|数据库不存在|
|323|CONTINUOUS_QUERY_ERROR|连续查询功能错误|
|324|NO_TEMPLATE_ON_MNODE|当前元数据节点不存在元数据模板|
|325|DIFFERENT_TEMPLATE|元数据模板不一致|
|326|TEMPLATE_IS_IN_USE|元数据模板正在使用|
|327|TEMPLATE_INCOMPATIBLE|元数据模板不兼容|
|328|SEGMENT_NOT_FOUND|未找到 Segment|
|329|PAGE_OUT_OF_SPACE|SchemaFile 中 Page 空间不够|
|330|RECORD_DUPLICATED|记录重复|
|331|SEGMENT_OUT_OF_SPACE|SchemaFile 中 segment 空间不够|
|332|SCHEMA_FILE_NOT_EXISTS|SchemaFile 不存在|
|333|WRITE_AHEAD_LOG_ERROR|WAL 异常|
|334|CREATE_PIPE_SINK_ERROR|创建 PIPE Sink 失败|
|335|PIPE_ERROR|PIPE 异常|
|336|PIPESERVER_ERROR|PIPE server 异常|
|337|SERIES_OVERFLOW|序列数量超过阈值|
|338|MEASUREMENT_ALREADY_EXIST|序列数量超过阈值|
|340|CREATE_TEMPLATE_ERROR|创建物理量模板失败|
|341|SYNC_FILE_REDIRECTION_ERROR|同步文件时重定向异常|
|342|SYNC_FILE_ERROR|同步文件异常|
|343|VERIFY_METADATA_ERROR|校验元数据失败|
|344|TIMESERIES_IN_BLACK_LIST|时间序列正在删除|
|349|OVERSIZE_RECORD|记录大小超过元数据文件页面大小|
|350|SCHEMA_FILE_REDO_LOG_BROKEN|SchemaFile 的 redo 日志损坏|
|355|TRIGGER_FIRE_ERROR|触发器执行错误|
|360|TRIGGER_LOAD_CLASS_ERROR|触发器加载类异常|
|361|TRIGGER_DOWNLOAD_ERROR|从 ConfigNode 下载触发器异常|
|362|CREATE_TRIGGER_INSTANCE_ERROR|创建触发器实例异常|
|363|ACTIVE_TRIGGER_INSTANCE_ERROR|激活触发器实例异常|
|364|DROP_TRIGGER_INSTANCE_ERROR|删除触发器实例异常|
|365|UPDATE_TRIGGER_LOCATION_ERROR|更新有状态的触发器所在 DataNode 异常|
|370|UDF_LOAD_CLASS_ERROR|UDF 加载类异常|
|371|UDF_DOWNLOAD_ERROR|无法从 ConfigNode 下载 UDF|
|372|CREATE_FUNCTION_ON_DATANODE_ERROR|在 DataNode 创建 UDF 失败|
|373|DROP_FUNCTION_ON_DATANODE_ERROR|在 DataNode 卸载 UDF 失败|
|400|EXECUTE_STATEMENT_ERROR|执行语句错误|
|401|SQL_PARSE_ERROR|SQL 语句分析错误|
|402|GENERATE_TIME_ZONE_ERROR|生成时区错误|
|403|SET_TIME_ZONE_ERROR|设置时区错误|
|405|QUERY_NOT_ALLOWED|查询语句不允许|
|407|LOGICAL_OPERATOR_ERROR|逻辑符相关错误|
|408|LOGICAL_OPTIMIZE_ERROR|逻辑优化相关错误|
|409|UNSUPPORTED_FILL_TYPE|不支持的填充类型|
|411|QUERY_PROCESS_ERROR|查询处理相关错误|
|412|WRITE_PROCESS_ERROR|写入相关错误|
|413|WRITE_PROCESS_REJECT|写入拒绝错误|
|416|SEMANTIC_ERROR|SQL 语义错误|
|417|LOAD_PIECE_OF_TSFILE_ERROR|加载 TsFile 片段异常|
|423|MEMORY_NOT_ENOUGH|MPP 框架中任务执行内存不足|
|500|INTERNAL_SERVER_ERROR|服务器内部错误|
|501|CLOSE_OPERATION_ERROR|关闭操作错误|
|502|READ_ONLY_SYSTEM|系统只读|
|503|DISK_SPACE_INSUFFICIENT|磁盘空间不足|
|504|START_UP_ERROR|启动错误|
|505|SHUT_DOWN_ERROR|关机错误|
|506|MULTIPLE_ERROR|多行语句执行错误|
|508|TSBLOCK_SERIALIZE_ERROR|TsBlock 序列化错误|
|600|WRONG_LOGIN_PASSWORD|用户名或密码错误|
|601|NOT_LOGIN|没有登录|
|602|NO_PERMISSION|没有操作权限|
|603|UNINITIALIZED_AUTH_ERROR|授权人未初始化|
|604|EXECUTE_PERMISSION_EXCEPTION_ERROR||
|605|USER_NOT_EXIST|用户不存在|
|606|ROLE_NOT_EXIST|角色不存在|
|607|AUTHENTICATION_FAILED|权限认证失败|
|608|CLEAR_PERMISSION_CACHE_ERROR|清空权限缓存失败|
|701|TIME_OUT|操作超时|
|703|UNSUPPORTED_OPERATION|不支持的操作|
|706|NO_CONNECTION|连接获取失败|
|707|NEED_REDIRECTION|需要重定向|
|709|ALL_RETRY_ERROR|所有重试失败|
|710|MIGRATE_REGION_ERROR|Region 迁移失败|
|711|CREATE_REGION_ERROR|创建 region 失败|
|712|DELETE_REGION_ERROR|删除 region 失败|
|713|PARTITION_CACHE_UPDATE_ERROR|更新分区缓存失败|
|714|DESERIALIZE_PIECE_OF_TSFILE_ERROR|反序列化 TsFile 片段异常|
|715|CONSENSUS_NOT_INITIALIZED|共识层未初始化，不能提供服务|
|800|CONFIGURATION_ERROR|配置文件有错误项|
|901|DATANODE_ALREADY_REGISTERED|DataNode 在集群中已经注册|
|903|DATABASE_ALREADY_EXISTS|Database 已存在|
|904|NO_ENOUGH_DATANODE|DataNode 数量不足，无法移除节点或创建副本|
|905|ERROR_GLOBAL_CONFIG|全局配置参数异常|
|906|ADD_CONFIGNODE_ERROR|新增 ConfigNode 失败|
|907|REMOVE_CONFIGNODE_ERROR|移除 ConfigNode 失败|
|912|DATANODE_NOT_EXIST|此 DataNode 不存在|
|917|DATANODE_STOP_ERROR|DataNode 关闭失败|
|918|REGION_LEADER_CHANGE_ERROR|Region leader 迁移失败|
|919|REMOVE_DATANODE_ERROR|移除 datanode 失败|
|920|OVERLAP_WITH_EXISTING_TASK|与正在执行的其他操作冲突|
|921|NOT_AVAILABLE_REGION_GROUP|无法找到可用的 Region 副本组|
|922|CREATE_TRIGGER_ERROR|ConfigNode 创建 Trigger 失败|
|923|DROP_TRIGGER_ERROR|ConfigNode 删除 Trigger 失败|
|925|REGISTER_REMOVED_DATANODE|注册的 DataNode 已经被移除|
|930|NO_SUCH_CQ|CQ 任务不存在|
|931|CQ_ALREADY_ACTIVE|CQ 任务已激活|
|932|CQ_AlREADY_EXIST|CQ 任务已存在|
|933|CQ_UPDATE_LAST_EXEC_TIME_ERROR|CQ 更新上一次执行时间失败|

> 在最新版本中，我们重构了 IoTDB 的异常类。通过将错误信息统一提取到异常类中，并为所有异常添加不同的错误代码，从而当捕获到异常并引发更高级别的异常时，错误代码将保留并传递，以便用户了解详细的错误原因。
除此之外，我们添加了一个基础异常类“ProcessException”，由所有异常扩展。
