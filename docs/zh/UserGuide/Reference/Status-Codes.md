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

## 状态码

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

| 状态码  | 状态类型                                   | 状态信息                      |
|:-----|:---------------------------------------|:--------------------------|
| 200  | SUCCESS_STATUS                         | 成功状态                      |
| 201  | INCOMPATIBLE_VERSION                   | 版本不兼容                     |
| 202  | CONFIGURATION_ERROR                    | 配置文件有错误项                  |
| 203  | START_UP_ERROR                         | 启动错误                      |
| 204  | SHUT_DOWN_ERROR                        | 关机错误                      |
| 300  | UNSUPPORTED_OPERATION                  | 不支持的操作                    |
| 301  | EXECUTE_STATEMENT_ERROR                | 执行语句错误                    |
| 302  | MULTIPLE_ERROR                         | 多行语句执行错误                  |
| 303  | ILLEGAL_PARAMETER                      | 参数错误                      |
| 304  | OVERLAP_WITH_EXISTING_TASK             | 与正在执行的其他操作冲突              |
| 305  | INTERNAL_SERVER_ERROR                  | 服务器内部错误                   |
| 306  | DISPATCH_ERROR                         | 分发错误                      |
| 400  | REDIRECTION_RECOMMEND                  | 推荐客户端重定向                  |
| 500  | DATABASE_NOT_EXIST                     | 数据库不存在                    |
| 501  | DATABASE_ALREADY_EXISTS                | 数据库已存在                    |
| 502  | SERIES_OVERFLOW                        | 序列数量超过阈值                  |
| 503  | TIMESERIES_ALREADY_EXIST               | 时间序列已存在                   |
| 504  | TIMESERIES_IN_BLACK_LIST               | 时间序列正在删除                  |
| 505  | ALIAS_ALREADY_EXIST                    | 路径别名已经存在                  |
| 506  | PATH_ALREADY_EXIST                     | 路径已经存在                    |
| 507  | METADATA_ERROR                         | 处理元数据错误                   |
| 508  | PATH_NOT_EXIST                         | 路径不存在                     |
| 509  | ILLEGAL_PATH                           | 路径不合法                     |
| 510  | CREATE_TEMPLATE_ERROR                  | 创建物理量模板失败                 |
| 511  | DUPLICATED_TEMPLATE                    | 元数据模板重复                   |
| 512  | UNDEFINED_TEMPLATE                     | 元数据模板未定义                  |
| 513  | TEMPLATE_NOT_SET                       | 元数据模板未设置                  |
| 514  | DIFFERENT_TEMPLATE                     | 元数据模板不一致                  |
| 515  | TEMPLATE_IS_IN_USE                     | 元数据模板正在使用                 |
| 516  | TEMPLATE_INCOMPATIBLE                  | 元数据模板不兼容                  |
| 517  | SEGMENT_NOT_FOUND                      | 未找到 Segment               |
| 518  | PAGE_OUT_OF_SPACE                      | PBTreeFile 中 Page 空间不够    |
| 519  | RECORD_DUPLICATED                      | 记录重复                      |
| 520  | SEGMENT_OUT_OF_SPACE                   | PBTreeFile 中 segment 空间不够 |
| 521  | PBTREE_FILE_NOT_EXISTS                | PBTreeFile 不存在            |
| 522  | OVERSIZE_RECORD                        | 记录大小超过元数据文件页面大小           |
| 523  | PBTREE_FILE_REDO_LOG_BROKEN           | PBTreeFile 的 redo 日志损坏    |
| 524  | TEMPLATE_NOT_ACTIVATED                 | 元数据模板未激活                  |
| 526  | SCHEMA_QUOTA_EXCEEDED                  | 集群元数据超过配额上限               |
| 527  | MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE | 元数据模板中已存在物理量              |
| 600  | SYSTEM_READ_ONLY                       | IoTDB 系统只读                |
| 601  | STORAGE_ENGINE_ERROR                   | 存储引擎相关错误                  |
| 602  | STORAGE_ENGINE_NOT_READY               | 存储引擎还在恢复中，还不能接受读写操作       |
| 603  | DATAREGION_PROCESS_ERROR               | DataRegion 相关错误           |
| 604  | TSFILE_PROCESSOR_ERROR                 | TsFile 处理器相关错误            |
| 605  | WRITE_PROCESS_ERROR                    | 写入相关错误                    |
| 606  | WRITE_PROCESS_REJECT                   | 写入拒绝错误                    |
| 607  | OUT_OF_TTL                             | 插入时间少于 TTL 时间边界           |
| 608  | COMPACTION_ERROR                       | 合并错误                      |
| 609  | ALIGNED_TIMESERIES_ERROR               | 对齐时间序列错误                  |
| 610  | WAL_ERROR                              | WAL 异常                    |
| 611  | DISK_SPACE_INSUFFICIENT                | 磁盘空间不足                    |
| 700  | SQL_PARSE_ERROR                        | SQL 语句分析错误                |
| 701  | SEMANTIC_ERROR                         | SQL 语义错误                  |
| 702  | GENERATE_TIME_ZONE_ERROR               | 生成时区错误                    |
| 703  | SET_TIME_ZONE_ERROR                    | 设置时区错误                    |
| 704  | QUERY_NOT_ALLOWED                      | 查询语句不允许                   |
| 705  | LOGICAL_OPERATOR_ERROR                 | 逻辑符相关错误                   |
| 706  | LOGICAL_OPTIMIZE_ERROR                 | 逻辑优化相关错误                  |
| 707  | UNSUPPORTED_FILL_TYPE                  | 不支持的填充类型                  |
| 708  | QUERY_PROCESS_ERROR                    | 查询处理相关错误                  |
| 709  | MPP_MEMORY_NOT_ENOUGH                  | MPP 框架中任务执行内存不足           |
| 710  | CLOSE_OPERATION_ERROR                  | 关闭操作错误                    |
| 711  | TSBLOCK_SERIALIZE_ERROR                | TsBlock 序列化错误             |
| 712  | INTERNAL_REQUEST_TIME_OUT              | MPP 操作超时                  |
| 713  | INTERNAL_REQUEST_RETRY_ERROR           | 内部操作重试失败                  |
| 714  | NO_SUCH_QUERY                          | 查询不存在                     |
| 715  | QUERY_WAS_KILLED                       | 查询执行时被终止                  |
| 800  | UNINITIALIZED_AUTH_ERROR               | 授权模块未初始化                  |
| 801  | WRONG_LOGIN_PASSWORD                   | 用户名或密码错误                  |
| 802  | NOT_LOGIN                              | 没有登录                      |
| 803  | NO_PERMISSION                          | 没有操作权限                    |
| 804  | USER_NOT_EXIST                         | 用户不存在                     |
| 805  | USER_ALREADY_EXIST                     | 用户已存在                     |
| 806  | USER_ALREADY_HAS_ROLE                  | 用户拥有对应角色                  |
| 807  | USER_NOT_HAS_ROLE                      | 用户未拥有对应角色                 |
| 808  | ROLE_NOT_EXIST                         | 角色不存在                     |
| 809  | ROLE_ALREADY_EXIST                     | 角色已存在                     |
| 810  | ALREADY_HAS_PRIVILEGE                  | 已拥有对应权限                   |
| 811  | NOT_HAS_PRIVILEGE                      | 未拥有对应权限                   |
| 812  | CLEAR_PERMISSION_CACHE_ERROR           | 清空权限缓存失败                  |
| 813  | UNKNOWN_AUTH_PRIVILEGE                 | 未知权限                      |
| 814  | UNSUPPORTED_AUTH_OPERATION             | 不支持的权限操作                  |
| 815  | AUTH_IO_EXCEPTION                      | 权限模块IO异常                  |
| 900  | MIGRATE_REGION_ERROR                   | Region 迁移失败               |
| 901  | CREATE_REGION_ERROR                    | 创建 region 失败              |
| 902  | DELETE_REGION_ERROR                    | 删除 region 失败              |
| 903  | PARTITION_CACHE_UPDATE_ERROR           | 更新分区缓存失败                  |
| 904  | CONSENSUS_NOT_INITIALIZED              | 共识层未初始化，不能提供服务            |
| 905  | REGION_LEADER_CHANGE_ERROR             | Region leader 迁移失败        |
| 906  | NO_AVAILABLE_REGION_GROUP              | 无法找到可用的 Region 副本组        |
| 907  | LACK_DATA_PARTITION_ALLOCATION         | 调用创建数据分区方法的返回结果里缺少信息      |
| 1000 | DATANODE_ALREADY_REGISTERED            | DataNode 在集群中已经注册         |
| 1001 | NO_ENOUGH_DATANODE                     | DataNode 数量不足，无法移除节点或创建副本 |
| 1002 | ADD_CONFIGNODE_ERROR                   | 新增 ConfigNode 失败          |
| 1003 | REMOVE_CONFIGNODE_ERROR                | 移除 ConfigNode 失败          |
| 1004 | DATANODE_NOT_EXIST                     | 此 DataNode 不存在            |
| 1005 | DATANODE_STOP_ERROR                    | DataNode 关闭失败             |
| 1006 | REMOVE_DATANODE_ERROR                  | 移除 datanode 失败            |
| 1007 | REGISTER_DATANODE_WITH_WRONG_ID        | 注册的 DataNode 中有错误的注册id    |
| 1008 | CAN_NOT_CONNECT_DATANODE               | 连接 DataNode 失败            |
| 1100 | LOAD_FILE_ERROR                        | 加载文件错误                    |
| 1101 | LOAD_PIECE_OF_TSFILE_ERROR             | 加载 TsFile 片段异常            |
| 1102 | DESERIALIZE_PIECE_OF_TSFILE_ERROR      | 反序列化 TsFile 片段异常          |
| 1103 | SYNC_CONNECTION_ERROR                  | 同步连接错误                    |
| 1104 | SYNC_FILE_REDIRECTION_ERROR            | 同步文件时重定向异常                |
| 1105 | SYNC_FILE_ERROR                        | 同步文件异常                    |
| 1106 | CREATE_PIPE_SINK_ERROR                 | 创建 PIPE Sink 失败           |
| 1107 | PIPE_ERROR                             | PIPE 异常                   |
| 1108 | PIPESERVER_ERROR                       | PIPE server 异常            |
| 1109 | VERIFY_METADATA_ERROR                  | 校验元数据失败                   |
| 1200 | UDF_LOAD_CLASS_ERROR                   | UDF 加载类异常                 |
| 1201 | UDF_DOWNLOAD_ERROR                     | 无法从 ConfigNode 下载 UDF     |
| 1202 | CREATE_UDF_ON_DATANODE_ERROR           | 在 DataNode 创建 UDF 失败      |
| 1203 | DROP_UDF_ON_DATANODE_ERROR             | 在 DataNode 卸载 UDF 失败      |
| 1300 | CREATE_TRIGGER_ERROR                   | ConfigNode 创建 Trigger 失败  |
| 1301 | DROP_TRIGGER_ERROR                     | ConfigNode 删除 Trigger 失败  |
| 1302 | TRIGGER_FIRE_ERROR                     | 触发器执行错误                   |
| 1303 | TRIGGER_LOAD_CLASS_ERROR               | 触发器加载类异常                  |
| 1304 | TRIGGER_DOWNLOAD_ERROR                 | 从 ConfigNode 下载触发器异常      |
| 1305 | CREATE_TRIGGER_INSTANCE_ERROR          | 创建触发器实例异常                 |
| 1306 | ACTIVE_TRIGGER_INSTANCE_ERROR          | 激活触发器实例异常                 |
| 1307 | DROP_TRIGGER_INSTANCE_ERROR            | 删除触发器实例异常                 |
| 1308 | UPDATE_TRIGGER_LOCATION_ERROR          | 更新有状态的触发器所在 DataNode 异常   |
| 1400 | NO_SUCH_CQ                             | CQ 任务不存在                  |
| 1401 | CQ_ALREADY_ACTIVE                      | CQ 任务已激活                  |
| 1402 | CQ_AlREADY_EXIST                       | CQ 任务已存在                  |
| 1403 | CQ_UPDATE_LAST_EXEC_TIME_ERROR         | CQ 更新上一次执行时间失败            |

> 在最新版本中，我们重构了 IoTDB 的异常类。通过将错误信息统一提取到异常类中，并为所有异常添加不同的错误代码，从而当捕获到异常并引发更高级别的异常时，错误代码将保留并传递，以便用户了解详细的错误原因。
除此之外，我们添加了一个基础异常类“ProcessException”，由所有异常扩展。
