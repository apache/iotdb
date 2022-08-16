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

##  状态码

从 0.10 版本开始 IoTDB 引入了**状态码**这一概念。例如，因为 IoTDB 需要在写入数据之前首先注册时间序列，一种可能的解决方案是：

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

利用状态码，我们就可以不必写诸如`if (e.getErrorMessage().contains("exist"))`的代码，只需要使用`e.getStatusType().getCode() == TSStatusCode.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`。

这里是状态码和相对应信息的列表：

|状态码|状态类型|状态信息|
|:---|:---|:---|
|200|SUCCESS_STATUS|成功状态|
|201|STILL_EXECUTING_STATUS|仍在执行状态|
|202|INVALID_HANDLE_STATUS|无效句柄状态|
|203|INCOMPATIBLE_VERSION|版本不兼容|
|298|NODE_DELETE_FAILED_ERROR|删除节点失败|
|299|ALIAS_ALREADY_EXIST_ERROR|路径别名已经存在|
|300|PATH_ALREADY_EXIST_ERROR|路径已经存在|
|301|PATH_NOT_EXIST_ERROR|路径不存在|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|不支持的获取元数据操作|
|303|METADATA_ERROR|处理元数据错误|
|305|OUT_OF_TTL_ERROR|插入时间少于 TTL 时间边界|
|306|CONFIG_ADJUSTER|IoTDB 系统负载过大|
|307|MERGE_ERROR|合并错误|
|308|SYSTEM_CHECK_ERROR|系统检查错误|
|309|SYNC_DEVICE_OWNER_CONFLICT_ERROR|回传设备冲突错误|
|310|SYNC_CONNECTION_EXCEPTION|回传连接错误|
|311|STORAGE_GROUP_PROCESSOR_ERROR|存储组处理器相关错误|
|312|STORAGE_GROUP_ERROR|存储组相关错误|
|313|STORAGE_ENGINE_ERROR|存储引擎相关错误|
|314|TSFILE_PROCESSOR_ERROR|TsFile 处理器相关错误|
|315|PATH_ILLEGAL|路径不合法|
|316|LOAD_FILE_ERROR|加载文件错误|
|317|STORAGE_GROUP_NOT_READY| 存储组还在恢复中，还不能接受读写操作 |
|400|EXECUTE_STATEMENT_ERROR|执行语句错误|
|401|SQL_PARSE_ERROR|SQL 语句分析错误|
|402|GENERATE_TIME_ZONE_ERROR|生成时区错误|
|403|SET_TIME_ZONE_ERROR|设置时区错误|
|404|NOT_STORAGE_GROUP_ERROR|操作对象不是存储组|
|405|QUERY_NOT_ALLOWED|查询语句不允许|
|406|AST_FORMAT_ERROR|AST 格式相关错误|
|407|LOGICAL_OPERATOR_ERROR|逻辑符相关错误|
|408|LOGICAL_OPTIMIZE_ERROR|逻辑优化相关错误|
|409|UNSUPPORTED_FILL_TYPE_ERROR|不支持的填充类型|
|410|PATH_ERROR|路径相关错误|
|411|QUERY_PROCESS_ERROR|查询处理相关错误|
|412|WRITE_PROCESS_ERROR|写入相关错误|
|413|WRITE_PROCESS_REJECT|写入拒绝错误|
|414|QUERY_ID_NOT_EXIST|Query id 不存在|
|500|INTERNAL_SERVER_ERROR|服务器内部错误|
|501|CLOSE_OPERATION_ERROR|关闭操作错误|
|502|READ_ONLY_SYSTEM_ERROR|系统只读|
|503|DISK_SPACE_INSUFFICIENT_ERROR|磁盘空间不足|
|504|START_UP_ERROR|启动错误|
|505|SHUT_DOWN_ERROR|关机错误|
|506|MULTIPLE_ERROR|多行语句执行错误|
|600|WRONG_LOGIN_PASSWORD_ERROR|用户名或密码错误|
|601|NOT_LOGIN_ERROR|没有登录|
|602|NO_PERMISSION_ERROR|没有操作权限|
|603|UNINITIALIZED_AUTH_ERROR|授权人未初始化|
|700|PARTITION_NOT_READY|分区表未准备好|
|701|TIME_OUT|操作超时|
|702|NO_LEADER|Leader 找不到|
|703|UNSUPPORTED_OPERATION|不支持的操作|
|704|NODE_READ_ONLY|节点只读|
|705|CONSISTENCY_FAILURE|一致性检查失败|
|706|NO_CONNECTION|连接获取失败|
|707|NEED_REDIRECTION|需要重定向|
|800|CONFIG_ERROR|配置文件有错误项|

> 在最新版本中，我们重构了 IoTDB 的异常类。通过将错误信息统一提取到异常类中，并为所有异常添加不同的错误代码，从而当捕获到异常并引发更高级别的异常时，错误代码将保留并传递，以便用户了解详细的错误原因。
除此之外，我们添加了一个基础异常类“ProcessException”，由所有异常扩展。
