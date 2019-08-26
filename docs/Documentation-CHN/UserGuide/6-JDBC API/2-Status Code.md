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

# Chapter6: JDBC API

## Status Code

在最新版本中引入了**状态码**这一概念。例如，因为IoTDB需要在写入数据之前首先注册时间序列，一种可能的解决方案是：

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

利用状态码，我们就可以不必写诸如`if (e.getErrorMessage().contains("exist"))`的代码，只需要使用`e.getStatusType() == TSStatusType.TIME_SERIES_NOT_EXIST_ERROR`。

这里是状态码和相对应信息的列表：

|状态码|状态类型|状态信息|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|301|TIMESERIES_NOT_EXIST_ERROR|时间序列不存在|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|不支持的获取元数据操作|
|303|FETCH_METADATA_ERROR|获取元数据失败|
|304|CHECK_FILE_LEVEL_ERROR|检查文件层级错误|
|400|EXECUTE_STATEMENT_ERROR|执行语句错误|
|401|SQL_PARSE_ERROR|SQL语句分析错误|
|402|GENERATE_TIME_ZONE_ERROR|生成时区错误|
|403|SET_TIME_ZONE_ERROR|设置时区错误|
|500|INTERNAL_SERVER_ERROR|服务器内部错误|
|600|WRONG_LOGIN_PASSWORD_ERROR|用户名或密码错误|
|601|NOT_LOGIN_ERROR|没有登录|
|602|NO_PERMISSION_ERROR|没有操作权限|
|603|UNINITIALIZED_AUTH_ERROR|授权人未初始化|
