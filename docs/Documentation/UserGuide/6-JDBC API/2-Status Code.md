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

**Status Code** is introduced in the latest version. For example, as IoTDB requires registering the time series first before writing data, a kind of solution is:

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

With Status Code, instead of writing codes like `if (e.getErrorMessage().contains("exist"))`, we can simply use `e.getStatusType().getCode() == TSStatusType.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`.

Here is a list of Status Code and related message:

|Status Code|Status Type|Status Message|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|301|TIMESERIES_NOT_EXIST_ERROR|Timeseries does not exist|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|Unsupported fetch metadata operation|
|303|FETCH_METADATA_ERROR|Failed to fetch metadata|
|304|CHECK_FILE_LEVEL_ERROR|Meet error while checking file level|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|500|INTERNAL_SERVER_ERROR|Internal server error|
|600|WRONG_LOGIN_PASSWORD_ERROR|Username or password is wrong|
|601|NOT_LOGIN_ERROR|Has not logged in|
|602|NO_PERMISSION_ERROR|No permissions for this operation|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|
