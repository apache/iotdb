/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.it.env.cluster.env;

/** Localized diagnostics for the integration-test readiness deadlines. */
public final class IntegrationTestEnvMessages {

  private IntegrationTestEnvMessages() {}

  public static final String EXCEPTION_CLUSTER_STATUS_CHECK_FAILED_RETRY_BUDGET_ARG_SECONDS_AEB4010E =
      "集群状态检查失败（重试时间预算：%d 秒）";

  public static final String EXCEPTION_JDBC_CONNECTIONS_TO_DATANODES_ARE_NOT_READY_RETRY_BUDGET_ARG_SECONDS_ENDPOINTS_ARG_LASTCONNECTIONFAILURES_ARG_LOGDIRS_ARG_D822C35C =
      "连接 DataNode 的 JDBC 连接尚未就绪（重试时间预算：%d 秒）。endpoints=%s, lastConnectionFailures=%s, logDirs=%s";

  public static final String EXCEPTION_FAILED_TO_GET_CONNECTION_TO_CONFIGNODE_ARG_DF03F50F =
      "无法连接 ConfigNode %s";
}
