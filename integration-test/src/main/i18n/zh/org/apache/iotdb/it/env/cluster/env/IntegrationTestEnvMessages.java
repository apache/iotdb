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

  public static final String
      LOG_NODE_ARG_S_STOP_SCRIPT_DID_NOT_EXIT_WITHIN_ARG_MS_KILLING_STOP_SCRIPT_9393F866 =
          "节点 {} 的停止脚本在 {} ms 内未退出，正在强制终止停止脚本";

  public static final String LOG_NODE_ARG_S_STOP_SCRIPT_EXITED_WITH_CODE_ARG_E5C267CF =
      "节点 {} 的停止脚本退出，退出码为 {}";

  public static final String
      LOG_FAILED_TO_RUN_STOP_SCRIPT_FOR_NODE_ARG_FALLING_BACK_TO_DIRECT_PROCESS_TERMINATION_13E3FD59 =
          "执行节点 {} 的停止脚本失败，回退到直接终止进程";

  public static final String
      LOG_NODE_ARG_DID_NOT_EXIT_WITHIN_ARG_MS_FORCIBLY_TERMINATING_PROCESS_TREE_65FA74DB =
          "节点 {} 在 {} ms 内未退出，正在强制终止进程树";

  public static final String LOG_FAILED_TO_CONFIRM_EXIT_OF_NODE_ARG_SURVIVING_PIDS_ARG_CBF3E211 =
      "无法确认节点 {} 已退出（仍在运行的 PID：{}）";

  public static final String LOG_IN_TEST_ARG_ARG_STOPPED_57DB922B = "在测试 {} 中，{} 已停止。";

  public static final String
      LOG_NODE_ARG_IS_STILL_ALIVE_AFTER_STOPFORCIBLY_SKIPPING_DIRECTORY_CLEANUP_AND_LOCK_FILE_REMOVAL_55F75817 =
          "节点 {} 在 stopForcibly 后仍在运行，跳过目录清理和锁文件删除";
}
