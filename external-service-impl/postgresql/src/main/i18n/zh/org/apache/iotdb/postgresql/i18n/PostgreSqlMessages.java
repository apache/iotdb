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

package org.apache.iotdb.postgresql.i18n;

/** Log and exception messages of the PostgreSQL service (zh). */
public final class PostgreSqlMessages {

  private PostgreSqlMessages() {}

  public static final String EXCEPTION_ARG_ARG_IS_NOT_A_LOOPBACK_ADDRESS_AND_NEEDS_TLS_BUT_NO_KEY_STORE_IS_CONFIGURED_KEY_STORE_PATH_723B55BF =
      "%s=%s 不是回环地址，需要 TLS，但未配置密钥库（key_store_path）";

  public static final String EXCEPTION_BINARY_FORMAT_OF_TYPE_ARG_IS_NOT_SUPPORTED_116397E1 =
      "不支持类型 %s 的二进制格式";

  public static final String EXCEPTION_CANNOT_INSPECT_ARG_A8699317 =
      "无法检查 %s";

  public static final String EXCEPTION_CANNOT_OPEN_ARG_ARG_ARG_ARG_63428D45 =
      "无法打开 %s:%s（%s）：%s";

  public static final String EXCEPTION_INTERRUPTED_WHILE_OPENING_ARG_ARG_F80AAF97 =
      "打开 %s:%s 时被中断";

  public static final String EXCEPTION_INVALID_SETTINGS_ARG_4D6B78F5 =
      "无效的设置：%s";

  public static final String EXCEPTION_INVALID_VALUE_FOR_PARAMETER_STATEMENT_TIMEOUT_ARG_30718625 =
      "参数 \"statement_timeout\" 的值无效：\"%s\"";

  public static final String EXCEPTION_NOTHING_IS_WRITTEN_WHILE_SKIPPING_5DE21CBD =
      "跳过时不写入任何内容";

  public static final String EXCEPTION_NOT_A_BOOLEAN_ARG_BDDA84BC =
      "不是布尔值：%s";

  public static final String EXCEPTION_NO_BINARY_FORMAT_FOR_ARG_62465CF6 =
      "%s 没有二进制格式";

  public static final String EXCEPTION_NO_TEXT_FORMAT_FOR_ARG_F368DE42 =
      "%s 没有文本格式";

  public static final String EXCEPTION_ONLY_THE_HEX_FORMAT_OF_BYTEA_IS_SUPPORTED_86198CC1 =
      "仅支持 bytea 的十六进制格式";

  public static final String EXCEPTION_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_A5563154 =
      "PostgreSQL 端点未启动：%s";

  public static final String EXCEPTION_THE_JVM_SUPPORTS_NONE_OF_THE_ALLOWED_TLS_PROTOCOLS_ARG_OR_CIPHER_SUITES_833995C7 =
      "JVM 不支持任何允许的 TLS 协议 %s 或密码套件";

  public static final String EXCEPTION_THE_KEY_STORE_ARG_CANNOT_BE_USED_ARG_5908F0BB =
      "密钥库 %s 无法使用：%s";

  public static final String EXCEPTION_THE_KEY_STORE_ARG_IS_NOT_READABLE_A4A4A28C =
      "密钥库 %s 不可读";

  public static final String LOG_POSTGRESQL_CLIENT_ARG_DISCONNECTED_WHILE_RESULTS_WERE_SENT_F0C48BEA =
      "PostgreSQL 客户端 {} 在发送结果时断开连接";

  public static final String LOG_POSTGRESQL_CLIENT_ARG_STOPPED_READING_RESULTS_FOR_ARG_CLOSING_THE_CONNECTION_0A30373B =
      "PostgreSQL 客户端 {} 已停止读取结果 {}；正在关闭连接";

  public static final String LOG_POSTGRESQL_CONNECTION_ARG_FAILED_C649F2E0 =
      "PostgreSQL 连接 {} 失败";

  public static final String LOG_POSTGRESQL_ENDPOINT_CANNOT_READ_ARG_ARG_B4423CA2 =
      "PostgreSQL 端点：无法读取 {}：{}";

  public static final String LOG_POSTGRESQL_ENDPOINT_LISTENING_ON_ARG_9DBA69A9 =
      "PostgreSQL 端点正在监听 {}";

  public static final String LOG_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_626B180C =
      "PostgreSQL 端点未启动：{}";

  public static final String LOG_POSTGRESQL_LOGIN_FAILED_7130B38D =
      "PostgreSQL 登录失败";

  public static final String LOG_POSTGRESQL_MESSAGE_ARG_OF_ARG_FAILED_EB758E37 =
      "PostgreSQL 消息 '{}'（用户 {}）处理失败";

  public static final String LOG_POSTGRESQL_SESSION_OF_ARG_WAS_ENDED_CLOSING_THE_CONNECTION_85AD9B1D =
      "{} 的 PostgreSQL 会话已结束；正在关闭连接";

  public static final String LOG_REFUSED_POSTGRESQL_STATEMENT_ARG_ARG_602B9945 =
      "已拒绝 PostgreSQL 语句（{}）：{}";

  public static final String LOG_THE_DATABASE_ARG_IS_HIDDEN_FROM_POSTGRESQL_CLIENTS_ITS_NAME_IS_A_RESERVED_POSTGRESQL_SCHEMA_B92A6ECA =
      "数据库 {} 对 PostgreSQL 客户端隐藏：其名称是保留的 PostgreSQL schema";

  public static final String LOG_UNKNOWN_POSTGRESQL_PARAMETER_ARG_ACCEPTED_AND_IGNORED_C2408108 =
      "未知的 PostgreSQL 参数 {} 已接受并忽略";
}
