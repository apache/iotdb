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

package org.apache.iotdb.rest.i18n;

public final class RestMessages {

  // --- RestService ---
  public static final String REST_SERVICE_START_FAILED = "RestService 启动失败：{}";
  public static final String REST_SERVICE_START_SUCCESS = "RestService 启动成功";
  public static final String REST_SERVICE_STOP_FAILED = "RestService 停止失败：{}";

  // --- StatementConstructionHandler (v1 / v2 / table) ---
  public static final String INVALID_INPUT = "无效输入：";

  // --- RequestValidationHandler (v2) ---
  public static final String PREFIX_PATHS_EMPTY = "prefix_paths 不能为空";

  private RestMessages() {}
}
