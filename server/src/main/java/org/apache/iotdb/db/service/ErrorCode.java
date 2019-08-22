/**
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

package org.apache.iotdb.db.service;

public class ErrorCode {
  public static final int TIME_SERIES_NOT_EXIST_ERROR = 301;
  public static final int FETCH_METADATA_ERROR = 303;
  public static final int CHECK_FILE_LEVEL_ERROR = 304;
  public static final int EXECUTE_STATEMENT_ERROR = 400;
  public static final int SQL_PARSE_ERROR = 401;
  public static final int TIME_ZONE_ERROR = 402;
  public static final int INTERNAL_SERVER_ERROR = 500;
  public static final int WRONG_LOGIN_PASSWORD_ERROR = 600;
  public static final int NOT_LOGIN_ERROR = 601;
  public static final int NO_PERMISSION_ERROR = 602;
  public static final int UNINITIALIZED_AUTH_ERROR = 603;
}
