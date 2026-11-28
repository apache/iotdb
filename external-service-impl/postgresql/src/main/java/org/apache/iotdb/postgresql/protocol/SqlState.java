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

package org.apache.iotdb.postgresql.protocol;

/** SQLSTATE codes the adapter sends. */
public final class SqlState {

  public static final String READ_ONLY_SQL_TRANSACTION = "25006";
  public static final String FEATURE_NOT_SUPPORTED = "0A000";
  public static final String SYNTAX_ERROR = "42601";
  public static final String UNDEFINED_TABLE = "42P01";
  public static final String UNDEFINED_COLUMN = "42703";
  public static final String INVALID_SCHEMA_NAME = "3F000";
  public static final String INSUFFICIENT_PRIVILEGE = "42501";
  public static final String QUERY_CANCELED = "57014";
  public static final String TOO_MANY_CONNECTIONS = "53300";
  public static final String OUT_OF_MEMORY = "53200";
  public static final String INTERNAL_ERROR = "XX000";
  public static final String INVALID_PASSWORD = "28P01";
  public static final String INVALID_AUTHORIZATION = "28000";
  public static final String PROTOCOL_VIOLATION = "08P01";
  public static final String CONNECTION_FAILURE = "08006";
  public static final String ADMIN_SHUTDOWN = "57P01";
  public static final String INVALID_PARAMETER_VALUE = "22023";
  public static final String INVALID_CURSOR_NAME = "34000";
  public static final String DUPLICATE_CURSOR = "42P03";
  public static final String DUPLICATE_PREPARED_STATEMENT = "42P05";
  public static final String INVALID_SQL_STATEMENT_NAME = "26000";
  public static final String INVALID_TEXT_REPRESENTATION = "22P02";
  public static final String CHARACTER_NOT_IN_REPERTOIRE = "22021";
  public static final String SYNTAX_OR_ACCESS_RULE = "42000";
  public static final String NUMERIC_VALUE_OUT_OF_RANGE = "22003";
  public static final String DIVISION_BY_ZERO = "22012";
  public static final String DATETIME_FIELD_OVERFLOW = "22008";
  public static final String INSUFFICIENT_RESOURCES = "53000";

  private SqlState() {}
}
