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

package org.apache.iotdb.commons.i18n;

public final class SchemaMessages {

  // MNode messages
  public static final String MEASUREMENT_NODE_CANNOT_GET_CHILD =
      "current node %s is a MeasurementMNode, can not get child %s";
  public static final String MEASUREMENT_NODE_CANNOT_GET_CHILD_LOG =
      "current node {} is a MeasurementMNode, can not get child {}";
  public static final String UNDEFINED_MNODE_TYPE = "Undefined MNode type %s";
  public static final String WRONG_MNODE_TYPE = "Wrong MNode Type";
  public static final String WRONG_NODE_TYPE = "Wrong node type";
  public static final String INVALID_INPUT = "Invalid input: %s";

  // Template messages
  public static final String PATH_ALREADY_EXISTS = "path already exists";

  // Table messages
  public static final String CANNOT_REMOVE_TAG_COLUMN = "Cannot remove an tag column: %s";
  public static final String UNKNOWN_TABLE_UPDATE_OPERATION_TYPE =
      "Unknown table update operation type%s";

  // Column messages
  public static final String UNSUPPORTED_COLUMN_TYPE_IN_TSFILE =
      "Unsupported column type in TsFile: %s";
  public static final String UNKNOWN_COLUMN_TYPE = "Unknown column type: %s";
  public static final String SIZE_SHOULD_NOT_BE_NEGATIVE_ONE = "size should not be -1";

  // View messages
  public static final String INVALID_VIEW_EXPRESSION_TYPE = "Invalid viewExpression type: %s";
  public static final String UNSUPPORTED_METHOD_FOR_LOGICAL_VIEW_SCHEMA =
      "unsupported method for LogicalViewSchema";
  public static final String CANNOT_CALCULATE_VIEW_SCHEMA_SIZE =
      "Can not calculate the size of view schemaengine before serializing.";
  public static final String VIEW_STORES_ILLEGAL_PATH =
      "View with measurementID [%s] is broken. It stores illegal path [%s].";
  public static final String UNEXPECTED_VALUE_IN_LIKE_VIEW_EXPRESSION =
      "Unexpected value in LikeViewExpression: %s";

  // Filter messages
  public static final String UNSUPPORTED_SCHEMA_FILTER_TYPE =
      "Unsupported schema filter type: %s";
  public static final String INPUT_SINGLE_FILTER_MUST_BE_DEVICE_ID_FILTER =
      "Input single filter must be DeviceIdFilter";

  // TTL messages
  public static final String NOT_TTL_RULE_SET_FOR = "Not TTL rule set for %s";

  private SchemaMessages() {}

  public static final String PATH_DUPLICATED = "Path duplicated: ";
  public static final String UNKNOWN_TABLE_UPDATE_OP_TYPE = "Unknown table update operation type";
  public static final String SCHEMA_INVALID_INPUT = "Invalid input: ";

  public static final String TOO_MANY_DOTS_IN_TABLE_NAME =
      "Too many dots in table name: %s";

  public static final String S_IS_NULL =
      "%s is null";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DATABASE_MUST_SPECIFIED_SESSION_DATABASE_NOT_SET_CBF6F21F = "Database must be specified when session database is not set";
  public static final String MESSAGE_NOT_TTL_RULE_669A7BA4 = "Not TTL rule";
  public static final String MESSAGE_SET_4BE61E08 = " set for ";
  public static final String EXCEPTION_FAILED_GET_ORIGINAL_DATABASE_BECAUSE_ARG_NULL_TABLE_ARG_5AE54514 = "Failed to get the original database, because the %s is null for table %s";
  public static final String EXCEPTION_FAILED_PARSE_TREE_VIEW_STRING_ARG_CONVERT_IDEVICEID_6E735586 = "Failed to parse the tree view string %s when convert to IDeviceID";
  public static final String EXCEPTION_SESSION_IS_NULL_6CF0F47D = "session is null";
  public static final String EXCEPTION_NAME_IS_NULL_C8B35959 = "name is null";
  public static final String EXCEPTION_ARG_IS_NOT_LOWERCASE_COLON_ARG_D78298F6 = "%s is not lowercase: %s";
  public static final String EMPTY_MESSAGE = "";
  public static final String EXCEPTION_OPERATOR_IS_NULL_F5BB9F59 = "operator is null";
  public static final String EXCEPTION_VALUE_IS_NULL_192F6BFF = "value is null";

}
