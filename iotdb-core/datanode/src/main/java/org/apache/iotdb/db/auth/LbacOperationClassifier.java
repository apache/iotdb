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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;

import java.util.EnumSet;
import java.util.Set;

/**
 * Operation classifier for determining whether SQL statements are READ or WRITE operations. Based
 * on section 2.8 of the LBAC requirements document.
 */
public class LbacOperationClassifier {

  /** Enum for operation types */
  public enum OperationType {
    READ,
    WRITE,
    BOTH // Some complex operations may involve both read and write
  }

  /** Set of Statement types that are considered WRITE operations */
  private static final Set<StatementType> WRITE_OPERATIONS =
      EnumSet.of(
          // Data insertion operations
          StatementType.INSERT,
          StatementType.BATCH_INSERT,
          StatementType.BATCH_INSERT_ROWS,
          StatementType.BATCH_INSERT_ONE_DEVICE,
          StatementType.MULTI_BATCH_INSERT,
          StatementType.SELECT_INTO,

          // Database/storage group management operations
          StatementType.STORAGE_GROUP_SCHEMA,
          StatementType.DELETE_STORAGE_GROUP,

          // Time series management operations
          StatementType.CREATE_TIME_SERIES,
          StatementType.CREATE_ALIGNED_TIME_SERIES,
          StatementType.CREATE_MULTI_TIME_SERIES,
          StatementType.ALTER_TIME_SERIES,
          StatementType.DELETE_TIME_SERIES,
          StatementType.CHANGE_ALIAS,
          StatementType.CHANGE_TAG_OFFSET,
          StatementType.INTERNAL_CREATE_TIMESERIES,
          StatementType.INTERNAL_CREATE_MULTI_TIMESERIES,

          // Data deletion operations
          StatementType.DELETE,
          StatementType.DELETE_PARTITION,

          // Template operations
          StatementType.CREATE_TEMPLATE,
          StatementType.SET_TEMPLATE,
          StatementType.ACTIVATE_TEMPLATE,
          StatementType.ALTER_TEMPLATE,
          StatementType.UNSET_TEMPLATE,
          StatementType.PRUNE_TEMPLATE,
          StatementType.APPEND_TEMPLATE,
          StatementType.DROP_TEMPLATE,
          StatementType.DEACTIVATE_TEMPLATE,
          StatementType.BATCH_ACTIVATE_TEMPLATE,
          StatementType.INTERNAL_BATCH_ACTIVATE_TEMPLATE,

          // Logical view operations
          StatementType.CREATE_LOGICAL_VIEW,
          StatementType.ALTER_LOGICAL_VIEW,
          StatementType.DELETE_LOGICAL_VIEW,
          StatementType.RENAME_LOGICAL_VIEW,

          // Trigger operations
          StatementType.CREATE_TRIGGER,
          StatementType.DROP_TRIGGER,

          // Function operations
          StatementType.CREATE_FUNCTION,
          StatementType.DROP_FUNCTION,

          // Pipe operations
          StatementType.CREATE_PIPE,
          StatementType.ALTER_PIPE,
          StatementType.START_PIPE,
          StatementType.STOP_PIPE,
          StatementType.DROP_PIPE,
          StatementType.CREATE_PIPESINK,
          StatementType.DROP_PIPESINK,
          StatementType.CREATE_PIPEPLUGIN,
          StatementType.DROP_PIPEPLUGIN,

          // Topic and subscription operations
          StatementType.CREATE_TOPIC,
          StatementType.DROP_TOPIC,
          StatementType.DROP_SUBSCRIPTION,

          // Continuous query operations
          StatementType.CREATE_CONTINUOUS_QUERY,
          StatementType.DROP_CONTINUOUS_QUERY,

          // System management operations
          StatementType.TTL,
          StatementType.FLUSH,
          StatementType.MERGE,
          StatementType.FULL_MERGE,
          StatementType.CLEAR_CACHE,
          StatementType.LOAD_CONFIGURATION,
          StatementType.CREATE_SCHEMA_SNAPSHOT,
          StatementType.SET_SPACE_QUOTA,
          StatementType.SET_THROTTLE_QUOTA,
          StatementType.SET_CONFIGURATION,
          StatementType.SET_SYSTEM_MODE,
          StatementType.START_REPAIR_DATA,
          StatementType.STOP_REPAIR_DATA,
          StatementType.SETTLE,

          // Data loading operations
          StatementType.LOAD_DATA,
          StatementType.LOAD_FILES,
          StatementType.REMOVE_FILE,
          StatementType.UNLOAD_FILE,

          // User and role management operations
          StatementType.CREATE_USER,
          StatementType.DELETE_USER,
          StatementType.MODIFY_PASSWORD,
          StatementType.GRANT_USER_PRIVILEGE,
          StatementType.REVOKE_USER_PRIVILEGE,
          StatementType.GRANT_USER_ROLE,
          StatementType.REVOKE_USER_ROLE,
          StatementType.CREATE_ROLE,
          StatementType.DELETE_ROLE,
          StatementType.GRANT_ROLE_PRIVILEGE,
          StatementType.REVOKE_ROLE_PRIVILEGE,
          StatementType.GRANT_WATERMARK_EMBEDDING,
          StatementType.REVOKE_WATERMARK_EMBEDDING,

          // Security label operations
          StatementType.ALTER_DATABASE_SECURITY_LABEL);

  /** Set of Statement types that are considered READ operations */
  private static final Set<StatementType> READ_OPERATIONS =
      EnumSet.of(
          // Data query operations
          StatementType.QUERY,
          StatementType.LAST,
          StatementType.GROUP_BY_TIME,
          StatementType.GROUP_BY_FILL,
          StatementType.AGGREGATION,
          StatementType.FILL,
          StatementType.UDAF,
          StatementType.UDTF,
          StatementType.COUNT,

          // Metadata query operations
          StatementType.SHOW,
          StatementType.SHOW_MERGE_STATUS,
          StatementType.SHOW_QUERIES,
          StatementType.SHOW_QUERY_RESOURCE,
          StatementType.SHOW_SCHEMA_TEMPLATE,
          StatementType.SHOW_NODES_IN_SCHEMA_TEMPLATE,
          StatementType.SHOW_PATH_SET_SCHEMA_TEMPLATE,
          StatementType.SHOW_PATH_USING_SCHEMA_TEMPLATE,
          StatementType.SHOW_TRIGGERS,
          StatementType.SHOW_PIPES,
          StatementType.SHOW_PIPEPLUGINS,
          StatementType.SHOW_TOPICS,
          StatementType.SHOW_SUBSCRIPTIONS,
          StatementType.SHOW_CONTINUOUS_QUERIES,
          StatementType.SHOW_SPACE_QUOTA,
          StatementType.SHOW_THROTTLE_QUOTA,

          // User and role query operations
          StatementType.LIST_USER,
          StatementType.LIST_ROLE,
          StatementType.LIST_USER_PRIVILEGE,
          StatementType.LIST_ROLE_PRIVILEGE,
          StatementType.LIST_USER_ROLES,
          StatementType.LIST_ROLE_USERS,

          // Schema fetch operations
          StatementType.FETCH_SCHEMA);

  /** Private constructor - utility class should not be instantiated */
  private LbacOperationClassifier() {
    // empty constructor
  }

  /**
   * Classify the given Statement as READ, WRITE, or BOTH operation
   *
   * @param statement SQL statement to classify
   * @return operation type (READ/WRITE/BOTH)
   */
  public static OperationType classifyOperation(Statement statement) {
    if (statement == null) {
      throw new IllegalArgumentException("Statement cannot be null");
    }

    StatementType statementType = statement.getType();

    // First check if it's explicitly a write operation
    if (WRITE_OPERATIONS.contains(statementType)) {
      return OperationType.WRITE;
    }

    // Then check if it's explicitly a read operation
    if (READ_OPERATIONS.contains(statementType)) {
      return OperationType.READ;
    }

    // Handle special cases with specific logic
    return classifySpecialCases(statement, statementType);
  }

  /**
   * Handle special cases for operation classification
   *
   * @param statement SQL statement to classify
   * @param statementType statement type
   * @return operation type
   */
  private static OperationType classifySpecialCases(
      Statement statement, StatementType statementType) {
    switch (statementType) {
        // Index operations: treat as write operations for simplicity
      case CREATE_INDEX:
      case DROP_INDEX:
        return OperationType.WRITE;

        // Query index: treat as read operation
      case QUERY_INDEX:
        return OperationType.READ;

        // System operations: treat as write operations
      case KILL:
      case TRACING:
        return OperationType.WRITE;

        // Tree node operations: treat as write operations
      case MNODE:
      case MEASUREMENT_MNODE:
      case STORAGE_GROUP_MNODE:
      case AUTO_CREATE_DEVICE_MNODE:
        return OperationType.WRITE;

        // Author operations: treat as write operations
      case AUTHOR:
        return OperationType.WRITE;

        // Pipe enriched operations: treat as write operations
      case PIPE_ENRICHED:
        return OperationType.WRITE;

        // Default case: if cannot be clearly classified, return BOTH for caller to handle
      default:
        return OperationType.BOTH;
    }
  }

  /**
   * Check if the operation is a read operation
   *
   * @param statement SQL statement to check
   * @return true if it's a read operation, false otherwise
   */
  public static boolean isReadOperation(Statement statement) {
    OperationType type = classifyOperation(statement);
    return type == OperationType.READ;
  }

  /**
   * Check if the operation is a write operation
   *
   * @param statement SQL statement to check
   * @return true if it's a write operation, false otherwise
   */
  public static boolean isWriteOperation(Statement statement) {
    OperationType type = classifyOperation(statement);
    return type == OperationType.WRITE;
  }

  /**
   * Check if the operation involves both read and write
   *
   * @param statement SQL statement to check
   * @return true if it involves both read and write operations, false otherwise
   */
  public static boolean isBothOperation(Statement statement) {
    OperationType type = classifyOperation(statement);
    return type == OperationType.BOTH;
  }
}
