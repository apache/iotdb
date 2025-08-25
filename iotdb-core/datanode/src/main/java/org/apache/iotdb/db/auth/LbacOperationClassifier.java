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
 * Operation classifier for determining whether SQL statements are READ, WRITE, or BOTH operations.
 * Only database-related operations are classified for LBAC (Label-Based Access Control). Based on
 * section 2.8 of the LBAC requirements document.
 */
public class LbacOperationClassifier {

  /** Operation types for LBAC classification */
  public enum OperationType {
    READ, // Read-only operations
    WRITE, // Write-only operations
    BOTH // Some complex operations may involve both read and write
  }

  /** Set of Statement types that are considered WRITE operations (Database-related only) */
  private static final Set<StatementType> WRITE_OPERATIONS =
      EnumSet.of(
          // Data insertion operations
          StatementType.INSERT,
          StatementType.BATCH_INSERT,
          StatementType.BATCH_INSERT_ROWS,
          StatementType.BATCH_INSERT_ONE_DEVICE,
          StatementType.MULTI_BATCH_INSERT,

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
          StatementType.DELETE_TIME_SERIES,
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

          // Security label operations (Database-related)
          StatementType.ALTER_DATABASE_SECURITY_LABEL);

  /** Set of Statement types that are considered READ operations (Database-related only) */
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

          // Metadata query operations (Database-related)
          StatementType.SHOW,
          StatementType.SHOW_SCHEMA_TEMPLATE,
          StatementType.SHOW_NODES_IN_SCHEMA_TEMPLATE,
          StatementType.SHOW_PATH_SET_SCHEMA_TEMPLATE,
          StatementType.SHOW_PATH_USING_SCHEMA_TEMPLATE,
          StatementType.SHOW_DATABASE_SECURITY_LABEL,

          // Schema fetch operations
          StatementType.FETCH_SCHEMA,

          // Index query operations
          StatementType.QUERY_INDEX);

  /** Set of Statement types that involve both READ and WRITE operations */
  private static final Set<StatementType> BOTH_OPERATIONS =
      EnumSet.of(
          // SELECT INTO operation - reads from source and writes to target
          StatementType.SELECT_INTO);

  /** Private constructor - utility class should not be instantiated */
  private LbacOperationClassifier() {
    // Utility class
  }

  /**
   * Classify the given Statement as READ, WRITE, or BOTH operation. Only database-related
   * operations are classified for LBAC.
   *
   * @param statement SQL statement to classify
   * @return operation type (READ/WRITE/BOTH) or null if not a database-related operation
   */
  public static OperationType classifyOperation(Statement statement) {
    if (statement == null) {
      return null;
    }

    StatementType statementType = statement.getType();
    if (statementType == null) {
      return classifySpecialCases(statement, StatementType.NULL);
    }

    // Check if this is a database-related operation for LBAC
    if (!isLbacRelevantOperation(statement)) {
      return null; // Not subject to LBAC
    }

    // Check BOTH operations first (most specific)
    if (BOTH_OPERATIONS.contains(statementType)) {
      return OperationType.BOTH;
    }

    // Check WRITE operations
    if (WRITE_OPERATIONS.contains(statementType)) {
      return OperationType.WRITE;
    }

    // Check READ operations
    if (READ_OPERATIONS.contains(statementType)) {
      return OperationType.READ;
    }

    // Handle special cases
    return classifySpecialCases(statement, statementType);
  }

  /**
   * Check if the given statement is a database-related operation that should be subject to LBAC.
   *
   * @param statement SQL statement to check
   * @return true if it's a database-related operation for LBAC, false otherwise
   */
  public static boolean isLbacRelevantOperation(Statement statement) {
    if (statement == null) {
      return false;
    }

    StatementType statementType = statement.getType();
    if (statementType == null) {
      return false;
    }

    // Check if the statement type is in any of our database-related operation sets
    return READ_OPERATIONS.contains(statementType)
        || WRITE_OPERATIONS.contains(statementType)
        || BOTH_OPERATIONS.contains(statementType);
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
        // NULL type: treat as read operation for safety
      case NULL:
        return OperationType.READ;

        // Default case: treat as write operation for safety
      default:
        return OperationType.WRITE;
    }
  }
}
