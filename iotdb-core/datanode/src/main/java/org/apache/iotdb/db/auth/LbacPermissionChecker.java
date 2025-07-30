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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.protocol.client.ConfigNodeInfo.CONFIG_REGION_ID;

/**
 * Core LBAC (Label-Based Access Control) permission checker. This class integrates operation
 * classification, label policy evaluation, and database label fetching to provide comprehensive
 * LBAC enforcement.
 */
public class LbacPermissionChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(LbacPermissionChecker.class);

  /** Operation type enumeration for LBAC checks */
  public enum OperationType {
    READ,
    WRITE,
    BOTH // Add BOTH operation type for operations like SELECT INTO
  }

  /** Private constructor - utility class should not be instantiated */
  private LbacPermissionChecker() {
    // empty constructor
  }

  /**
   * Check LBAC permissions for a statement execution. This is the main entry point for LBAC
   * permission checking.
   *
   * @param statement the statement to check permissions for
   * @param user the user attempting to execute the statement
   * @param devicePaths list of device paths involved in the statement (can be empty for non-data
   *     operations)
   * @return LbacCheckResult containing the permission check result
   */
  public static LbacCheckResult checkLbacPermission(
      Statement statement, User user, List<String> devicePaths) {

    LOGGER.debug(
        "=== LBAC CHECK START === User: {}, Statement: {}, DevicePaths: {}",
        user != null ? user.getName() : "null",
        statement != null ? statement.getClass().getSimpleName() : "null",
        devicePaths);

    if (statement == null) {
      LOGGER.debug("Statement is null, denying access");
      return LbacCheckResult.deny("Statement cannot be null");
    }

    if (user == null) {
      LOGGER.debug("User is null, denying access");
      return LbacCheckResult.deny("User cannot be null");
    }

    // Check if this is a database-related operation that should be subject to LBAC
    if (!LbacOperationClassifier.isLbacRelevantOperation(statement)) {
      LOGGER.debug(
          "Statement {} is not a database-related operation for LBAC, allowing access",
          statement.getClass().getSimpleName());
      return LbacCheckResult.allow();
    }

    // Get operation type for this statement
    LbacOperationClassifier.OperationType operationType =
        LbacOperationClassifier.classifyOperation(statement);

    if (operationType == null) {
      LOGGER.debug("Could not classify operation type, allowing access");
      return LbacCheckResult.allow();
    }

    // Check each device path using the new checkSingleDevicePath method
    for (String devicePath : devicePaths) {
      try {
        LbacCheckResult result = checkSingleDevicePath(devicePath, user.getName(), operationType);
        if (!result.isAllowed()) {
          LOGGER.debug("LBAC check failed for device path {}: {}", devicePath, result.getReason());
          return result;
        }
      } catch (Exception e) {
        LOGGER.error("Error during LBAC check for device path {}: {}", devicePath, e.getMessage());
        return LbacCheckResult.deny("LBAC check failed with error: " + e.getMessage());
      }
    }

    LOGGER.debug("LBAC check passed for all device paths");
    return LbacCheckResult.allow();
  }

  /**
   * Check LBAC permission for a single device path Implements the detailed LBAC logic as specified:
   *
   * <p>No read/write policies (both read and write policies are empty): - No label data → No LBAC
   * triggered, allow access - Has label data → No LBAC triggered, allow access
   *
   * <p>Has any policy (read/write/both exist): - No label data: - Only read policy: read operation
   * → trigger LBAC, deny access; write operation → no LBAC, allow access; both operations → trigger
   * LBAC, deny access - Only write policy: write operation → trigger LBAC, deny access; read
   * operation → no LBAC, allow access; both operations → trigger LBAC, deny access - Both read and
   * write policies: read operation → trigger LBAC, deny access; write operation → trigger LBAC,
   * deny access; both operations → trigger LBAC, deny access - Has label data: - Only read policy:
   * read operation → trigger LBAC, evaluate read policy with labels; write operation → no LBAC,
   * allow access; both operations → trigger LBAC, evaluate read policy with labels - Only write
   * policy: write operation → trigger LBAC, evaluate write policy with labels; read operation → no
   * LBAC, allow access; both operations → trigger LBAC, evaluate write policy with labels - Both
   * read and write policies: read operation → trigger LBAC, evaluate read policy with labels; write
   * operation → trigger LBAC, evaluate write policy with labels; both operations → trigger LBAC,
   * evaluate both policies with labels
   *
   * @param devicePath the device path to check
   * @param userName the username
   * @param operationType the operation type (READ/WRITE/BOTH)
   * @return LbacCheckResult for this specific device path
   */
  private static LbacCheckResult checkSingleDevicePath(
      String devicePath, String userName, LbacOperationClassifier.OperationType operationType) {

    try {
      LOGGER.debug(
          "Checking device path: {} for user: {} with operation: {}",
          devicePath,
          userName,
          operationType);

      // Get user object
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.debug("User {} not found, denying access", userName);
        return LbacCheckResult.deny("User not found");
      }

      // Get user's read and write policies
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);

      boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
      boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());
      boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

      LOGGER.debug(
          "User {} - Read policy: {}, Write policy: {}, Has any policy: {}",
          userName,
          hasReadPolicy,
          hasWritePolicy,
          hasAnyPolicy);

      // Get database security label
      SecurityLabel databaseLabel = getDatabaseSecurityLabel(devicePath);
      boolean databaseHasLabels = (databaseLabel != null && !databaseLabel.getLabels().isEmpty());

      LOGGER.debug(
          "Database {} - Has labels: {}, Labels: {}",
          devicePath,
          databaseHasLabels,
          databaseLabel != null ? databaseLabel.getLabels() : "null");

      // Case 1: No read/write policies (both read and write policies are empty)
      if (!hasAnyPolicy) {
        LOGGER.debug(
            "User has no read policy and no write policy, allowing access regardless of database labels");
        return LbacCheckResult.allow();
      }

      // Case 2: Has any policy (read/write/both exist)
      if (!databaseHasLabels) {
        // No label data
        return handleNoLabelsCase(operationType, hasReadPolicy, hasWritePolicy);
      } else {
        // Has label data
        return handleHasLabelsCase(
            user, devicePath, operationType, databaseLabel, hasReadPolicy, hasWritePolicy);
      }

    } catch (Exception e) {
      LOGGER.error("Error during LBAC check for device path {}: {}", devicePath, e.getMessage());
      return LbacCheckResult.deny("LBAC check failed with error: " + e.getMessage());
    }
  }

  /**
   * Evaluate the user's policy against the database label
   *
   * @param userLabelPolicy the user's label policy expression
   * @param databaseLabel the database security label
   * @param devicePath the device path being checked
   * @return LbacCheckResult based on policy evaluation
   */
  private static LbacCheckResult evaluatePolicy(
      String userLabelPolicy, SecurityLabel databaseLabel, String devicePath) {
    try {
      boolean policyMatches = LabelPolicyEvaluator.evaluate(userLabelPolicy, databaseLabel);

      if (policyMatches) {
        LOGGER.debug("Label policy matches for database security label: {}", devicePath);
        return LbacCheckResult.allow();
      } else {
        LOGGER.debug("Label policy does not match for database security label: {}", devicePath);
        return LbacCheckResult.deny(
            String.format(
                "Label policy does not match for database security label: %s", devicePath));
      }
    } catch (Exception e) {
      LOGGER.error("Error evaluating policy for database security label: {}", devicePath, e);
      return LbacCheckResult.deny("Error evaluating label policy: " + e.getMessage());
    }
  }

  /**
   * Handle case where user has no label policy defined
   *
   * @param devicePaths list of device paths
   * @param operationType operation type
   * @return LbacCheckResult based on default policy
   */

  /**
   * Handle case where database has no labels but user has policies Implements the LBAC logic for no
   * labels scenario: - Only read policy: read operation → trigger LBAC, deny access; write
   * operation → no LBAC, allow access; both operations → trigger LBAC, deny access - Only write
   * policy: write operation → trigger LBAC, deny access; read operation → no LBAC, allow access;
   * both operations → trigger LBAC, deny access - Both read and write policies: read operation →
   * trigger LBAC, deny access; write operation → trigger LBAC, deny access; both operations →
   * trigger LBAC, deny access
   */
  private static LbacCheckResult handleNoLabelsCase(
      LbacOperationClassifier.OperationType operationType,
      boolean hasReadPolicy,
      boolean hasWritePolicy) {

    LOGGER.debug("Database has no labels, applying secure default policy");

    switch (operationType) {
      case READ:
        if (hasReadPolicy) {
          // Only read policy + read operation → trigger LBAC, deny access
          LOGGER.debug("Read operation with read policy but no database labels - denying access");
          return LbacCheckResult.deny(
              "Database has no security label but user has read policy restrictions");
        } else {
          // No read policy + read operation → no LBAC, allow access
          LOGGER.debug("Read operation with no read policy - allowing access");
          return LbacCheckResult.allow();
        }

      case WRITE:
        if (hasWritePolicy) {
          // Only write policy + write operation → trigger LBAC, deny access
          LOGGER.debug("Write operation with write policy but no database labels - denying access");
          return LbacCheckResult.deny(
              "Database has no security label but user has write policy restrictions");
        } else {
          // No write policy + write operation → no LBAC, allow access
          LOGGER.debug("Write operation with no write policy - allowing access");
          return LbacCheckResult.allow();
        }

      case BOTH:
        // Both read and write operations (like SELECT INTO)
        if (hasReadPolicy || hasWritePolicy) {
          // Any policy + both operations → trigger LBAC, deny access
          LOGGER.debug("Both operations with any policy but no database labels - denying access");
          return LbacCheckResult.deny(
              "Database has no security label but user has policy restrictions");
        } else {
          // No policies + both operations → no LBAC, allow access
          LOGGER.debug("Both operations with no policies - allowing access");
          return LbacCheckResult.allow();
        }

      default:
        LOGGER.debug("Unknown operation type: {}", operationType);
        return LbacCheckResult.allow();
    }
  }

  /**
   * Handle case where database has labels and user has policies Implements the LBAC logic for has
   * labels scenario: - Only read policy: read operation → trigger LBAC, evaluate read policy with
   * labels; write operation → no LBAC, allow access; both operations → trigger LBAC, evaluate read
   * policy with labels - Only write policy: write operation → trigger LBAC, evaluate write policy
   * with labels; read operation → no LBAC, allow access; both operations → trigger LBAC, evaluate
   * write policy with labels - Both read and write policies: read operation → trigger LBAC,
   * evaluate read policy with labels; write operation → trigger LBAC, evaluate write policy with
   * labels; both operations → trigger LBAC, evaluate both policies with labels
   */
  private static LbacCheckResult handleHasLabelsCase(
      User user,
      String devicePath,
      LbacOperationClassifier.OperationType operationType,
      SecurityLabel databaseLabel,
      boolean hasReadPolicy,
      boolean hasWritePolicy) {

    LOGGER.debug("Database has labels, evaluating policy against labels");

    switch (operationType) {
      case READ:
        if (hasReadPolicy) {
          // Only read policy + read operation → trigger LBAC, evaluate read policy with
          // labels
          LOGGER.debug(
              "Read operation with read policy and database labels - evaluating read policy");
          return evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ),
              databaseLabel,
              devicePath);
        } else {
          // No read policy + read operation → no LBAC, allow access
          LOGGER.debug("Read operation with no read policy - allowing access");
          return LbacCheckResult.allow();
        }

      case WRITE:
        if (hasWritePolicy) {
          // Only write policy + write operation → trigger LBAC, evaluate write policy
          // with labels
          LOGGER.debug(
              "Write operation with write policy and database labels - evaluating write policy");
          return evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE),
              databaseLabel,
              devicePath);
        } else {
          // No write policy + write operation → no LBAC, allow access
          LOGGER.debug("Write operation with no write policy - allowing access");
          return LbacCheckResult.allow();
        }

      case BOTH:
        // Both read and write operations (like SELECT INTO)
        LOGGER.debug("Both operations with database labels - evaluating policies");

        if (hasReadPolicy && hasWritePolicy) {
          // Both read and write policies + both operations → trigger LBAC, evaluate both
          // policies with labels
          return handleBothOperationCase(
              user, devicePath, databaseLabel, hasReadPolicy, hasWritePolicy);
        } else if (hasReadPolicy) {
          // Only read policy + both operations → trigger LBAC, evaluate read policy with
          // labels
          LOGGER.debug("Both operations with read policy only - evaluating read policy");
          return evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ),
              databaseLabel,
              devicePath);
        } else if (hasWritePolicy) {
          // Only write policy + both operations → trigger LBAC, evaluate write policy
          // with labels
          LOGGER.debug("Both operations with write policy only - evaluating write policy");
          return evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE),
              databaseLabel,
              devicePath);
        } else {
          // No policies + both operations → no LBAC, allow access
          LOGGER.debug("Both operations with no policies - allowing access");
          return LbacCheckResult.allow();
        }

      default:
        LOGGER.debug("Unknown operation type: {}", operationType);
        return LbacCheckResult.allow();
    }
  }

  /**
   * Handle BOTH operation case where both read and write policies need to be checked
   *
   * @param user the user attempting the operation
   * @param devicePath the device path to check
   * @param databaseLabel the database security label
   * @param hasReadPolicy whether user has read policy
   * @param hasWritePolicy whether user has write policy
   * @return LbacCheckResult indicating permission status
   */
  private static LbacCheckResult handleBothOperationCase(
      User user,
      String devicePath,
      SecurityLabel databaseLabel,
      boolean hasReadPolicy,
      boolean hasWritePolicy) {

    // Check read policy first
    if (hasReadPolicy) {
      LOGGER.debug("Checking read policy for BOTH operation");
      LbacCheckResult readResult =
          evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ),
              databaseLabel,
              devicePath);

      if (!readResult.isAllowed()) {
        LOGGER.debug("Read policy check failed for BOTH operation: {}", readResult.getReason());
        return readResult;
      }
    }

    // Check write policy
    if (hasWritePolicy) {
      LOGGER.debug("Checking write policy for BOTH operation");
      LbacCheckResult writeResult =
          evaluatePolicy(
              getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE),
              databaseLabel,
              devicePath);

      if (!writeResult.isAllowed()) {
        LOGGER.debug("Write policy check failed for BOTH operation: {}", writeResult.getReason());
        return writeResult;
      }
    }

    // Both checks passed
    LOGGER.debug("Both read and write policy checks passed for BOTH operation");
    return LbacCheckResult.allow();
  }

  /**
   * Get user label policy for specific operation type Must separate read policy and write policy,
   * policies only have two types
   *
   * @param user User object
   * @param operationType Operation type (READ/WRITE)
   * @return Label policy expression or null if no policy exists
   */
  private static String getUserLabelPolicy(
      User user, LbacOperationClassifier.OperationType operationType) {

    LOGGER.debug(
        "Getting label policy for user: {} and operation: {}", user.getName(), operationType);

    // Use independent read and write policy fields
    if (operationType == LbacOperationClassifier.OperationType.READ) {
      // Use read policy for READ operations
      String readPolicyExpression = user.getReadLabelPolicyExpression();
      if (readPolicyExpression != null && !readPolicyExpression.trim().isEmpty()) {
        LOGGER.debug("User {} has READ label policy: '{}'", user.getName(), readPolicyExpression);
        return readPolicyExpression;
      } else {
        LOGGER.debug("User {} has no READ label policy", user.getName());
        return null;
      }
    } else if (operationType == LbacOperationClassifier.OperationType.WRITE) {
      // Use write policy for WRITE operations
      String writePolicyExpression = user.getWriteLabelPolicyExpression();
      if (writePolicyExpression != null && !writePolicyExpression.trim().isEmpty()) {
        LOGGER.debug("User {} has WRITE label policy: '{}'", user.getName(), writePolicyExpression);
        return writePolicyExpression;
      } else {
        LOGGER.debug("User {} has no WRITE label policy", user.getName());
        return null;
      }
    } else if (operationType == LbacOperationClassifier.OperationType.BOTH) {
      // For BOTH operations, we need to check both read and write policies
      // This is handled by the calling method, so we return null here
      LOGGER.debug("User {} BOTH operation - policy evaluation handled by caller", user.getName());
      return null;
    }

    LOGGER.debug(
        "User {} has no label policy for operation type: {}", user.getName(), operationType);
    return null;
  }

  /**
   * Check if LBAC is enabled in the system TODO: This should check system configuration
   *
   * @return true if LBAC is enabled, false otherwise
   */
  public static boolean isLbacEnabled() {
    try {
      // Get LBAC configuration from IoTDB descriptor
      return org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().isEnableLbac();
    } catch (Exception e) {
      LOGGER.warn("Failed to get LBAC configuration, defaulting to enabled", e);
      return true; // Default to enabled in case of error
    }
  }

  /**
   * Validate a label policy expression syntax
   *
   * @param policyExpression the policy expression to validate
   * @return ValidationResult containing validation status and any error messages
   */
  public static ValidationResult validateLabelPolicy(String policyExpression) {
    if (policyExpression == null || policyExpression.trim().isEmpty()) {
      return ValidationResult.invalid("Policy expression cannot be null or empty");
    }

    try {
      boolean isValid = LabelPolicyEvaluator.isValidExpression(policyExpression);
      if (isValid) {
        return ValidationResult.valid();
      } else {
        return ValidationResult.invalid("Invalid policy expression syntax");
      }
    } catch (Exception e) {
      LOGGER.debug("Policy validation failed", e);
      return ValidationResult.invalid("Policy validation error: " + e.getMessage());
    }
  }

  /** Result class for LBAC permission checks */
  public static class LbacCheckResult {
    private final boolean allowed;
    private final String reason;
    private final TSStatusCode statusCode;

    private LbacCheckResult(boolean allowed, String reason, TSStatusCode statusCode) {
      this.allowed = allowed;
      this.reason = reason;
      this.statusCode = statusCode;
    }

    public static LbacCheckResult allow() {
      return new LbacCheckResult(true, "Access granted", TSStatusCode.SUCCESS_STATUS);
    }

    public static LbacCheckResult deny(String reason) {
      return new LbacCheckResult(false, reason, TSStatusCode.NO_PERMISSION);
    }

    public boolean isAllowed() {
      return allowed;
    }

    public String getReason() {
      return reason;
    }

    public TSStatusCode getStatusCode() {
      return statusCode;
    }

    @Override
    public String toString() {
      return String.format(
          "LbacCheckResult{allowed=%s, reason='%s', statusCode=%s}", allowed, reason, statusCode);
    }
  }

  /** Result class for policy validation */
  public static class ValidationResult {
    private final boolean valid;
    private final String errorMessage;

    private ValidationResult(boolean valid, String errorMessage) {
      this.valid = valid;
      this.errorMessage = errorMessage;
    }

    public static ValidationResult valid() {
      return new ValidationResult(true, null);
    }

    public static ValidationResult invalid(String errorMessage) {
      return new ValidationResult(false, errorMessage);
    }

    public boolean isValid() {
      return valid;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    @Override
    public String toString() {
      return String.format("ValidationResult{valid=%s, errorMessage='%s'}", valid, errorMessage);
    }
  }

  /**
   * Centralized LBAC permission check for all database-related operations. This method enforces the
   * security default policy and read/write strategy separation.
   *
   * @param statement the statement to check permissions for
   * @param userName the username attempting to execute the statement
   * @return TSStatus indicating the LBAC permission check result
   */
  public static TSStatus checkLbacPermissionForStatement(Statement statement, String userName) {
    try {
      // If LBAC is not enabled, allow access
      if (!isLbacEnabled()) {
        return new org.apache.iotdb.common.rpc.thrift.TSStatus(
            org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      // Classify operation type: READ, WRITE, BOTH
      LbacOperationClassifier.OperationType operationType =
          LbacOperationClassifier.classifyOperation(statement);
      if (operationType == null) {
        // Not a data-related operation, allow
        return new org.apache.iotdb.common.rpc.thrift.TSStatus(
            org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      // Get all involved database paths (for batch check)
      java.util.List<String> dbPaths = new java.util.ArrayList<>();
      java.util.List<? extends org.apache.iotdb.commons.path.PartialPath> paths =
          statement.getPaths();
      if (paths != null) {
        for (org.apache.iotdb.commons.path.PartialPath path : paths) {
          // Extract database path from device path
          String dbPath = extractDatabasePathFromDevicePath(path.getFullPath());
          if (dbPath != null && !dbPaths.contains(dbPath)) {
            dbPaths.add(dbPath);
          }
        }
      }
      if (dbPaths.isEmpty()) {
        // No database path, allow
        return new org.apache.iotdb.common.rpc.thrift.TSStatus(
            org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      // Get user object and label policies
      User user = getUserByName(userName);
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);
      boolean hasReadPolicy = readPolicy != null && !readPolicy.trim().isEmpty();
      boolean hasWritePolicy = writePolicy != null && !writePolicy.trim().isEmpty();
      boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

      LOGGER.warn("=== LBAC DEBUG INFO ===");
      LOGGER.warn("User: {}", userName);
      LOGGER.warn("Read policy: '{}' (hasReadPolicy: {})", readPolicy, hasReadPolicy);
      LOGGER.warn("Write policy: '{}' (hasWritePolicy: {})", writePolicy, hasWritePolicy);
      LOGGER.warn("Has any policy: {}", hasAnyPolicy);
      LOGGER.warn("Operation type: {}", operationType);
      LOGGER.warn("Database paths: {}", dbPaths);
      // For each database path, apply security default policy
      for (String dbPath : dbPaths) {
        // Get security label for this database
        org.apache.iotdb.commons.schema.SecurityLabel dbLabel = getDatabaseSecurityLabel(dbPath);
        boolean hasLabel = dbLabel != null && !dbLabel.getLabels().isEmpty();
        // 1. No read/write policy
        if (!hasAnyPolicy) {
          // No policy, allow access regardless of label
          continue;
        }
        // 2. Has any policy
        if (!hasLabel) {
          // No label on database, apply security default
          switch (operationType) {
            case READ:
              if (hasReadPolicy) {
                return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                        org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                    .setMessage("LBAC: user has read policy but database has no label");
              } else {
                continue;
              }
            case WRITE:
              if (hasWritePolicy) {
                return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                        org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                    .setMessage("LBAC: user has write policy but database has no label");
              } else {
                continue;
              }
            case BOTH:
              // select into, both read and write policy must be checked
              if (hasReadPolicy || hasWritePolicy) {
                return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                        org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                    .setMessage("LBAC: user has read/write policy but database has no label");
              } else {
                continue;
              }
          }
        } else {
          // 3. Has label, check by operation type
          switch (operationType) {
            case READ:
              if (hasReadPolicy) {
                if (!evaluatePolicy(readPolicy, dbLabel, dbPath).isAllowed()) {
                  return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                          org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                      .setMessage("LBAC: read policy not satisfied");
                }
              }
              continue;
            case WRITE:
              if (hasWritePolicy) {
                if (!evaluatePolicy(writePolicy, dbLabel, dbPath).isAllowed()) {
                  return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                          org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                      .setMessage("LBAC: write policy not satisfied");
                }
              }
              continue;
            case BOTH:
              boolean readOk = true, writeOk = true;
              if (hasReadPolicy) {
                readOk = evaluatePolicy(readPolicy, dbLabel, dbPath).isAllowed();
              }
              if (hasWritePolicy) {
                writeOk = evaluatePolicy(writePolicy, dbLabel, dbPath).isAllowed();
              }
              if (!readOk || !writeOk) {
                return new org.apache.iotdb.common.rpc.thrift.TSStatus(
                        org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
                    .setMessage("LBAC: read/write policy not satisfied");
              }
              continue;
          }
        }
      }
      // All checks passed
      return new org.apache.iotdb.common.rpc.thrift.TSStatus(
          org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      return new org.apache.iotdb.common.rpc.thrift.TSStatus(
              org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("LBAC: exception - " + e.getMessage());
    }
  }

  /**
   * Get user object by username This method provides a centralized way to get user information for
   * LBAC checks
   *
   * @param userName Username
   * @return User object or null if not found
   */
  private static User getUserByName(String userName) {
    try {
      // Try to get user from authority fetcher cache
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      if (authorityFetcher != null) {
        return authorityFetcher.getAuthorCache().getUserCache(userName);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to get user {} for LBAC check: {}", userName, e.getMessage());
    }
    return null;
  }

  /**
   * Handle case where database has no labels but user has policies For database-level operations
   */
  private static boolean handleNoLabelsCaseForDatabase(
      LbacOperationClassifier.OperationType operationType,
      boolean hasReadPolicy,
      boolean hasWritePolicy) {

    switch (operationType) {
      case READ:
        // Only read policy + read operation → trigger LBAC, deny access
        // No read policy + read operation → no LBAC, allow access
        return !hasReadPolicy;

      case WRITE:
        // Only write policy + write operation → trigger LBAC, deny access
        // No write policy + write operation → no LBAC, allow access
        return !hasWritePolicy;

      case BOTH:
        // Any policy + both operations → trigger LBAC, deny access
        // No policies + both operations → no LBAC, allow access
        return !(hasReadPolicy || hasWritePolicy);

      default:
        return true;
    }
  }

  /** Handle case where database has labels and user has policies For database-level operations */
  private static boolean handleHasLabelsCaseForDatabase(
      User user,
      String databaseName,
      LbacOperationClassifier.OperationType operationType,
      Map<String, String> securityLabels,
      boolean hasReadPolicy,
      boolean hasWritePolicy) {

    // Convert to SecurityLabel object
    SecurityLabel databaseLabel = new SecurityLabel();
    for (Map.Entry<String, String> entry : securityLabels.entrySet()) {
      databaseLabel.setLabel(entry.getKey(), entry.getValue());
    }

    switch (operationType) {
      case READ:
        if (hasReadPolicy) {
          // Only read policy + read operation → trigger LBAC, evaluate read policy with
          // labels
          LbacCheckResult readResult =
              evaluatePolicy(
                  getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ),
                  databaseLabel,
                  databaseName);
          return readResult.isAllowed();
        } else {
          // No read policy + read operation → no LBAC, allow access
          return true;
        }

      case WRITE:
        if (hasWritePolicy) {
          // Only write policy + write operation → trigger LBAC, evaluate write policy
          // with labels
          LbacCheckResult writeResult =
              evaluatePolicy(
                  getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE),
                  databaseLabel,
                  databaseName);
          return writeResult.isAllowed();
        } else {
          // No write policy + write operation → no LBAC, allow access
          return true;
        }

      case BOTH:
        // Both read and write operations (like SELECT INTO)
        boolean readOk = true, writeOk = true;

        if (hasReadPolicy) {
          // Check read policy
          LbacCheckResult readResult =
              evaluatePolicy(
                  getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ),
                  databaseLabel,
                  databaseName);
          readOk = readResult.isAllowed();
        }

        if (hasWritePolicy) {
          // Check write policy
          LbacCheckResult writeResult =
              evaluatePolicy(
                  getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE),
                  databaseLabel,
                  databaseName);
          writeOk = writeResult.isAllowed();
        }

        // Both policies must be satisfied
        return readOk && writeOk;

      default:
        return true;
    }
  }

  /**
   * Extract database path from device path This method extracts the first two parts of the device
   * path as the database path and handles cases with wildcards.
   *
   * @param devicePath the device path to extract database path from
   * @return the extracted database path or null if not applicable
   */
  public static String extractDatabasePathFromDevicePath(String devicePath) {
    if (devicePath == null || devicePath.trim().isEmpty()) {
      return null;
    }

    if (devicePath.contains("*") || devicePath.contains("**")) {
      LOGGER.debug(
          "Device path contains wildcards, cannot extract specific database path: {}", devicePath);
      return null;
    }

    // Split by dots and take the first two parts as database path
    String[] parts = devicePath.split("\\.");
    if (parts.length >= 2) {
      String databasePath = parts[0] + "." + parts[1];
      LOGGER.debug("Extracted database path: {} from device path: {}", databasePath, devicePath);
      return databasePath;
    } else if (parts.length == 1) {
      LOGGER.debug("Single part path, treating as database: {}", devicePath);
      return parts[0];
    }

    LOGGER.debug("Could not extract database path from device path: {}", devicePath);
    return null;
  }

  /**
   * Get database security label for a device path
   *
   * @param devicePath the device path to get security label for
   * @return SecurityLabel object or null if not found
   */
  private static SecurityLabel getDatabaseSecurityLabel(String devicePath) {
    try {
      // Extract database path from device path
      String databasePath = extractDatabasePathFromDevicePath(devicePath);
      if (databasePath == null) {
        LOGGER.debug("Could not extract database path from device path: {}", devicePath);
        return null;
      }

      // Get security label from database
      return DatabaseLabelFetcher.getSecurityLabelForPath(databasePath);
    } catch (Exception e) {
      LOGGER.warn(
          "Error getting database security label for path {}: {}", devicePath, e.getMessage());
      return null;
    }
  }

  /**
   * Get database info for a database path Helper method to get database information consistently
   */
  private static TDatabaseInfo getDatabaseInfo(String databasePath) {
    try {
      // Get database info from ConfigNode
      org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor
          executor =
              org.apache.iotdb.db.queryengine.plan.execution.config.executor
                  .ClusterConfigTaskExecutor.getInstance();

      // Create a request to get database information
      TGetDatabaseReq req =
          new TGetDatabaseReq(
              Arrays.asList(databasePath.split("\\.")),
              org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_SCOPE.serialize());

      // Use the same client manager as other methods
      IClientManager<ConfigRegionId, ConfigNodeClient> clientManager =
          ConfigNodeClientManager.getInstance();

      try (ConfigNodeClient client = clientManager.borrowClient(CONFIG_REGION_ID)) {

        TShowDatabaseResp resp = client.showDatabase(req);

        if (resp.getStatus() != null
            && resp.getStatus().getCode()
                == org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && resp.getDatabaseInfoMap() != null
            && resp.getDatabaseInfoMap().containsKey(databasePath)) {

          return resp.getDatabaseInfoMap().get(databasePath);
        } else {
          LOGGER.debug("Database {} not found or error in response", databasePath);
          return null;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error getting database info for path {}: {}", databasePath, e.getMessage());
      return null;
    }
  }

  /**
   * Check LBAC permission for database-level operations with complete LBAC logic This method
   * implements the detailed LBAC logic as specified:
   *
   * <p>No read/write policies (both read and write policies are empty): - No label data → No LBAC
   * triggered, allow access - Has label data → No LBAC triggered, allow access
   *
   * <p>Has any policy (read/write/both exist): - No label data: - Only read policy: read operation
   * → trigger LBAC, deny access; write operation → no LBAC, allow access; both operations → trigger
   * LBAC, deny access - Only write policy: write operation → trigger LBAC, deny access; read
   * operation → no LBAC, allow access; both operations → trigger LBAC, deny access - Both read and
   * write policies: read operation → trigger LBAC, deny access; write operation → trigger LBAC,
   * deny access; both operations → trigger LBAC, deny access - Has label data: - Only read policy:
   * read operation → trigger LBAC, evaluate read policy with labels; write operation → no LBAC,
   * allow access; both operations → trigger LBAC, evaluate read policy with labels - Only write
   * policy: write operation → trigger LBAC, evaluate write policy with labels; read operation → no
   * LBAC, allow access; both operations → trigger LBAC, evaluate write policy with labels - Both
   * read and write policies: read operation → trigger LBAC, evaluate read policy with labels; write
   * operation → trigger LBAC, evaluate write policy with labels; both operations → trigger LBAC,
   * evaluate both policies with labels
   *
   * @param userName the username
   * @param databasePath the database path
   * @param operationType the operation type (READ/WRITE/BOTH)
   * @return true if access is allowed, false otherwise
   */
  public static boolean checkLbacPermissionForDatabase(
      String userName, String databasePath, LbacOperationClassifier.OperationType operationType) {

    try {
      LOGGER.info("=== CHECKING DATABASE LBAC PERMISSION WITH COMPLETE LOGIC ===");
      LOGGER.info("User: {}, Database: {}, Operation: {}", userName, databasePath, operationType);

      // Check if LBAC is enabled
      if (!isLbacEnabled()) {
        LOGGER.info(
            "LBAC disabled, allowing access for user {} to database {}", userName, databasePath);
        return true;
      }

      // Super user has access to everything
      if (AuthorityChecker.SUPER_USER.equals(userName)) {
        LOGGER.info("Super user {} accessing database {}", userName, databasePath);
        return true;
      }

      // Get user object
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User {} not found for LBAC check", userName);
        return false;
      }

      // Extract clean database path
      String cleanDatabasePath = AuthorityChecker.extractDatabaseFromPath(databasePath);
      if (cleanDatabasePath == null) {
        LOGGER.warn("Cannot extract valid database path from input: {}", databasePath);
        return false;
      }

      // Get database security label
      SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(cleanDatabasePath);
      boolean databaseHasLabels = (databaseLabel != null && !databaseLabel.getLabels().isEmpty());

      LOGGER.info("Database {} has labels: {}", cleanDatabasePath, databaseHasLabels);
      if (databaseHasLabels) {
        LOGGER.info("Database labels: {}", databaseLabel.getLabels());
      }

      // Get user's read and write policies
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);

      boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
      boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());
      boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

      LOGGER.info("User has read policy: {} ({})", hasReadPolicy, readPolicy);
      LOGGER.info("User has write policy: {} ({})", hasWritePolicy, writePolicy);
      LOGGER.info("User has any policy: {}", hasAnyPolicy);

      // Rule 1 & 2: No read/write policies (both read and write policies are empty)
      if (!hasAnyPolicy) {
        LOGGER.info(
            "User has no read and write policies, allowing access regardless of database labels");
        return true;
      }

      // Rule 3: Has any policy + No labels
      if (hasAnyPolicy && !databaseHasLabels) {
        LOGGER.info("User has any policies but database has no labels, checking by operation type");
        return handleNoLabelsCaseForDatabase(operationType, hasReadPolicy, hasWritePolicy);
      }

      // Rule 4: Has any policy + Has labels
      if (hasAnyPolicy && databaseHasLabels) {
        LOGGER.info(
            "User has any policies and database has labels, checking by operation type and evaluating policy");
        return handleHasLabelsCaseForDatabase(
            user,
            cleanDatabasePath,
            operationType,
            databaseLabel.getLabels(),
            hasReadPolicy,
            hasWritePolicy);
      }

      // Default case: allow access
      LOGGER.info("Default case: allowing access");
      return true;

    } catch (Exception e) {
      LOGGER.error(
          "Error checking database LBAC permission for {}: {}", databasePath, e.getMessage(), e);
      return false; // Deny access on error
    }
  }
}
