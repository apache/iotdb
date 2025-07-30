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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  private static LbacCheckResult handleNoUserPolicy(
      List<String> devicePaths, LbacOperationClassifier.OperationType operationType) {

    LOGGER.debug("User has no {} label policy, applying default rules", operationType);

    // Default behavior according to the specified logic:
    // 1. No policy + No labels: Allow access
    // 2. No policy + Has labels: Allow access (changed from deny to allow)
    // 3. Has policy + No labels: Deny access (handled in checkSingleDevicePath)
    // 4. Has policy + Has labels: Evaluate policy (handled in
    // checkSingleDevicePath)

    // For users without policies, allow access regardless of database labels
    LOGGER.debug("User has no {} policy, allowing access to all databases", operationType);
    return LbacCheckResult.allow();
  }

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
   * Check if user has any label policies (read/write/both) This method determines whether the user
   * has any policy restrictions
   *
   * @param user User object to check
   * @return true if user has any policies, false otherwise
   */
  public static boolean userHasLabelPolicies(User user) {
    if (user == null) {
      return false;
    }

    // Check read policy
    String readPolicy = user.getReadLabelPolicyExpression();
    boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());

    // Check write policy
    String writePolicy = user.getWriteLabelPolicyExpression();
    boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());

    // Has any policy (read/write/both exist)
    boolean hasPolicies = hasReadPolicy || hasWritePolicy;

    LOGGER.debug(
        "User {} - Read policy: {}, Write policy: {}, Has policies: {}",
        user.getName(),
        hasReadPolicy,
        hasWritePolicy,
        hasPolicies);

    return hasPolicies;
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
   * Filter security label map based on user's RBAC and LBAC permissions This method combines both
   * RBAC (AuthorityChecker) and LBAC filtering Following the same pattern as AuthorityChecker for
   * consistency
   */
  public static Map<String, String> filterSecurityLabelMapByPermissions(
      Map<String, String> originalMap, String userName) {

    if (originalMap == null || originalMap.isEmpty()) {
      return Collections.emptyMap();
    }

    // Super user sees all databases
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return originalMap;
    }

    Map<String, String> filteredMap = new HashMap<>();

    for (Map.Entry<String, String> entry : originalMap.entrySet()) {
      String database = entry.getKey();

      // Step 1: RBAC filtering - check if user can see this database
      if (AuthorityChecker.checkDBVisible(userName, database)) {
        // Step 2: LBAC filtering - check LBAC permissions
        if (checkLbacPermissionForDatabase(
            userName, database, LbacOperationClassifier.OperationType.READ)) {
          filteredMap.put(database, entry.getValue());
        }
      }
    }

    return filteredMap;
  }

  /**
   * Check LBAC permission for a specific database with given operation type. This method follows
   * the same pattern as AuthorityChecker.checkDBVisible and includes enhanced wildcard pattern
   * support.
   */
  public static boolean checkLbacPermissionForDatabase(
      String userName, String database, LbacOperationClassifier.OperationType operationType) {

    try {
      if (!isLbacEnabled()) {
        LOGGER.debug(
            "LBAC disabled, allowing access for user {} to database {}", userName, database);
        return true; // LBAC disabled, allow access
      }

      if (AuthorityChecker.SUPER_USER.equals(userName)) {
        LOGGER.debug("Super user {} accessing database {}", userName, database);
        return true; // Super user always has access
      }

      // Extract clean database path from potentially wildcarded input
      String cleanDatabasePath = AuthorityChecker.extractDatabaseFromPath(database);
      if (cleanDatabasePath == null) {
        LOGGER.warn("Cannot extract valid database path from input: {}", database);
        return false; // Cannot extract valid database path, deny access for security
      }

      // Get user for LBAC check
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User {} not found for LBAC check", userName);
        return false; // User not found, deny access
      }

      // Get database label using the clean database path
      SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(cleanDatabasePath);
      if (databaseLabel == null) {
        LOGGER.debug("No security label found for database: {}", cleanDatabasePath);
      } else {
        LOGGER.debug(
            "Database {} has security label: {}", cleanDatabasePath, databaseLabel.getLabels());
      }

      // Get user's label policy for the operation type
      String userPolicy = getUserLabelPolicy(user, operationType);
      if (userPolicy == null || userPolicy.trim().isEmpty()) {
        LOGGER.debug(
            "No label policy found for user {} with operation type {}", userName, operationType);
      } else {
        LOGGER.debug("User {} has label policy for {}: {}", userName, operationType, userPolicy);
      }

      // Evaluate label policy
      boolean result = LabelPolicyEvaluator.evaluate(userPolicy, databaseLabel);
      LOGGER.debug(
          "LBAC evaluation result for user {} on database {}: {}", userName, database, result);
      return result;

    } catch (Exception e) {
      LOGGER.warn("Error checking LBAC permission for database {}: {}", database, e.getMessage());
      return false; // On error, deny access for security
    }
  }

  /**
   * Filter database info map based on LBAC permissions for the current user Only show databases
   * that the user has permission to access If user has policy but database has no label, deny
   * access to that database
   */
  public static Map<String, TDatabaseInfo> filterDatabaseInfoMapByLbac(
      Map<String, TDatabaseInfo> databaseInfoMap, String currentUser) {

    LOGGER.warn("=== LBAC DATABASE FILTER START ===");
    LOGGER.warn("Filtering databases for user: {}", currentUser);
    LOGGER.warn("Original database count: {}", databaseInfoMap.size());

    if (currentUser == null) {
      LOGGER.warn("Current user is null, denying access to all databases");
      return Collections.emptyMap();
    }

    // If user is super user, return all databases
    if (AuthorityChecker.SUPER_USER.equals(currentUser)) {
      LOGGER.warn("User {} is super user, showing all databases", currentUser);
      return databaseInfoMap;
    }

    // Get user object for LBAC check
    User user = getUserByName(currentUser);
    if (user == null) {
      LOGGER.warn("User {} not found for LBAC check, denying access to all databases", currentUser);
      return Collections.emptyMap();
    }

    Map<String, TDatabaseInfo> filteredMap = new HashMap<>();
    int allowedCount = 0;
    int deniedCount = 0;

    for (Map.Entry<String, TDatabaseInfo> entry : databaseInfoMap.entrySet()) {
      String databaseName = entry.getKey();
      TDatabaseInfo databaseInfo = entry.getValue();

      LOGGER.warn("Checking database: {}", databaseName);

      // Check LBAC permission for this specific database
      // For SHOW DATABASES operation, use READ operation type
      if (checkDatabaseLbacPermission(
          user, databaseName, databaseInfo, LbacOperationClassifier.OperationType.READ)) {
        filteredMap.put(databaseName, databaseInfo);
        allowedCount++;
        LOGGER.warn("Database {} ALLOWED by LBAC", databaseName);
      } else {
        deniedCount++;
        LOGGER.warn("Database {} DENIED by LBAC", databaseName);
      }
    }

    LOGGER.warn(
        "LBAC filtering complete - Allowed: {}, Denied: {}, Final database count: {}",
        allowedCount,
        deniedCount,
        filteredMap.size());
    LOGGER.warn("=== LBAC DATABASE FILTER END ===");

    return filteredMap;
  }

  /**
   * Check LBAC permission for a specific database Implements the complete LBAC logic as specified:
   * 1. No read/write policies + No labels: Allow access (no LBAC triggered) 2. No read/write
   * policies + Has labels: Allow access (no LBAC triggered) 3. Has any policy + No labels: Check by
   * operation type 4. Has any policy + Has labels: Evaluate specific policy based on operation type
   */
  public static boolean checkDatabaseLbacPermission(
      User user,
      String databaseName,
      TDatabaseInfo databaseInfo,
      LbacOperationClassifier.OperationType operationType) { // Add operation type parameter
    try {
      LOGGER.warn("=== CHECKING DATABASE LBAC PERMISSION ===");
      LOGGER.warn("Database: {}", databaseName);
      LOGGER.warn("User: {}", user.getName());
      LOGGER.warn("Operation: {}", operationType);

      // Get database security labels
      Map<String, String> securityLabels = databaseInfo.getSecurityLabel();
      boolean databaseHasLabels = (securityLabels != null && !securityLabels.isEmpty());
      LOGGER.warn("Database has labels: {}", databaseHasLabels);
      if (databaseHasLabels) {
        LOGGER.warn("Database labels: {}", securityLabels);
      }

      // Get user's read and write policies
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);

      boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
      boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());
      boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

      LOGGER.warn("User has read policy: {} ({})", hasReadPolicy, readPolicy);
      LOGGER.warn("User has write policy: {} ({})", hasWritePolicy, writePolicy);
      LOGGER.warn("User has any policy: {}", hasAnyPolicy);

      // Rule 1 & 2: No read/write policies (both read and write policies are empty)
      if (!hasAnyPolicy) {
        LOGGER.warn(
            "User has no read and write policies, allowing access regardless of database labels");
        return true;
      }

      // Rule 3: Has any policy + No labels
      if (hasAnyPolicy && !databaseHasLabels) {
        LOGGER.warn("User has any policies but database has no labels, checking by operation type");
        return handleNoLabelsCaseForDatabase(operationType, hasReadPolicy, hasWritePolicy);
      }

      // Rule 4: Has any policy + Has labels
      if (hasAnyPolicy && databaseHasLabels) {
        LOGGER.warn(
            "User has any policies and database has labels, checking by operation type and evaluating policy");
        return handleHasLabelsCaseForDatabase(
            user, databaseName, operationType, securityLabels, hasReadPolicy, hasWritePolicy);
      }

      // Default case: allow access
      LOGGER.warn("Default case: allowing access");
      return true;

    } catch (Exception e) {
      LOGGER.error(
          "Error checking database LBAC permission for {}: {}", databaseName, e.getMessage(), e);
      return false; // Deny access on error
    }
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
   * Filter security label map based on LBAC permissions for the current user Only include databases
   * that the current user has permission to access If user has policy but database has no label,
   * deny access to that database
   *
   * @param securityLabelMap Original security label map (databaseName -> label string)
   * @param currentUser Current user name
   * @return Filtered security label map containing only accessible databases
   */
  public static Map<String, String> filterSecurityLabelMapByLbac(
      Map<String, String> securityLabelMap, String currentUser) {

    LOGGER.warn("=== LBAC SECURITY LABEL FILTER START ===");
    LOGGER.warn("Filtering security label map for user: {}", currentUser);
    LOGGER.warn("Original map size: {}", securityLabelMap.size());

    // Check if user is super user
    if (AuthorityChecker.SUPER_USER.equals(currentUser)) {
      LOGGER.warn("User {} is super user, bypassing LBAC filtering", currentUser);
      return new HashMap<>(securityLabelMap);
    }

    // Get user's LBAC policies
    Map<String, String> userReadPolicies = getUserReadPolicies(currentUser);
    Map<String, String> userWritePolicies = getUserWritePolicies(currentUser);

    LOGGER.warn("User read policies: {}", userReadPolicies);
    LOGGER.warn("User write policies: {}", userWritePolicies);

    Map<String, String> filteredMap = new HashMap<>();
    int deniedCount = 0;
    int allowedCount = 0;

    for (Map.Entry<String, String> entry : securityLabelMap.entrySet()) {
      String databasePath = entry.getKey();
      String databaseLabel = entry.getValue();

      LOGGER.warn("Checking database: {} with label: {}", databasePath, databaseLabel);

      // Check LBAC permission for this database
      LbacCheckResult result =
          checkSingleDevicePath(
              databasePath,
              currentUser,
              LbacOperationClassifier.OperationType.READ); // Show operation is considered as READ

      if (result.isAllowed()) {
        filteredMap.put(databasePath, databaseLabel);
        allowedCount++;
        LOGGER.warn("Database {} ALLOWED by LBAC", databasePath);
      } else {
        deniedCount++;
        LOGGER.warn("Database {} DENIED by LBAC: {}", databasePath, result.getReason());
      }
    }

    LOGGER.warn(
        "LBAC filtering complete - Allowed: {}, Denied: {}, Final map size: {}",
        allowedCount,
        deniedCount,
        filteredMap.size());
    LOGGER.warn("=== LBAC SECURITY LABEL FILTER END ===");

    return filteredMap;
  }

  /**
   * Get user's read policies as a map
   *
   * @param userName the user name
   * @return map of read policies
   */
  private static Map<String, String> getUserReadPolicies(String userName) {
    try {
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User {} not found", userName);
        return new HashMap<>();
      }

      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      Map<String, String> policies = new HashMap<>();
      if (readPolicy != null && !readPolicy.trim().isEmpty()) {
        policies.put("read", readPolicy);
      }
      return policies;
    } catch (Exception e) {
      LOGGER.error("Error getting read policies for user: {}", userName, e);
      return new HashMap<>();
    }
  }

  /**
   * Get user's write policies as a map
   *
   * @param userName the user name
   * @return map of write policies
   */
  private static Map<String, String> getUserWritePolicies(String userName) {
    try {
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User {} not found", userName);
        return new HashMap<>();
      }

      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);
      Map<String, String> policies = new HashMap<>();
      if (writePolicy != null && !writePolicy.trim().isEmpty()) {
        policies.put("write", writePolicy);
      }
      return policies;
    } catch (Exception e) {
      LOGGER.error("Error getting write policies for user: {}", userName, e);
      return new HashMap<>();
    }
  }

  public static String extractDatabasePathFromDevicePath(String devicePath) {
    if (devicePath == null || devicePath.trim().isEmpty()) {
      return null;
    }

    // 检查是否包含通配符，如果包含则不是具体的数据库路径
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
   * Parse security label from string format using regex Support single quotes only Handles various
   * formats like: - "env = 'prod' , level = 3" - "env='prod',level=3"
   *
   * @param labelString the security label string
   * @return SecurityLabel object or null if parsing fails
   */
  private static SecurityLabel parseSecurityLabelFromString(String labelString) {
    if (labelString == null || labelString.trim().isEmpty()) {
      return null;
    }

    try {
      SecurityLabel securityLabel = new SecurityLabel();

      // Regex pattern to match key-value pairs with flexible spacing
      // Support single quotes only
      Pattern pattern =
          Pattern.compile(
              "\\s*([a-zA-Z][a-zA-Z0-9_]*)\\s*=\\s*('?[^',]*'?)\\s*(?:,|$)",
              Pattern.CASE_INSENSITIVE);

      Matcher matcher = pattern.matcher(labelString);

      while (matcher.find()) {
        String key = matcher.group(1).trim();
        String value = matcher.group(2).trim();

        // Remove single quotes if present
        if (value.startsWith("'") && value.endsWith("'")) {
          value = value.substring(1, value.length() - 1);
        }

        if (!key.isEmpty() && !value.isEmpty()) {
          securityLabel.setLabel(key, value);
          LOGGER.debug("Parsed label: {} = {}", key, value);
        }
      }

      return securityLabel;
    } catch (Exception e) {
      LOGGER.error("Error parsing security label from string: {}", labelString, e);
      return null;
    }
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

  private static boolean checkLabelMatch(String label, String policy) {

    if (policy == null || policy.trim().isEmpty()) {
      return true;
    }
    if (label == null) {
      return false;
    }
    return label.contains(policy);
  }
}
