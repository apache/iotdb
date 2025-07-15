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

import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    WRITE
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

    LOGGER.warn(
        "=== LBAC CHECK START === User: {}, Statement: {}, DevicePaths: {}",
        user != null ? user.getName() : "null",
        statement != null ? statement.getClass().getSimpleName() : "null",
        devicePaths);

    if (statement == null) {
      LOGGER.warn("Statement is null, denying access");
      return LbacCheckResult.deny("Statement cannot be null");
    }

    if (user == null) {
      LOGGER.warn("User is null, denying access");
      return LbacCheckResult.deny("User cannot be null");
    }

    // Check if this is a database-related operation that should be subject to LBAC
    if (!LbacOperationClassifier.isLbacRelevantOperation(statement)) {
      LOGGER.warn(
          "Statement {} is not a database-related operation for LBAC, allowing access",
          statement.getClass().getSimpleName());
      return LbacCheckResult.allow();
    }

    try {
      LOGGER.warn(
          "Checking LBAC permission for user: {} on statement: {}",
          user.getName(),
          statement.getClass().getSimpleName());

      // Step 1: Classify operation type (READ/WRITE/BOTH)
      LbacOperationClassifier.OperationType operationType =
          LbacOperationClassifier.classifyOperation(statement);

      LOGGER.warn("Operation classified as: {}", operationType);

      // Step 2: Handle BOTH operations (like SELECT INTO) - check both read and write
      // permissions
      if (operationType == LbacOperationClassifier.OperationType.BOTH) {
        LOGGER.warn("=== LBAC CHECK BOTH === Checking both read and write permissions");

        // Check read permissions first
        String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
        if (readPolicy == null || readPolicy.trim().isEmpty()) {
          LOGGER.warn("User has no read label policy, applying default rules");
          LbacCheckResult readResult =
              handleNoUserPolicy(devicePaths, LbacOperationClassifier.OperationType.READ);
          if (!readResult.isAllowed()) {
            LOGGER.warn(
                "=== LBAC CHECK BOTH DENIED === Read permission denied: {}",
                readResult.getReason());
            return readResult;
          }
        } else {
          for (String devicePath : devicePaths) {
            LOGGER.warn("Checking read permission for device path: {}", devicePath);
            LbacCheckResult readResult =
                checkSingleDevicePath(
                    devicePath, readPolicy, LbacOperationClassifier.OperationType.READ);
            if (!readResult.isAllowed()) {
              LOGGER.warn(
                  "=== LBAC CHECK BOTH DENIED === Read permission denied: {}",
                  readResult.getReason());
              return readResult;
            }
          }
        }

        // Check write permissions
        String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);
        if (writePolicy == null || writePolicy.trim().isEmpty()) {
          LOGGER.warn("User has no write label policy, applying default rules");
          LbacCheckResult writeResult =
              handleNoUserPolicy(devicePaths, LbacOperationClassifier.OperationType.WRITE);
          if (!writeResult.isAllowed()) {
            LOGGER.warn(
                "=== LBAC CHECK BOTH DENIED === Write permission denied: {}",
                writeResult.getReason());
            return writeResult;
          }
        } else {
          for (String devicePath : devicePaths) {
            LOGGER.warn("Checking write permission for device path: {}", devicePath);
            LbacCheckResult writeResult =
                checkSingleDevicePath(
                    devicePath, writePolicy, LbacOperationClassifier.OperationType.WRITE);
            if (!writeResult.isAllowed()) {
              LOGGER.warn(
                  "=== LBAC CHECK BOTH DENIED === Write permission denied: {}",
                  writeResult.getReason());
              return writeResult;
            }
          }
        }

        LOGGER.warn("=== LBAC CHECK BOTH ALLOWED === Both read and write permissions granted");
        return LbacCheckResult.allow();
      }

      // Step 3: Handle READ/WRITE operations (original logic)
      String userLabelPolicy = getUserLabelPolicy(user, operationType);

      LOGGER.warn("User label policy for {} operation: {}", operationType, userLabelPolicy);

      // Step 4: If user has no label policy, apply default rules
      if (userLabelPolicy == null || userLabelPolicy.trim().isEmpty()) {
        LOGGER.warn("User has no {} label policy, applying default rules", operationType);
        return handleNoUserPolicy(devicePaths, operationType);
      }

      LOGGER.warn("User has label policy, checking device paths...");

      // Step 5: Check LBAC for each device path involved
      for (String devicePath : devicePaths) {
        LOGGER.warn("Checking device path: {}", devicePath);
        LbacCheckResult result = checkSingleDevicePath(devicePath, userLabelPolicy, operationType);
        LOGGER.warn("Device path {} check result: {}", devicePath, result.isAllowed());
        if (!result.isAllowed()) {
          LOGGER.warn("LBAC CHECK FAILED for device path: {} - {}", devicePath, result.getReason());
          return result; // Return first failure
        }
      }

      // All checks passed
      LOGGER.warn("=== LBAC CHECK PASSED === for user: {}", user.getName());
      return LbacCheckResult.allow();

    } catch (Exception e) {
      LOGGER.error("Error during LBAC permission check for user: {}", user.getName(), e);
      return LbacCheckResult.deny("Internal error during LBAC check: " + e.getMessage());
    }
  }

  /**
   * Check LBAC permission for a single device path Implements the following logic: 1. No read/write
   * policies + No labels: Allow access (no LBAC triggered) 2. No read/write policies + Has labels:
   * Allow access (no LBAC triggered) 3. Has read/write policies + No labels: Deny access (LBAC
   * triggered) 4. Has read policy only + Has labels + Read operation: Trigger LBAC 5. Has read
   * policy only + Has labels + Write operation: No LBAC triggered, access allowed 6. Has write
   * policy only + Has labels + Read operation: No LBAC triggered, access allowed 7. Has write
   * policy only + Has labels + Write operation: Trigger LBAC 8. Has both read and write policies +
   * Has labels + Any operation: Trigger LBAC
   *
   * @param devicePath the device path to check
   * @param userLabelPolicy the user's label policy expression
   * @param operationType the operation type (READ/WRITE)
   * @return LbacCheckResult for this specific device path
   */
  private static LbacCheckResult checkSingleDevicePath(
      String devicePath,
      String userLabelPolicy,
      LbacOperationClassifier.OperationType operationType) {

    try {
      LOGGER.debug(
          "Checking device path: {} with policy: {} for operation: {}",
          devicePath,
          userLabelPolicy,
          operationType);

      // Get database security label for this device path
      SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(devicePath);
      boolean databaseHasLabels = (databaseLabel != null);
      boolean userHasPolicy = (userLabelPolicy != null && !userLabelPolicy.trim().isEmpty());

      LOGGER.debug(
          "Database has labels: {}, User has policy: {}, Operation: {}",
          databaseHasLabels,
          userHasPolicy,
          operationType);

      // Rule 1 & 2: No policies + No labels OR No policies + Has labels
      if (!userHasPolicy) {
        LOGGER.debug("User has no policies, allowing access regardless of database labels");
        return LbacCheckResult.allow();
      }

      // Rule 3: Has policies + No labels
      if (userHasPolicy && !databaseHasLabels) {
        LOGGER.debug("User has policies but database has no labels, denying access");
        return LbacCheckResult.deny(
            "Database has no security label but user has label policy restrictions");
      }

      // Rules 4-8: Has policies + Has labels
      if (userHasPolicy && databaseHasLabels) {
        // Get user's read and write policies to determine the specific rule
        boolean hasReadPolicy = hasReadPolicy(userLabelPolicy);
        boolean hasWritePolicy = hasWritePolicy(userLabelPolicy);

        LOGGER.debug("User has read policy: {}, write policy: {}", hasReadPolicy, hasWritePolicy);

        // Rule 4: Has read policy only + Read operation
        if (hasReadPolicy
            && !hasWritePolicy
            && operationType == LbacOperationClassifier.OperationType.READ) {
          LOGGER.debug("Rule 4: Has read policy only + Read operation, triggering LBAC");
          return evaluatePolicy(userLabelPolicy, databaseLabel, devicePath);
        }

        // Rule 5: Has read policy only + Write operation
        if (hasReadPolicy
            && !hasWritePolicy
            && operationType == LbacOperationClassifier.OperationType.WRITE) {
          LOGGER.debug("Rule 5: Has read policy only + Write operation, allowing access");
          return LbacCheckResult.allow();
        }

        // Rule 6: Has write policy only + Read operation
        if (!hasReadPolicy
            && hasWritePolicy
            && operationType == LbacOperationClassifier.OperationType.READ) {
          LOGGER.debug("Rule 6: Has write policy only + Read operation, allowing access");
          return LbacCheckResult.allow();
        }

        // Rule 7: Has write policy only + Write operation
        if (!hasReadPolicy
            && hasWritePolicy
            && operationType == LbacOperationClassifier.OperationType.WRITE) {
          LOGGER.debug("Rule 7: Has write policy only + Write operation, triggering LBAC");
          return evaluatePolicy(userLabelPolicy, databaseLabel, devicePath);
        }

        // Rule 8: Has both read and write policies + Any operation
        if (hasReadPolicy && hasWritePolicy) {
          LOGGER.debug("Rule 8: Has both read and write policies + Any operation, triggering LBAC");
          return evaluatePolicy(userLabelPolicy, databaseLabel, devicePath);
        }
      }

      // Default case: allow access
      LOGGER.debug("Default case: allowing access");
      return LbacCheckResult.allow();

    } catch (MetadataException e) {
      LOGGER.error("Metadata error checking device path: {}", devicePath, e);
      return LbacCheckResult.deny("Error accessing database metadata: " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Unexpected error checking device path: {}", devicePath, e);
      return LbacCheckResult.deny("Unexpected error during LBAC check: " + e.getMessage());
    }
  }

  /**
   * Check if the user has a read policy
   *
   * @param userLabelPolicy the user's label policy expression
   * @return true if user has read policy, false otherwise
   */
  private static boolean hasReadPolicy(String userLabelPolicy) {
    // This is a simplified check - in a real implementation, you would need to
    // parse the policy expression to determine if it's a read policy
    // For now, we'll assume any policy is a read policy if it contains "read" or
    // "READ"
    return userLabelPolicy != null && userLabelPolicy.toLowerCase().contains("read");
  }

  /**
   * Check if the user has a write policy
   *
   * @param userLabelPolicy the user's label policy expression
   * @return true if user has write policy, false otherwise
   */
  private static boolean hasWritePolicy(String userLabelPolicy) {
    // This is a simplified check - in a real implementation, you would need to
    // parse the policy expression to determine if it's a write policy
    // For now, we'll assume any policy is a write policy if it contains "write" or
    // "WRITE"
    return userLabelPolicy != null && userLabelPolicy.toLowerCase().contains("write");
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
        LOGGER.debug("Label policy matches for device path: {}", devicePath);
        return LbacCheckResult.allow();
      } else {
        LOGGER.debug("Label policy does not match for device path: {}", devicePath);
        return LbacCheckResult.deny(
            String.format("Label policy does not match for device path: %s", devicePath));
      }
    } catch (Exception e) {
      LOGGER.error("Error evaluating policy for device path: {}", devicePath, e);
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
   * Get user's label policy for the specified operation type
   *
   * @param user the user
   * @param operationType the operation type (READ/WRITE)
   * @return label policy expression or null if none defined
   */
  private static String getUserLabelPolicy(
      User user, LbacOperationClassifier.OperationType operationType) {

    LOGGER.debug(
        "Getting label policy for user: {} and operation: {}", user.getName(), operationType);

    // Check for specific read/write policy expressions first
    if (operationType == LbacOperationClassifier.OperationType.READ) {
      // Check for read-specific policy
      String readPolicyExpression = user.getReadLabelPolicyExpression();
      if (readPolicyExpression != null && !readPolicyExpression.trim().isEmpty()) {
        LOGGER.debug(
            "User {} has specific READ label policy: '{}'", user.getName(), readPolicyExpression);
        return readPolicyExpression;
      }
    } else if (operationType == LbacOperationClassifier.OperationType.WRITE) {
      // Check for write-specific policy
      String writePolicyExpression = user.getWriteLabelPolicyExpression();
      if (writePolicyExpression != null && !writePolicyExpression.trim().isEmpty()) {
        LOGGER.debug(
            "User {} has specific WRITE label policy: '{}'", user.getName(), writePolicyExpression);
        return writePolicyExpression;
      }
    }

    // Fall back to legacy fields for backward compatibility
    String labelPolicyExpression = user.getLabelPolicyExpression();
    String labelPolicyScope = user.getLabelPolicyScope();

    LOGGER.debug(
        "User {} has legacy labelPolicyExpression: '{}', labelPolicyScope: '{}'",
        user.getName(),
        labelPolicyExpression,
        labelPolicyScope);

    // If no label policy expression is set, return null
    if (labelPolicyExpression == null || labelPolicyExpression.trim().isEmpty()) {
      LOGGER.debug("User {} has no label policy expression", user.getName());
      return null;
    }

    // Check if the policy scope matches the operation type
    if (labelPolicyScope != null) {
      // Convert operation type to lowercase string for comparison
      String operationString = operationType.name().toLowerCase();

      // Policy scope should contain the operation type (e.g., "FOR WRITE", "FOR
      // READ")
      if (!labelPolicyScope.toLowerCase().contains(operationString)) {
        LOGGER.debug(
            "User {} legacy label policy scope '{}' does not match operation type {}",
            user.getName(),
            labelPolicyScope,
            operationType);
        return null;
      }
    }

    LOGGER.debug(
        "User {} has matching legacy label policy for {} operation: '{}'",
        user.getName(),
        operationType,
        labelPolicyExpression);

    return labelPolicyExpression;
  }

  /**
   * Check if LBAC is enabled in the system TODO: This should check system configuration
   *
   * @return true if LBAC is enabled, false otherwise
   */
  public static boolean isLbacEnabled() {
    // TODO: Implement configuration check
    // This could check a system property, configuration file, or database setting
    return true; // For now, assume LBAC is enabled
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
}
