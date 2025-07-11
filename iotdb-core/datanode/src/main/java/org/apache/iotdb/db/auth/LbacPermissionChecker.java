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

    try {
      LOGGER.warn(
          "Checking LBAC permission for user: {} on statement: {}",
          user.getName(),
          statement.getClass().getSimpleName());

      // Step 1: Classify operation type (READ/WRITE)
      LbacOperationClassifier.OperationType operationType =
          LbacOperationClassifier.classifyOperation(statement);

      LOGGER.warn("Operation classified as: {}", operationType);

      // Step 2: Get user's label policies for this operation type
      String userLabelPolicy = getUserLabelPolicy(user, operationType);

      LOGGER.warn("User label policy for {} operation: {}", operationType, userLabelPolicy);

      // Step 3: If user has no label policy, apply default rules
      if (userLabelPolicy == null || userLabelPolicy.trim().isEmpty()) {
        LOGGER.warn("User has no label policy, applying default rules");
        return handleNoUserPolicy(devicePaths, operationType);
      }

      LOGGER.warn("User has label policy, checking device paths...");

      // Step 4: Check LBAC for each device path involved
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
   * Check LBAC permission for a single device path
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
      LOGGER.debug("Checking device path: {} with policy: {}", devicePath, userLabelPolicy);

      // Get database security label for this device path
      SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(devicePath);

      // If database has no security label
      if (databaseLabel == null) {
        LOGGER.debug(
            "Database has no security label for path: {}, checking policy requirements",
            devicePath);

        // If user has a policy but database has no label, this is typically denied
        // unless the policy explicitly allows access to unlabeled resources
        return LbacCheckResult.deny(
            "Database has no security label but user has label policy restrictions");
      }

      // Evaluate user's label policy against database labels
      boolean policyMatches = LabelPolicyEvaluator.evaluate(userLabelPolicy, databaseLabel);

      if (policyMatches) {
        LOGGER.debug("Label policy matches for device path: {}", devicePath);
        return LbacCheckResult.allow();
      } else {
        LOGGER.debug("Label policy does not match for device path: {}", devicePath);
        return LbacCheckResult.deny(
            String.format("Label policy does not match for device path: %s", devicePath));
      }

    } catch (MetadataException e) {
      LOGGER.error("Metadata error checking device path: {}", devicePath, e);
      return LbacCheckResult.deny("Error accessing database metadata: " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Unexpected error checking device path: {}", devicePath, e);
      return LbacCheckResult.deny("Unexpected error during LBAC check: " + e.getMessage());
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

    LOGGER.debug("User has no label policy, applying default rules");

    // Default behavior: users without label policies can access databases without
    // labels
    // but cannot access databases with labels
    for (String devicePath : devicePaths) {
      try {
        SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(devicePath);

        if (databaseLabel != null && !databaseLabel.getLabels().isEmpty()) {
          LOGGER.debug(
              "User has no policy but database has labels for path: {}, denying access",
              devicePath);
          return LbacCheckResult.deny(
              String.format(
                  "User has no label policy but database has security labels for path: %s",
                  devicePath));
        }
      } catch (MetadataException e) {
        LOGGER.error("Error checking database labels for path: {}", devicePath, e);
        return LbacCheckResult.deny("Error accessing database metadata: " + e.getMessage());
      }
    }

    LOGGER.debug("User has no policy and databases have no labels, allowing access");
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

    // Get the user's label policy expression from User object
    String labelPolicyExpression = user.getLabelPolicyExpression();
    String labelPolicyScope = user.getLabelPolicyScope();

    LOGGER.debug(
        "User {} has labelPolicyExpression: '{}', labelPolicyScope: '{}'",
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
            "User {} label policy scope '{}' does not match operation type {}",
            user.getName(),
            labelPolicyScope,
            operationType);
        return null;
      }
    }

    LOGGER.debug(
        "User {} has matching label policy for {} operation: '{}'",
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
