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
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        // Get user's read and write policies
        String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
        String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);

        boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
        boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());

        LOGGER.warn("User has read policy: {}, write policy: {}", hasReadPolicy, hasWritePolicy);

        // Check if user has any policy (read or write)
        if (!hasReadPolicy && !hasWritePolicy) {
          // No policies - allow access regardless of database labels
          LOGGER.warn("User has no policies, allowing access for BOTH operation");
          return LbacCheckResult.allow();
        }

        // User has at least one policy - check each device path
        for (String devicePath : devicePaths) {
          LOGGER.warn("Checking BOTH operation for device path: {}", devicePath);

          // Get database security label
          SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(devicePath);
          boolean databaseHasLabels = (databaseLabel != null);

          // If database has no labels and user has any policy, deny access
          if (!databaseHasLabels && (hasReadPolicy || hasWritePolicy)) {
            LOGGER.warn("Database has no labels but user has policies, denying access");
            return LbacCheckResult.deny(
                "Database has no security label but user has label policy restrictions");
          }

          // If database has labels, check policies
          if (databaseHasLabels) {
            // Check read policy if user has read policy
            if (hasReadPolicy) {
              LOGGER.warn("Checking read policy for BOTH operation");
              LbacCheckResult readResult = evaluatePolicy(readPolicy, databaseLabel, devicePath);
              if (!readResult.isAllowed()) {
                LOGGER.warn(
                    "Read policy check failed for BOTH operation: {}", readResult.getReason());
                return readResult;
              }
            }

            // Check write policy if user has write policy
            if (hasWritePolicy) {
              LOGGER.warn("Checking write policy for BOTH operation");
              LbacCheckResult writeResult = evaluatePolicy(writePolicy, databaseLabel, devicePath);
              if (!writeResult.isAllowed()) {
                LOGGER.warn(
                    "Write policy check failed for BOTH operation: {}", writeResult.getReason());
                return writeResult;
              }
            }
          }
        }

        LOGGER.warn("=== LBAC CHECK BOTH ALLOWED === Both read and write permissions granted");
        return LbacCheckResult.allow();
      }

      // Step 3: Handle READ/WRITE operations
      String userLabelPolicy = getUserLabelPolicy(user, operationType);
      LOGGER.warn("User label policy for {} operation: {}", operationType, userLabelPolicy);

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
   * Check LBAC permission for a single device path Implements the new strategy logic: 1. No
   * read/write policies + No labels: Allow access (no LBAC triggered) 2. No read/write policies +
   * Has labels: Allow access (no LBAC triggered) 3. Has any policy + No labels: Deny access (LBAC
   * triggered) 4. Has any policy + Has labels: Evaluate specific policy based on operation type
   *
   * @param devicePath the device path to check
   * @param userLabelPolicy the user's label policy expression for the specific operation type
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

      // Rule 3: Has any policy + No labels
      if (userHasPolicy && !databaseHasLabels) {
        LOGGER.debug("User has policies but database has no labels, denying access");
        return LbacCheckResult.deny(
            "Database has no security label but user has label policy restrictions");
      }

      // Rule 4: Has any policy + Has labels
      if (userHasPolicy && databaseHasLabels) {
        LOGGER.debug("User has policies and database has labels, evaluating policy");
        return evaluatePolicy(userLabelPolicy, databaseLabel, devicePath);
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

  /**
   * Centralized LBAC permission check for all database-related operations This method should be
   * called from Statement.checkPermissionBeforeProcess() methods to provide unified LBAC
   * enforcement across all database operations
   *
   * @param statement the statement to check permissions for
   * @param userName the username attempting to execute the statement
   * @return TSStatus indicating the permission check result
   */
  public static TSStatus checkLbacPermissionForStatement(Statement statement, String userName) {
    try {
      LOGGER.debug(
          "=== CENTRALIZED LBAC CHECK === Statement: {}, User: {}",
          statement != null ? statement.getClass().getSimpleName() : "null",
          userName);

      // Check if this is a database-related operation that should be subject to LBAC
      if (!LbacOperationClassifier.isLbacRelevantOperation(statement)) {
        LOGGER.debug(
            "Statement {} is not a database-related operation for LBAC, allowing access",
            statement != null ? statement.getClass().getSimpleName() : "null");
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }

      // Get user object
      User user = getUserByName(userName);
      if (user == null) {
        LOGGER.warn("User not found for LBAC check: {}", userName);
        return new TSStatus(TSStatusCode.USER_NOT_EXIST.getStatusCode())
            .setMessage("User not found for LBAC check");
      }

      // Get device paths from statement
      List<? extends PartialPath> paths = statement.getPaths();
      List<String> devicePaths = new ArrayList<>();
      for (PartialPath path : paths) {
        devicePaths.add(path.getFullPath());
      }

      LOGGER.debug(
          "Checking LBAC permission for {} device paths: {}", devicePaths.size(), devicePaths);

      // Perform LBAC permission check
      LbacCheckResult result = checkLbacPermission(statement, user, devicePaths);

      if (result.isAllowed()) {
        LOGGER.debug("LBAC permission check passed for user: {}", userName);
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      } else {
        LOGGER.debug(
            "LBAC permission check failed for user: {} - {}", userName, result.getReason());
        return new TSStatus(result.getStatusCode().getStatusCode())
            .setMessage("LBAC permission check failed: " + result.getReason());
      }

    } catch (Exception e) {
      LOGGER.error("LBAC permission check failed with exception for user: {}", userName, e);
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("LBAC permission check failed: " + e.getMessage());
    }
  }

  /**
   * Check if user has any label policies for read or write operations This method can be used to
   * determine if a user has any LBAC policies set
   *
   * @param user User object to check
   * @return true if user has any label policies, false otherwise
   */
  public static boolean userHasLabelPolicies(User user) {
    if (user == null) {
      return false;
    }

    // Check for read policy
    String readPolicy = user.getReadLabelPolicyExpression();
    boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());

    // Check for write policy
    String writePolicy = user.getWriteLabelPolicyExpression();
    boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());

    // Check for legacy policy
    String legacyPolicy = user.getLabelPolicyExpression();
    boolean hasLegacyPolicy = (legacyPolicy != null && !legacyPolicy.trim().isEmpty());

    boolean hasPolicies = hasReadPolicy || hasWritePolicy || hasLegacyPolicy;

    LOGGER.debug(
        "User {} - Read policy: {}, Write policy: {}, Legacy policy: {}, Has policies: {}",
        user.getName(),
        hasReadPolicy,
        hasWritePolicy,
        hasLegacyPolicy,
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
   * Filter database info map based on LBAC permissions for the current user. Only show databases
   * that the user has permission to access. If user has policy but database has no label, deny
   * access to that database.
   *
   * @param databaseInfoMap Original database info map
   * @param currentUser Current user name
   * @return Filtered database info map containing only accessible databases
   */
  public static Map<String, TDatabaseInfo> filterDatabaseInfoMapByLbac(
      Map<String, TDatabaseInfo> databaseInfoMap, String currentUser) {

    LOGGER.warn("=== LBAC DATABASE FILTER START === User: {}", currentUser);

    if (currentUser == null) {
      LOGGER.warn("Current user is null, denying access to all databases");
      return Collections.emptyMap();
    }

    // If user is super user, return all databases
    if (AuthorityChecker.SUPER_USER.equals(currentUser)) {
      LOGGER.debug("User {} is super user, showing all databases", currentUser);
      return databaseInfoMap;
    }

    // Get user object for LBAC check
    User user = getUserByName(currentUser);
    if (user == null) {
      LOGGER.warn("User {} not found for LBAC check, denying access to all databases", currentUser);
      return Collections.emptyMap();
    }

    Map<String, TDatabaseInfo> filteredMap = new HashMap<>();

    for (Map.Entry<String, TDatabaseInfo> entry : databaseInfoMap.entrySet()) {
      String databaseName = entry.getKey();
      TDatabaseInfo databaseInfo = entry.getValue();

      // Check LBAC permission for this specific database
      if (checkDatabaseLbacPermission(user, databaseName, databaseInfo)) {
        filteredMap.put(databaseName, databaseInfo);
        LOGGER.debug("User {} has LBAC permission for database: {}", currentUser, databaseName);
      } else {
        LOGGER.debug("User {} denied LBAC access to database: {}", currentUser, databaseName);
      }
    }

    LOGGER.warn(
        "=== LBAC DATABASE FILTER COMPLETE === Filtered {} databases for user: {}",
        filteredMap.size(),
        currentUser);
    return filteredMap;
  }

  /**
   * Check LBAC permission for a specific database. If user has policy but database has no label,
   * deny access.
   *
   * @param user User object
   * @param databaseName Database name to check
   * @param databaseInfo Database info containing security labels
   * @return true if user has permission, false otherwise
   */
  private static boolean checkDatabaseLbacPermission(
      User user, String databaseName, TDatabaseInfo databaseInfo) {
    try {
      // Get database security labels
      Map<String, String> securityLabels = databaseInfo.getSecurityLabel();

      // Check if user has any label policies
      boolean userHasPolicies = userHasLabelPolicies(user);

      // Check if database has security labels
      boolean databaseHasLabels = (securityLabels != null && !securityLabels.isEmpty());

      LOGGER.debug(
          "Database: {}, User has policies: {}, Database has labels: {}",
          databaseName,
          userHasPolicies,
          databaseHasLabels);

      // If user has policies but database has no labels, deny access
      if (userHasPolicies && !databaseHasLabels) {
        LOGGER.debug(
            "User has label policies but database {} has no security labels, denying access",
            databaseName);
        return false;
      }

      // If database has no labels and user has no policies, allow access
      if (!databaseHasLabels && !userHasPolicies) {
        LOGGER.debug(
            "Neither user nor database has labels/policies, allowing access to database {}",
            databaseName);
        return true;
      }

      // If database has no labels but user has policies, deny access
      if (!databaseHasLabels && userHasPolicies) {
        LOGGER.debug(
            "User has policies but database {} has no labels, denying access", databaseName);
        return false;
      }

      // Both user has policies and database has labels - check policy matching
      LOGGER.debug(
          "Both user has policies and database has labels, checking policy matching for database {}",
          databaseName);

      // Create device path list for LBAC check
      List<String> devicePaths = new ArrayList<>();
      devicePaths.add(databaseName);

      // Use LbacPermissionChecker to check LBAC permission
      // Note: We need to create a dummy statement for the check
      // This is a simplified check for database listing
      LbacCheckResult result = checkLbacPermissionForDatabase(user, devicePaths);
      boolean hasPermission = result.isAllowed();

      if (hasPermission) {
        LOGGER.debug("LBAC permission check passed for database {}", databaseName);
      } else {
        LOGGER.debug(
            "LBAC permission check failed for database {}: {}", databaseName, result.getReason());
      }

      return hasPermission;
    } catch (Exception e) {
      LOGGER.warn("LBAC permission check failed for database {}: {}", databaseName, e.getMessage());
      return false; // Deny access on error
    }
  }

  /**
   * Simplified LBAC permission check for database listing. This method checks if user has any read
   * policy and if it matches the database labels.
   *
   * @param user User object
   * @param devicePaths List of device paths (database names)
   * @return LbacCheckResult for the permission check
   */
  private static LbacCheckResult checkLbacPermissionForDatabase(
      User user, List<String> devicePaths) {
    try {
      // Check if user has any read policy
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());

      if (!hasReadPolicy) {
        LOGGER.debug("User has no read policy, allowing access for database listing");
        return LbacCheckResult.allow();
      }

      // User has read policy - check each device path
      for (String devicePath : devicePaths) {
        LOGGER.debug("Checking read policy for database: {}", devicePath);

        // Get database security label
        SecurityLabel databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(devicePath);
        boolean databaseHasLabels = (databaseLabel != null);

        // If database has no labels and user has read policy, deny access
        if (!databaseHasLabels && hasReadPolicy) {
          LOGGER.debug("Database has no labels but user has read policy, denying access");
          return LbacCheckResult.deny(
              "Database has no security label but user has label policy restrictions");
        }

        // If database has labels, check read policy
        if (databaseHasLabels) {
          LOGGER.debug("Checking read policy for database with labels");
          LbacCheckResult readResult = evaluatePolicy(readPolicy, databaseLabel, devicePath);
          if (!readResult.isAllowed()) {
            LOGGER.debug("Read policy check failed: {}", readResult.getReason());
            return readResult;
          }
        }
      }

      LOGGER.debug("Database listing LBAC check passed");
      return LbacCheckResult.allow();

    } catch (Exception e) {
      LOGGER.error("Error during database listing LBAC check for user: {}", user.getName(), e);
      return LbacCheckResult.deny("Internal error during LBAC check: " + e.getMessage());
    }
  }

  /**
   * Filter security label map based on LBAC permissions for the current user. Only include
   * databases that the user has permission to access. If user has policy but database has no label,
   * deny access to that database.
   *
   * @param securityLabelMap Original security label map (databaseName -> label string)
   * @param currentUser Current user name
   * @return Filtered security label map containing only accessible databases
   */
  public static Map<String, String> filterSecurityLabelMapByLbac(
      Map<String, String> securityLabelMap, String currentUser) {

    LOGGER.info("=== LBAC SECURITY LABEL FILTER START === User: {}", currentUser);
    LOGGER.info("Input security label map: {}", securityLabelMap);

    if (currentUser == null) {
      LOGGER.warn("Current user is null, denying access to all databases");
      return Collections.emptyMap();
    }

    // If user is super user, return all databases
    if (AuthorityChecker.SUPER_USER.equals(currentUser)) {
      LOGGER.debug("User {} is super user, showing all databases", currentUser);
      return securityLabelMap;
    }

    // Get user object for LBAC check
    User user = getUserByName(currentUser);
    if (user == null) {
      LOGGER.warn("User {} not found for LBAC check, denying access to all databases", currentUser);
      return Collections.emptyMap();
    }

    Map<String, String> filteredMap = new HashMap<>();
    int totalDatabases = securityLabelMap != null ? securityLabelMap.size() : 0;
    int accessibleDatabases = 0;
    int deniedDatabases = 0;

    if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
      for (Map.Entry<String, String> entry : securityLabelMap.entrySet()) {
        String databaseName = entry.getKey();
        String securityLabel = entry.getValue();

        LOGGER.debug(
            "Processing database: '{}', security label: '{}'", databaseName, securityLabel);

        // Check if user has any label policies
        boolean userHasPolicies = userHasLabelPolicies(user);

        // Check if database has security labels (non-empty label string)
        // Note: ConfigNode returns empty string "" for databases without labels
        boolean databaseHasLabels = (securityLabel != null && !securityLabel.trim().isEmpty());

        LOGGER.debug(
            "Database: {}, User has policies: {}, Database has labels: {}",
            databaseName,
            userHasPolicies,
            databaseHasLabels);

        // If user has policies but database has no labels, deny access
        if (userHasPolicies && !databaseHasLabels) {
          deniedDatabases++;
          LOGGER.debug(
              "User has label policies but database {} has no security labels, denying access",
              databaseName);
          continue;
        }

        // If database has no labels and user has no policies, allow access
        if (!databaseHasLabels && !userHasPolicies) {
          accessibleDatabases++;
          filteredMap.put(databaseName, securityLabel);
          LOGGER.debug(
              "Neither user nor database has labels/policies, allowing access to database {}",
              databaseName);
          continue;
        }

        // For databases with labels and users with policies, check LBAC permission
        try {
          List<String> devicePaths = new ArrayList<>();
          devicePaths.add(databaseName);

          // Create a dummy statement for the check
          // This is a simplified check for database listing
          LbacCheckResult result = checkLbacPermissionForDatabase(user, devicePaths);
          boolean hasPermission = result.isAllowed();

          if (hasPermission) {
            accessibleDatabases++;
            filteredMap.put(databaseName, securityLabel);
            LOGGER.debug("LBAC permission check passed for database {}", databaseName);
          } else {
            deniedDatabases++;
            LOGGER.debug(
                "LBAC permission check failed for database {}: {}",
                databaseName,
                result.getReason());
          }
        } catch (Exception e) {
          deniedDatabases++;
          LOGGER.warn(
              "LBAC permission check failed for database {}: {}", databaseName, e.getMessage());
        }
      }
    } else {
      LOGGER.warn("Security label map is null or empty, no databases to process");
    }

    LOGGER.info(
        "LBAC filtering completed for user {}: {}/{} databases accessible, {} denied",
        currentUser,
        accessibleDatabases,
        totalDatabases,
        deniedDatabases);
    LOGGER.info("Filtered result: {}", filteredMap);

    return filteredMap;
  }
}
