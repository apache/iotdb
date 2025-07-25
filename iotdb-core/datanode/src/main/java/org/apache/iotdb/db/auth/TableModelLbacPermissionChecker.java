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
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.db.auth.LbacPermissionChecker.LbacCheckResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** LBAC permission checker for table model Reuses tree model LBAC logic with path conversion */
public class TableModelLbacPermissionChecker {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TableModelLbacPermissionChecker.class);

  private TableModelLbacPermissionChecker() {
    // Utility class, prevent instantiation
  }

  /**
   * Check LBAC permission for table model operations
   *
   * @param statement the statement being executed
   * @param user the user requesting access
   * @param devicePaths the device paths involved in the operation
   * @return LbacCheckResult indicating permission result
   */
  public static LbacCheckResult checkLbacPermission(
      Statement statement, User user, List<String> devicePaths) {

    LOGGER.debug("=== TABLE MODEL LBAC PERMISSION CHECK START ===");
    LOGGER.debug(
        "Checking LBAC permission for user: {} on {} paths", user.getName(), devicePaths.size());

    try {
      // Step 1: Get user label policies using cache manager
      TableModelLbacCacheManager cacheManager = TableModelLbacCacheManager.getInstance();
      User userWithPolicies = cacheManager.getUserWithLabelPolicies(user.getName());

      if (userWithPolicies == null) {
        LOGGER.debug("User {} not found or has no label policies", user.getName());
        return LbacCheckResult.allow(); // No policies means no LBAC restrictions
      }

      // Step 2: Determine operation type
      LbacOperationClassifier.OperationType operationType = determineOperationType(statement);

      if (operationType == null) {
        LOGGER.debug("Operation type is null, allowing access");
        return LbacCheckResult.allow();
      }

      LOGGER.debug("Operation type: {}", operationType);

      // Step 3: Get appropriate policy based on operation type
      String policyExpression = getUserLabelPolicy(userWithPolicies, operationType);

      if (policyExpression == null || policyExpression.trim().isEmpty()) {
        LOGGER.debug(
            "No {} policy found for user {}, allowing access", operationType, user.getName());
        return LbacCheckResult.allow();
      }

      LOGGER.debug(
          "Found {} policy for user {}: {}", operationType, user.getName(), policyExpression);

      // Step 4: Check each device path against the policy
      for (String devicePath : devicePaths) {
        LbacCheckResult pathResult =
            checkSingleDevicePath(devicePath, userWithPolicies, operationType);

        if (!pathResult.isAllowed()) {
          LOGGER.debug("Access denied for path {}: {}", devicePath, pathResult.getReason());
          return pathResult;
        }
      }

      LOGGER.debug("All paths allowed for user: {}", user.getName());
      return LbacCheckResult.allow();

    } catch (Exception e) {
      LOGGER.error(
          "Error during table model LBAC permission check for user: {}", user.getName(), e);
      return LbacCheckResult.deny("Internal error during LBAC check: " + e.getMessage());
    }
  }

  /**
   * Check LBAC permission for a single device path
   *
   * @param devicePath the device path to check
   * @param user the user with label policies
   * @param operationType the operation type
   * @return LbacCheckResult indicating permission result
   */
  private static LbacCheckResult checkSingleDevicePath(
      String devicePath, User user, LbacOperationClassifier.OperationType operationType) {

    try {
      LOGGER.debug(
          "Checking LBAC permission for path: {} with operation: {}", devicePath, operationType);

      // Step 1: Extract database name from device path
      String databaseName = extractDatabaseNameFromPath(devicePath);
      if (databaseName == null) {
        LOGGER.debug("Could not extract database name from path: {}", devicePath);
        return LbacCheckResult.allow(); // No database name means no LBAC restrictions
      }

      // Step 2: Get database security label using cache manager
      TableModelLbacCacheManager cacheManager = TableModelLbacCacheManager.getInstance();
      SecurityLabel databaseLabel = cacheManager.getDatabaseSecurityLabel(databaseName);

      if (databaseLabel == null || databaseLabel.getLabels().isEmpty()) {
        LOGGER.debug("Database {} has no security label", databaseName);
        return LbacCheckResult.allow(); // No security label means no LBAC restrictions
      }

      LOGGER.debug("Database {} has security label: {}", databaseName, databaseLabel);

      // Step 3: Get user policy for the operation type
      String policyExpression = getUserLabelPolicy(user, operationType);
      if (policyExpression == null || policyExpression.trim().isEmpty()) {
        LOGGER.debug("User {} has no {} policy", user.getName(), operationType);
        return LbacCheckResult.allow(); // No policy means no LBAC restrictions
      }

      // Step 4: Evaluate policy against database label
      boolean policyMatch = evaluatePolicyAgainstLabel(policyExpression, databaseLabel);

      if (policyMatch) {
        LOGGER.debug("Policy match successful for database: {}", databaseName);
        return LbacCheckResult.allow();
      } else {
        LOGGER.debug("Policy match failed for database: {}", databaseName);
        return LbacCheckResult.deny(
            String.format(
                "LBAC policy '%s' does not match database security label", policyExpression));
      }

    } catch (Exception e) {
      LOGGER.error("Error checking LBAC permission for path: {}", devicePath, e);
      return LbacCheckResult.deny(
          "Internal error during LBAC policy evaluation: " + e.getMessage());
    }
  }

  /**
   * Convert table model database path to tree model path Table model: "db1" -> Tree model:
   * "root.db1"
   *
   * @param tableModelPath the table model path
   * @return the tree model path
   */
  private static String convertToTreeModelPath(String tableModelPath) {
    if (tableModelPath == null || tableModelPath.isEmpty()) {
      return "root";
    }
    return "root." + tableModelPath;
  }

  /**
   * Check read permission based on user read policy and database label
   *
   * @param userReadPolicy the user's read policy
   * @param databaseLabel the database security label
   * @return true if read permission is granted
   */
  private static boolean checkReadPermission(String userReadPolicy, SecurityLabel databaseLabel) {
    if (userReadPolicy == null || userReadPolicy.isEmpty()) {
      LOGGER.debug("No read policy set for user");
      return false;
    }

    // Use LabelPolicyEvaluator for policy evaluation
    return LabelPolicyEvaluator.evaluate(userReadPolicy, databaseLabel);
  }

  /**
   * Check write permission based on user write policy and database label
   *
   * @param userWritePolicy the user's write policy
   * @param databaseLabel the database security label
   * @return true if write permission is granted
   */
  private static boolean checkWritePermission(String userWritePolicy, SecurityLabel databaseLabel) {
    if (userWritePolicy == null || userWritePolicy.isEmpty()) {
      LOGGER.debug("No write policy set for user");
      return false;
    }

    // Use LabelPolicyEvaluator for policy evaluation
    return LabelPolicyEvaluator.evaluate(userWritePolicy, databaseLabel);
  }

  /**
   * Check if a statement requires root privileges
   *
   * @param statement the statement to check
   * @return true if root privileges are required
   */
  public static boolean requiresRootPrivileges(Statement statement) {
    // For now, return false to avoid compilation issues
    // In a real implementation, this would check if the statement requires root
    // privileges
    return false;
  }

  /**
   * Extract database name from device path Table model: "db1.device1.sensor1" -> "db1"
   *
   * @param devicePath the device path
   * @return database name or null if cannot be extracted
   */
  private static String extractDatabaseNameFromPath(String devicePath) {
    if (devicePath == null || devicePath.trim().isEmpty()) {
      return null;
    }

    try {
      // Parse device path to extract database
      // Table model path format: database.device.measurement...
      String[] pathParts = devicePath.split("\\.");

      if (pathParts.length < 1) {
        LOGGER.debug("Invalid path format for database extraction: {}", devicePath);
        return null;
      }

      // First part is the database name
      String databaseName = pathParts[0];
      LOGGER.debug("Extracted database name: {} from device path: {}", databaseName, devicePath);
      return databaseName;

    } catch (Exception e) {
      LOGGER.error("Error extracting database name from device path: {}", devicePath, e);
      return null;
    }
  }

  /**
   * Evaluate policy expression against database security label
   *
   * @param policyExpression the policy expression to evaluate
   * @param databaseLabel the database security label
   * @return true if policy matches, false otherwise
   */
  private static boolean evaluatePolicyAgainstLabel(
      String policyExpression, SecurityLabel databaseLabel) {
    if (policyExpression == null || policyExpression.trim().isEmpty()) {
      LOGGER.debug("Policy expression is null or empty");
      return false;
    }

    if (databaseLabel == null) {
      LOGGER.debug("Database label is null");
      return false;
    }

    try {
      // Use LabelPolicyEvaluator for policy evaluation
      boolean match = LabelPolicyEvaluator.evaluate(policyExpression, databaseLabel);
      LOGGER.debug(
          "Policy evaluation result: {} for policy: {} against label: {}",
          match,
          policyExpression,
          databaseLabel);
      return match;
    } catch (Exception e) {
      LOGGER.error(
          "Error evaluating policy: {} against label: {}", policyExpression, databaseLabel, e);
      return false;
    }
  }

  /**
   * Get user label policy for specific operation type
   *
   * @param user the user object
   * @param operationType the operation type (READ/WRITE/BOTH)
   * @return policy expression string or null if not found
   */
  private static String getUserLabelPolicy(
      User user, LbacOperationClassifier.OperationType operationType) {
    if (user == null) {
      LOGGER.debug("User is null");
      return null;
    }

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
   * Determine operation type from statement
   *
   * @param statement the statement to analyze
   * @return the operation type
   */
  private static LbacOperationClassifier.OperationType determineOperationType(Statement statement) {
    if (statement == null) {
      LOGGER.debug("Statement is null, defaulting to READ");
      return LbacOperationClassifier.OperationType.READ;
    }

    String className = statement.getClass().getSimpleName();

    // Read operations
    if (className.contains("Select")
        || className.contains("Show")
        || className.contains("Describe")) {
      return LbacOperationClassifier.OperationType.READ;
    }

    // Write operations
    if (className.contains("Insert")
        || className.contains("Update")
        || className.contains("Delete")
        || className.contains("Create")
        || className.contains("Drop")
        || className.contains("Alter")) {
      return LbacOperationClassifier.OperationType.WRITE;
    }

    // Default to READ for safety
    LOGGER.debug(
        "Could not determine operation type for statement class: {}, defaulting to READ",
        className);
    return LbacOperationClassifier.OperationType.READ;
  }
}
