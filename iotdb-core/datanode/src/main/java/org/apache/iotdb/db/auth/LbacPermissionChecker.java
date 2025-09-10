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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.iotdb.db.auth.AuthorityChecker.SUPER_USER;

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
          String dbPath = extractDatabasePathFromPath(path.getFullPath());
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
   * Check LBAC permission for database access
   *
   * @param userName the username
   * @param databasePath the database path
   * @param operationType the operation type (READ/WRITE/BOTH)
   * @return true if access is allowed, false otherwise
   */
  public static boolean checkLbacPermissionForDatabase(
      String userName, String databasePath, LbacOperationClassifier.OperationType operationType) {

    try {

      // Check if LBAC is enabled
      if (!isLbacEnabled()) {
        return true;
      }

      // Super user has access to everything
      if (SUPER_USER.equals(userName)) {
        return true;
      }

      // Get user object
      User user = getUserByName(userName);
      if (user == null) {
        return false;
      }

      String cleanDatabasePath = extractDatabasePathWithModelDetection(databasePath);
      if (cleanDatabasePath == null) {
        return false;
      }

      boolean isTableModel = isTableModelPath(databasePath);

      SecurityLabel databaseLabel;
      if (isTableModel) {
        databaseLabel = getTableModelDatabaseSecurityLabel(cleanDatabasePath);
      } else {
        databaseLabel = DatabaseLabelFetcher.getSecurityLabelForPath(cleanDatabasePath);
      }

      boolean databaseHasLabels = (databaseLabel != null && !databaseLabel.getLabels().isEmpty());

      // Get user's read and write policies
      String readPolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.READ);
      String writePolicy = getUserLabelPolicy(user, LbacOperationClassifier.OperationType.WRITE);

      boolean hasReadPolicy = (readPolicy != null && !readPolicy.trim().isEmpty());
      boolean hasWritePolicy = (writePolicy != null && !writePolicy.trim().isEmpty());
      boolean hasAnyPolicy = hasReadPolicy || hasWritePolicy;

      // Rule 1 & 2: No read/write policies (both read and write policies are empty)
      if (!hasAnyPolicy) {
        return true;
      }

      // Rule 3: Has any policy + No labels
      if (hasAnyPolicy && !databaseHasLabels) {
        return handleNoLabelsCaseForDatabase(operationType, hasReadPolicy, hasWritePolicy);
      }

      // Rule 4: Has any policy + Has labels
      if (hasAnyPolicy && databaseHasLabels) {
        return handleHasLabelsCaseForDatabase(
            user,
            cleanDatabasePath,
            operationType,
            databaseLabel.getLabels(),
            hasReadPolicy,
            hasWritePolicy);
      }

      // Default case: allow access
      return true;

    } catch (Exception e) {
      return false; // Deny access on error
    }
  }

  private static String extractDatabasePathWithModelDetection(String path) {
    if (path == null || path.trim().isEmpty()) {
      return null;
    }

    if (path.contains("*") || path.contains("**")) {
      return null;
    }

    if (isTableModelPath(path)) {
      return path;
    } else {
      return extractDatabasePathFromPath(path);
    }
  }

  private static boolean isTableModelPath(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return !path.startsWith("root.");
  }

  private static SecurityLabel getTableModelDatabaseSecurityLabel(String databaseName) {
    try {

      return TableModelLbacCacheManager.getInstance().getDatabaseSecurityLabel(databaseName);
    } catch (Exception e) {
      LOGGER.warn("Failed to get table model database security label for: {}", databaseName, e);
      return null;
    }
  }

  /**
   * Get user object by username This method provides a centralized way to get user information for
   * LBAC checks
   *
   * @param userName Username
   * @return User object or null if not found
   */
  public static User getUserByName(String userName) {
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
   * @param Path the path to extract database path from
   * @return the extracted database path or null if not applicable
   */
  public static String extractDatabasePathFromPath(String Path) {
    if (Path == null || Path.trim().isEmpty()) {
      return null;
    }

    if (Path.contains("*") || Path.contains("**")) {
      return null;
    }

    // Split by dots and take the first two parts as database path
    String[] parts = Path.split("\\.");
    if (parts.length >= 2) {
      String databasePath = parts[0] + "." + parts[1];
      return databasePath;
    } else if (parts.length == 1) {
      return parts[0];
    }
    return null;
  }

  /**
   * Get database security label for a device path
   *
   * @param Path the device path to get security label for
   * @return SecurityLabel object or null if not found
   */
  private static SecurityLabel getDatabaseSecurityLabel(String Path) {
    try {
      // Extract database path from device path
      String databasePath = extractDatabasePathFromPath(Path);
      if (databasePath == null) {
        return null;
      }

      // Get security label from database
      return DatabaseLabelFetcher.getSecurityLabelForPath(databasePath);
    } catch (Exception e) {
      return null;
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
        return LbacCheckResult.allow();
      } else {
        return LbacCheckResult.deny(
            String.format(
                "Label policy does not match for database security label: %s", devicePath));
      }
    } catch (Exception e) {
      return LbacCheckResult.deny("Error evaluating label policy: " + e.getMessage());
    }
  }

  /**
   * Get user label policy for specific operation type Must separate read policy and write policy,
   * policies only have two types
   *
   * @param user User object
   * @param operationType Operation type (READ/WRITE)
   * @return Label policy expression or null if no policy exists
   */
  public static String getUserLabelPolicy(
      User user, LbacOperationClassifier.OperationType operationType) {

    // Use independent read and write policy fields
    if (operationType == LbacOperationClassifier.OperationType.READ) {
      // Use read policy for READ operations
      String readPolicyExpression = user.getReadLabelPolicyExpression();
      if (readPolicyExpression != null && !readPolicyExpression.trim().isEmpty()) {
        return readPolicyExpression;
      } else {
        return null;
      }
    } else if (operationType == LbacOperationClassifier.OperationType.WRITE) {
      // Use write policy for WRITE operations
      String writePolicyExpression = user.getWriteLabelPolicyExpression();
      if (writePolicyExpression != null && !writePolicyExpression.trim().isEmpty()) {
        return writePolicyExpression;
      } else {
        return null;
      }
    } else if (operationType == LbacOperationClassifier.OperationType.BOTH) {
      // For BOTH operations, we need to check both read and write policies
      // This is handled by the calling method, so we return null here
      return null;
    }

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
    }
    return false;
  }

  /**
   * Convert RBAC privilege type to LBAC operation type. RBAC write_data + write_schema → LBAC write
   * RBAC read_data + read_schema → LBAC read
   *
   * @param privilegeType The RBAC privilege type
   * @return The corresponding LBAC operation type, or null for non-database operations
   */
  public static LbacOperationClassifier.OperationType convertPrivilegeTypeToLbacOperation(
      PrivilegeType privilegeType) {
    switch (privilegeType) {
      case READ_DATA:
      case READ_SCHEMA:
        return LbacOperationClassifier.OperationType.READ;
      case WRITE_DATA:
      case WRITE_SCHEMA:
        return LbacOperationClassifier.OperationType.WRITE;
      default:
        // For non-database operations, return null
        return null;
    }
  }
}
