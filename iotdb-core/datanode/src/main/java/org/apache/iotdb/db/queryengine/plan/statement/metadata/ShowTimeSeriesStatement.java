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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTreeUtils;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.IAuthorityFetcher;
import org.apache.iotdb.db.auth.LbacPermissionChecker;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SHOW TIMESERIES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW [LATEST] TIMESERIES [pathPattern] [WHERE key { = | CONTAINS } value] [LIMIT limit]
 * [OFFSET offset]
 */
public class ShowTimeSeriesStatement extends ShowStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShowTimeSeriesStatement.class);

  private final PartialPath pathPattern;
  private SchemaFilter schemaFilter;
  // if is true, the result will be sorted according to the inserting frequency of
  // the time series
  private final boolean orderByHeat;
  private WhereCondition timeCondition;

  public ShowTimeSeriesStatement(PartialPath pathPattern, boolean orderByHeat) {
    super();
    this.pathPattern = pathPattern;
    this.orderByHeat = orderByHeat;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setTimeCondition(WhereCondition timeCondition) {
    this.timeCondition = timeCondition;
  }

  public WhereCondition getTimeCondition() {
    return timeCondition;
  }

  public boolean hasTimeCondition() {
    return timeCondition != null;
  }

  /**
   * Filter time series based on LBAC permissions for the given user Only show time series that the
   * user has permission to access
   *
   * @param timeSeriesList Original time series list
   * @param userName Current user name
   * @return Filtered time series list containing only accessible time series
   */
  public List<Object> filterTimeSeriesByLbac(List<Object> timeSeriesList, String userName) {

    // If user is super user, return all time series
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      LOGGER.debug("User {} is super user, showing all time series", userName);
      return timeSeriesList;
    }

    // Get user object for LBAC check
    User user = getUserByName(userName);
    if (user == null) {
      LOGGER.warn("User {} not found for LBAC check, denying access to all time series", userName);
      return Collections.emptyList();
    }

    List<Object> filteredList = new ArrayList<>();

    for (Object timeSeries : timeSeriesList) {
      // Extract database/device path from time series
      String devicePath = extractDevicePathFromTimeSeries(timeSeries);

      if (devicePath != null) {
        // Check LBAC permission for this specific time series
        if (checkTimeSeriesLbacPermission(user, devicePath)) {
          filteredList.add(timeSeries);
          LOGGER.debug(
              "User {} has LBAC permission for time series in device: {}", userName, devicePath);
        } else {
          LOGGER.debug(
              "User {} denied LBAC access to time series in device: {}", userName, devicePath);
        }
      } else {
        // If we can't extract device path, allow access (fallback)
        filteredList.add(timeSeries);
        LOGGER.debug("User {} allowed access to time series (no device path extracted)", userName);
      }
    }

    LOGGER.info(
        "LBAC filtering complete: {} out of {} time series accessible for user {}",
        filteredList.size(),
        timeSeriesList.size(),
        userName);

    return filteredList;
  }

  /**
   * Check LBAC permission for a specific time series device
   *
   * @param user User object
   * @param devicePath Device path to check
   * @return true if user has permission, false otherwise
   */
  private boolean checkTimeSeriesLbacPermission(User user, String devicePath) {
    try {
      // Create a list with single device path for LBAC check
      List<String> devicePaths = new ArrayList<>();
      devicePaths.add(devicePath);

      // Use LbacPermissionChecker to check permission
      LbacPermissionChecker.LbacCheckResult result =
          LbacPermissionChecker.checkLbacPermission(this, user, devicePaths);

      boolean hasPermission = result.isAllowed();
      if (!hasPermission) {
        LOGGER.debug("LBAC check failed for device {}: {}", devicePath, result.getReason());
      }

      return hasPermission;

    } catch (Exception e) {
      LOGGER.error("Error checking LBAC permission for device: {}", devicePath, e);
      return false; // Deny access on error
    }
  }

  /**
   * Extract device path from time series object This is a simplified implementation - in practice
   * you might need to adapt based on actual time series data structure
   *
   * @param timeSeries Time series object
   * @return Device path or null if cannot extract
   */
  private String extractDevicePathFromTimeSeries(Object timeSeries) {
    try {
      // This is a placeholder implementation
      // In practice, you would need to extract the device path from the actual time
      // series data structure
      // For example, if timeSeries is a String representing the full path, extract
      // the device part
      if (timeSeries instanceof String) {
        String fullPath = (String) timeSeries;
        // Extract device path from full path (e.g., "root.device1.sensor1" ->
        // "root.device1")
        int lastDotIndex = fullPath.lastIndexOf('.');
        if (lastDotIndex > 0) {
          return fullPath.substring(0, lastDotIndex);
        }
        return fullPath;
      }

      // For other data structures, implement appropriate extraction logic
      LOGGER.debug(
          "Cannot extract device path from time series object of type: {}",
          timeSeries != null ? timeSeries.getClass().getSimpleName() : "null");
      return null;

    } catch (Exception e) {
      LOGGER.error("Error extracting device path from time series", e);
      return null;
    }
  }

  /**
   * Get user object by username
   *
   * @param userName Username
   * @return User object or null if not found
   */
  private User getUserByName(String userName) {
    try {
      // Try to get user from authority fetcher cache
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      User user = authorityFetcher.getAuthorCache().getUserCache(userName);

      if (user != null) {
        return user;
      }

      // If not in cache, try to fetch from ConfigNode
      // This is a simplified version - in practice you might want to use
      // LbacIntegration.getUserByName
      LOGGER.warn("User {} not found in cache, LBAC filtering may not work correctly", userName);
      return null;

    } catch (Exception e) {
      LOGGER.error("Error getting user by name: {}", userName, e);
      return null;
    }
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // First check if user is super user
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }

    // Perform traditional RBAC permission check
    try {
      if (hasTimeCondition()) {
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          this.authorityScope =
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_DATA));
        }
      } else {
        // For regular SHOW TIMESERIES without time condition
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          this.authorityScope =
              AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA);
        }
      }
    } catch (AuthException e) {
      return new TSStatus(e.getCode().getStatusCode());
    }

    // For SHOW TIMESERIES, we don't perform strict LBAC check here
    // Instead, we will filter the results during execution based on LBAC
    // permissions
    // This allows users to see time series they have permission to access
    // even if some time series don't have labels or don't match their policy
    LOGGER.debug(
        "SHOW TIMESERIES permission check passed for user: {}, will filter results during execution",
        userName);

    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowTimeSeries(this, context);
  }
}
