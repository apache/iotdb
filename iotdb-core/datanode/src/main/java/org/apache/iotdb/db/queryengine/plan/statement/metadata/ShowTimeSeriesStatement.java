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
   * Filter time series list based on LBAC permissions for the current user Only show time series
   * that the user has permission to access If user has policy but database has no label, deny
   * access to that time series
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
      // Extract database path from time series
      String databasePath = extractDatabasePathFromTimeSeries(timeSeries);

      if (databasePath != null) {
        // Check LBAC permission for this specific database
        if (checkDatabaseLbacPermission(user, databasePath)) {
          filteredList.add(timeSeries);
          LOGGER.debug(
              "User {} has LBAC permission for time series in database: {}",
              userName,
              databasePath);
        } else {
          LOGGER.debug(
              "User {} denied LBAC access to time series in database: {}", userName, databasePath);
        }
      } else {
        // If we can't extract database path, allow access (fallback)
        filteredList.add(timeSeries);
        LOGGER.debug(
            "User {} allowed access to time series (no database path extracted)", userName);
      }
    }

    LOGGER.info(
        "LBAC filtering complete: {} out of {} time series accessible for user {}",
        filteredList.size(),
        timeSeriesList.size(),
        userName);

    return filteredList;
  }

  /** Extract database path from time series object */
  private String extractDatabasePathFromTimeSeries(Object timeSeries) {
    try {
      if (timeSeries instanceof String) {
        String fullPath = (String) timeSeries;
        // Extract database path from full path (e.g., "root.database1.device1.sensor1"
        // -> "root.database1")
        String[] pathParts = fullPath.split("\\.");
        if (pathParts.length >= 2) {
          return pathParts[0] + "." + pathParts[1];
        }
      }
      return null;
    } catch (Exception e) {
      LOGGER.error("Error extracting database path from time series: {}", timeSeries, e);
      return null;
    }
  }

  /** Check LBAC permission for a specific database */
  private boolean checkDatabaseLbacPermission(User user, String databasePath) {
    try {
      List<String> databasePaths = new ArrayList<>();
      databasePaths.add(databasePath);

      // Use LbacPermissionChecker to check permission
      LbacPermissionChecker.LbacCheckResult result =
          LbacPermissionChecker.checkLbacPermission(this, user, databasePaths);

      boolean hasPermission = result.isAllowed();
      if (!hasPermission) {
        LOGGER.debug("LBAC check failed for database {}: {}", databasePath, result.getReason());
      }

      return hasPermission;
    } catch (Exception e) {
      LOGGER.error("Error checking LBAC permission for database: {}", databasePath, e);
      return false; // Deny access on error
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
    // Check RBAC permissions first
    TSStatus rbacStatus;
    if (hasTimeCondition()) {
      try {
        if (!AuthorityChecker.SUPER_USER.equals(userName)) {
          this.authorityScope =
              PathPatternTreeUtils.intersectWithFullPathPrefixTree(
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_SCHEMA),
                  AuthorityChecker.getAuthorizedPathTree(userName, PrivilegeType.READ_DATA));
        }
      } catch (AuthException e) {
        return new TSStatus(e.getCode().getStatusCode());
      }
      rbacStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      rbacStatus = super.checkPermissionBeforeProcess(userName);
    }

    if (rbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return rbacStatus;
    }

    // Perform LBAC permission check for read operation using database paths
    try {
      List<PartialPath> queryPaths = getPaths();
      List<String> databasePaths = new ArrayList<>();

      for (PartialPath queryPath : queryPaths) {
        String devicePath = queryPath.getDevicePath().getFullPath();
        String databasePath = LbacPermissionChecker.extractDatabasePathFromDevicePath(devicePath);
        if (databasePath != null && !databasePaths.contains(databasePath)) {
          databasePaths.add(databasePath);
        }
      }

      // Use LbacPermissionChecker for centralized LBAC check
      TSStatus lbacStatus = LbacPermissionChecker.checkLbacPermissionForStatement(this, userName);
      if (lbacStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return lbacStatus;
      }
    } catch (Exception e) {
      // Reject access when LBAC check fails with exception
      return new TSStatus(TSStatusCode.NO_PERMISSION.getStatusCode())
          .setMessage("LBAC permission check failed: " + e.getMessage());
    }

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
