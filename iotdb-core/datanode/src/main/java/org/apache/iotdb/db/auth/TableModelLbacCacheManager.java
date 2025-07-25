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
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table Model LBAC Cache Manager for managing user policies and database security labels
 *
 * <p>This class provides caching mechanisms for table model LBAC functionality, reusing the tree
 * model's caching infrastructure while providing table model specific optimizations.
 *
 * <p>Key features: 1. User policy caching - reuses BasicAuthorityCache for user policies 2.
 * Database security label caching - reuses DatabaseLabelFetcher for database labels 3. Path
 * conversion - converts table model paths to tree model paths for cache lookup 4. Cache
 * invalidation - ensures policy changes are immediately reflected
 */
public class TableModelLbacCacheManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableModelLbacCacheManager.class);
  private static final IoTDBDescriptor CONF = IoTDBDescriptor.getInstance();

  // Singleton instance
  private static final TableModelLbacCacheManager INSTANCE = new TableModelLbacCacheManager();

  // Cache TTL for database security labels (5 minutes)
  private static final long DATABASE_LABEL_CACHE_TTL_MS = 5 * 60 * 1000;

  private TableModelLbacCacheManager() {
    // Private constructor for singleton
  }

  public static TableModelLbacCacheManager getInstance() {
    return INSTANCE;
  }

  /**
   * Get user with label policies from cache or fetch from ConfigNode
   *
   * @param username the username to get
   * @return User object with label policies, or null if not found
   */
  public User getUserWithLabelPolicies(String username) {
    if (username == null || username.trim().isEmpty()) {
      LOGGER.debug("Username is null or empty");
      return null;
    }

    try {
      // Get authority fetcher for cache access
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();

      // Try to get user from cache first
      User cachedUser = authorityFetcher.getAuthorCache().getUserCache(username);
      if (cachedUser != null) {
        LOGGER.debug(
            "Found user {} in cache with policies - Read: {}, Write: {}",
            username,
            cachedUser.getReadLabelPolicyExpression(),
            cachedUser.getWriteLabelPolicyExpression());
        return cachedUser;
      }

      LOGGER.debug("User {} not in cache, fetching from ConfigNode", username);

      // Fetch user from ConfigNode using LbacIntegration's method
      User fetchedUser = LbacIntegration.getUserFromConfigNode(username);

      if (fetchedUser != null) {
        // Cache the user for future use
        authorityFetcher.getAuthorCache().putUserCache(username, fetchedUser);
        LOGGER.debug("Successfully fetched and cached user: {} with policies", username);
      } else {
        LOGGER.warn("Failed to fetch user {} from ConfigNode", username);
      }

      return fetchedUser;

    } catch (Exception e) {
      LOGGER.error("Error getting user with label policies: {}", username, e);
      return null;
    }
  }

  /**
   * Get database security label for table model database
   *
   * @param databaseName the database name (e.g., "db1")
   * @return SecurityLabel if found, null if database has no security label
   */
  public SecurityLabel getDatabaseSecurityLabel(String databaseName) {
    if (databaseName == null || databaseName.trim().isEmpty()) {
      LOGGER.debug("Database name is null or empty");
      return null;
    }

    try {
      // Convert table model path to tree model path for cache lookup
      String treeModelPath = convertTableModelPathToTreeModelPath(databaseName);
      LOGGER.debug(
          "Converted table model path '{}' to tree model path '{}'", databaseName, treeModelPath);

      // Use DatabaseLabelFetcher to get security label (reuses tree model cache)
      SecurityLabel label = DatabaseLabelFetcher.getDatabaseSecurityLabel(treeModelPath);

      if (label != null) {
        LOGGER.debug("Found security label for database {}: {}", databaseName, label);
      } else {
        LOGGER.debug("No security label found for database {}", databaseName);
      }

      return label;

    } catch (Exception e) {
      LOGGER.error("Error getting database security label for: {}", databaseName, e);
      return null;
    }
  }

  /**
   * Invalidate user cache for table model LBAC
   *
   * @param username the username whose cache should be invalidated
   */
  public void invalidateUserCache(String username) {
    if (username == null || username.trim().isEmpty()) {
      LOGGER.debug("Username is null or empty for cache invalidation");
      return;
    }

    try {
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      authorityFetcher.getAuthorCache().invalidateCache(username, null);
      LOGGER.info("Invalidated user cache for table model LBAC: {}", username);
    } catch (Exception e) {
      LOGGER.error("Error invalidating user cache for: {}", username, e);
    }
  }

  /**
   * Invalidate database security label cache for table model LBAC
   *
   * @param databaseName the database name whose cache should be invalidated
   */
  public void invalidateDatabaseLabelCache(String databaseName) {
    if (databaseName == null || databaseName.trim().isEmpty()) {
      LOGGER.debug("Database name is null or empty for cache invalidation");
      return;
    }

    try {
      // Convert table model path to tree model path
      String treeModelPath = convertTableModelPathToTreeModelPath(databaseName);

      // Use DatabaseLabelFetcher to invalidate cache
      DatabaseLabelFetcher.invalidateCache(treeModelPath);
      LOGGER.info(
          "Invalidated database label cache for table model LBAC: {} -> {}",
          databaseName,
          treeModelPath);
    } catch (Exception e) {
      LOGGER.error("Error invalidating database label cache for: {}", databaseName, e);
    }
  }

  /** Clear all caches for table model LBAC */
  public void clearAllCaches() {
    try {
      // Clear user cache
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      authorityFetcher.getAuthorCache().invalidAllCache();

      // Clear database label cache
      DatabaseLabelFetcher.clearCache();

      LOGGER.info("Cleared all caches for table model LBAC");
    } catch (Exception e) {
      LOGGER.error("Error clearing all caches for table model LBAC", e);
    }
  }

  /**
   * Convert table model database path to tree model path for cache lookup
   *
   * @param tableModelPath the table model path (e.g., "db1")
   * @return the tree model path (e.g., "root.db1")
   */
  private String convertTableModelPathToTreeModelPath(String tableModelPath) {
    if (tableModelPath == null || tableModelPath.trim().isEmpty()) {
      return null;
    }

    String trimmed = tableModelPath.trim();

    // If already starts with "root.", return as is
    if (trimmed.startsWith("root.")) {
      return trimmed;
    }

    // If equals "root", return as is
    if (trimmed.equals("root")) {
      return trimmed;
    }

    // Otherwise, prepend "root."
    return "root." + trimmed;
  }

  /**
   * Get cache statistics for monitoring
   *
   * @return cache statistics information
   */
  public String getCacheStatistics() {
    try {
      IAuthorityFetcher authorityFetcher = AuthorityChecker.getAuthorityFetcher();
      int databaseLabelCacheSize = DatabaseLabelFetcher.getCacheSize();

      return String.format(
          "Table Model LBAC Cache Statistics - Database Label Cache Size: %d",
          databaseLabelCacheSize);
    } catch (Exception e) {
      LOGGER.error("Error getting cache statistics", e);
      return "Error getting cache statistics: " + e.getMessage();
    }
  }

  /** Cleanup expired cache entries */
  public void cleanupExpiredEntries() {
    try {
      // Cleanup database label cache
      DatabaseLabelFetcher.cleanupExpiredEntries();
      LOGGER.debug("Cleaned up expired cache entries for table model LBAC");
    } catch (Exception e) {
      LOGGER.error("Error cleaning up expired cache entries", e);
    }
  }
}
