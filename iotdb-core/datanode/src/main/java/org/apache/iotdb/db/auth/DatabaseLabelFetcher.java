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

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseSecurityLabelReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseSecurityLabelResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Database label fetcher for retrieving security labels from databases. Supports caching for
 * performance optimization.
 */
public class DatabaseLabelFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseLabelFetcher.class);

  // Cache for database security labels to improve performance
  private static final Map<String, SecurityLabel> LABEL_CACHE = new ConcurrentHashMap<>();

  // Cache TTL in milliseconds (default: 5 minutes)
  private static final long CACHE_TTL_MS = 5 * 60 * 1000;

  // Cache entry with timestamp for TTL management
  private static class CachedLabel {
    private final SecurityLabel label;
    private final long timestamp;

    public CachedLabel(SecurityLabel label) {
      this.label = label;
      this.timestamp = System.currentTimeMillis();
    }

    public SecurityLabel getLabel() {
      return label;
    }

    public boolean isExpired() {
      return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
    }
  }

  private static final Map<String, CachedLabel> CACHED_LABELS = new ConcurrentHashMap<>();

  /** Private constructor - utility class should not be instantiated */
  private DatabaseLabelFetcher() {
    // empty constructor
  }

  /**
   * Get security label for a database from the given database path
   *
   * @param databasePath the database path (e.g., "root.db1")
   * @return SecurityLabel if found, null if database has no security label
   * @throws MetadataException if error occurs while fetching labels
   */
  public static SecurityLabel getDatabaseSecurityLabel(String databasePath)
      throws MetadataException {
    if (databasePath == null || databasePath.trim().isEmpty()) {
      LOGGER.debug("Database path is null or empty, returning null security label");
      return null;
    }

    String normalizedPath = normalizeDatabasePath(databasePath);
    LOGGER.debug("Fetching security label for database: {}", normalizedPath);

    // Check cache first
    CachedLabel cachedLabel = CACHED_LABELS.get(normalizedPath);
    if (cachedLabel != null && !cachedLabel.isExpired()) {
      LOGGER.debug("Cache hit for database: {}", normalizedPath);
      return cachedLabel.getLabel();
    }

    try {
      // Fetch from metadata store
      SecurityLabel label = fetchDatabaseLabelFromMetadata(normalizedPath);

      // Update cache
      if (label != null) {
        CACHED_LABELS.put(normalizedPath, new CachedLabel(label));
        LOGGER.debug("Cached security label for database: {}", normalizedPath);
      } else {
        // Cache null result as well to avoid repeated queries
        CACHED_LABELS.put(normalizedPath, new CachedLabel(null));
      }

      return label;
    } catch (Exception e) {
      LOGGER.error("Error fetching security label for database: {}", normalizedPath, e);
      throw new MetadataException(
          "Failed to fetch security label for database: " + normalizedPath, e);
    }
  }

  /**
   * Extract database path from a device/timeseries path
   *
   * @param devicePath full device or timeseries path (e.g., "root.db1.device1.sensor1")
   * @return database path (e.g., "root.db1")
   * @throws MetadataException if the path is invalid or database cannot be determined
   */
  public static String extractDatabasePath(String devicePath) throws MetadataException {
    if (devicePath == null || devicePath.trim().isEmpty()) {
      throw new MetadataException("Device path cannot be null or empty");
    }

    try {
      // Parse device path to extract database
      // IoTDB path format: root.database.device.measurement...
      String[] pathParts = devicePath.split("\\.");

      if (pathParts.length < 2 || !pathParts[0].equals("root")) {
        LOGGER.debug("Invalid path format for database extraction: {}", devicePath);
        return null;
      }

      // Assume database is the second level after root
      // e.g., root.db1.device1.sensor1 -> root.db1
      if (pathParts.length >= 2) {
        String databasePath = pathParts[0] + "." + pathParts[1];
        LOGGER.debug("Extracted database path: {} from device path: {}", databasePath, devicePath);
        return databasePath;
      }

      LOGGER.debug("No database found for path: {}", devicePath);
      return null;
    } catch (Exception e) {
      LOGGER.error("Error extracting database path from device path: {}", devicePath, e);
      throw new MetadataException("Failed to extract database path from: " + devicePath, e);
    }
  }

  /**
   * Get security label for a device/timeseries path
   *
   * @param devicePath full device or timeseries path
   * @return SecurityLabel if the database has security labels, null otherwise
   * @throws MetadataException if error occurs while processing
   */
  public static SecurityLabel getSecurityLabelForPath(String devicePath) throws MetadataException {
    String databasePath = extractDatabasePath(devicePath);
    if (databasePath == null) {
      LOGGER.debug("No database found for path: {}, returning null security label", devicePath);
      return null;
    }

    return getDatabaseSecurityLabel(databasePath);
  }

  /**
   * Invalidate cache for a specific database
   *
   * @param databasePath the database path to invalidate
   */
  public static void invalidateCache(String databasePath) {
    if (databasePath != null) {
      String normalizedPath = normalizeDatabasePath(databasePath);
      CACHED_LABELS.remove(normalizedPath);
      LOGGER.debug("Invalidated cache for database: {}", normalizedPath);
    }
  }

  /** Clear all cached labels */
  public static void clearCache() {
    CACHED_LABELS.clear();
    LOGGER.debug("Cleared all cached database security labels");
  }

  /**
   * Get current cache size (for monitoring/debugging)
   *
   * @return number of cached entries
   */
  public static int getCacheSize() {
    return CACHED_LABELS.size();
  }

  /**
   * Normalize database path to ensure consistent cache keys
   *
   * @param databasePath raw database path
   * @return normalized database path
   */
  private static String normalizeDatabasePath(String databasePath) {
    if (databasePath == null) {
      return null;
    }

    String trimmed = databasePath.trim();

    // Ensure it starts with "root."
    if (!trimmed.startsWith("root.")) {
      if (trimmed.equals("root")) {
        return "root";
      } else {
        return "root." + trimmed;
      }
    }

    return trimmed;
  }

  /**
   * Fetch database security label from metadata store This method should be implemented to
   * interface with the actual metadata storage
   *
   * @param databasePath normalized database path
   * @return SecurityLabel or null if not found
   */
  private static SecurityLabel fetchDatabaseLabelFromMetadata(String databasePath) {
    try {
      // TODO: This should interface with the actual metadata store where database
      // labels are stored
      // For now, we'll try to get it through the SchemaEngine or ConfigNode

      // Try to get from local schema engine first
      SecurityLabel label = getFromLocalSchema(databasePath);
      if (label != null) {
        return label;
      }

      // Fallback to ConfigNode if needed
      return getFromConfigNode(databasePath);

    } catch (Exception e) {
      LOGGER.error("Error fetching database label from metadata for: {}", databasePath, e);
      return null;
    }
  }

  /**
   * Get security label from local schema engine
   *
   * @param databasePath database path
   * @return SecurityLabel or null
   */
  private static SecurityLabel getFromLocalSchema(String databasePath) {
    try {
      LOGGER.debug("Attempting to get security label from local schema for: {}", databasePath);

      // 在IoTDB架构中，数据库的安全标签信息存储在ConfigNode中，
      // 而不是在DataNode的本地schema引擎中。DataNode的schema引擎
      // 主要负责存储时间序列的元数据，而数据库级别的信息（包括安全标签）
      // 由ConfigNode管理。

      // 检查数据库是否存在
      SchemaEngine schemaEngine = SchemaEngine.getInstance();
      if (schemaEngine == null) {
        LOGGER.debug("SchemaEngine not initialized for database: {}", databasePath);
        return null;
      }

      // 检查数据库是否在本地schema引擎中存在
      // 注意：这里只是检查数据库是否存在，实际的安全标签需要从ConfigNode获取
      Collection<ISchemaRegion> schemaRegions = schemaEngine.getAllSchemaRegions();
      if (schemaRegions != null) {
        for (ISchemaRegion schemaRegion : schemaRegions) {
          if (schemaRegion.getDatabaseFullPath().equals(databasePath)) {
            LOGGER.debug(
                "Database {} exists in local schema engine, but security labels are stored in ConfigNode",
                databasePath);
            // 数据库存在，但安全标签需要从ConfigNode获取
            return null;
          }
        }
      }

      LOGGER.debug("Database {} not found in local schema engine", databasePath);
      return null;

    } catch (Exception e) {
      LOGGER.debug("Failed to get security label from local schema for: {}", databasePath, e);
      return null;
    }
  }

  /**
   * Get security label from ConfigNode
   *
   * @param databasePath database path
   * @return SecurityLabel or null
   */
  private static SecurityLabel getFromConfigNode(String databasePath) {
    try {
      LOGGER.debug("Attempting to get security label from ConfigNode for: {}", databasePath);

      // Use ConfigNodeClient to query database security labels
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {

        // Create request for database security label
        TGetDatabaseSecurityLabelReq req = new TGetDatabaseSecurityLabelReq(databasePath);

        // Call ConfigNode to get database security label
        TGetDatabaseSecurityLabelResp resp = configNodeClient.getDatabaseSecurityLabel(req);

        LOGGER.debug(
            "ConfigNode response for database {}: status={}",
            databasePath,
            resp.getStatus().getCode());

        // Check if the request was successful
        if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          Map<String, String> securityLabelMap = resp.getSecurityLabel();

          if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
            // ConfigNode returns format: {"databasePath": "key1:value1,key2:value2"}
            // We need to extract the label string and parse it
            String labelString = securityLabelMap.get(databasePath);
            if (labelString != null && !labelString.trim().isEmpty()) {
              LOGGER.debug("Found security labels for database {}: {}", databasePath, labelString);

              // Parse the label string format: "key1:value1,key2:value2"
              Map<String, String> parsedLabels = new HashMap<>();
              String[] labelPairs = labelString.split(",");
              for (String pair : labelPairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length == 2) {
                  parsedLabels.put(keyValue[0].trim(), keyValue[1].trim());
                }
              }

              if (!parsedLabels.isEmpty()) {
                // Convert Map to SecurityLabel object
                SecurityLabel securityLabel = new SecurityLabel(parsedLabels);
                return securityLabel;
              }
            }
          }

          LOGGER.debug("Database {} has no security labels", databasePath);
          return null;
        } else {
          LOGGER.warn(
              "Failed to get security label from ConfigNode for database {}: {}",
              databasePath,
              resp.getStatus().getMessage());
          return null;
        }

      } catch (ClientManagerException | TException e) {
        LOGGER.error(
            "Error communicating with ConfigNode for database {}: {}",
            databasePath,
            e.getMessage());
        return null;
      }

    } catch (Exception e) {
      LOGGER.error(
          "Unexpected error getting security label from ConfigNode for database {}: {}",
          databasePath,
          e.getMessage());
      return null;
    }
  }

  /** Cleanup expired cache entries This method can be called periodically to maintain cache size */
  public static void cleanupExpiredEntries() {
    CACHED_LABELS.entrySet().removeIf(entry -> entry.getValue().isExpired());
    LOGGER.debug("Cleaned up expired cache entries, current cache size: {}", CACHED_LABELS.size());
  }
}
