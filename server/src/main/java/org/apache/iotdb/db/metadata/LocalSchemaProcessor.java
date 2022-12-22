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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.MeasurementAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.SchemaRegionPlanFactory;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>SchemaProcessor Singleton
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces and Implementation for StorageGroup and TTL operation
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for metadata count
 *         <li>Interfaces for level Node info Query
 *         <li>Interfaces for StorageGroup and TTL info Query
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces and methods for MNode query
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>TestOnly Interfaces
 * </ol>
 */
@SuppressWarnings("java:S1135") // ignore todos
// Now it's only used for test and isn't able to recover after restarting
@Deprecated
public class LocalSchemaProcessor {

  private static final Logger logger = LoggerFactory.getLogger(LocalSchemaProcessor.class);

  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final LocalConfigNode configManager = LocalConfigNode.getInstance();
  private final SchemaEngine schemaEngine = SchemaEngine.getInstance();

  // region SchemaProcessor Singleton
  private static class LocalSchemaProcessorHolder {

    private LocalSchemaProcessorHolder() {
      // allowed to do nothing
    }

    private static final LocalSchemaProcessor INSTANCE = new LocalSchemaProcessor();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static LocalSchemaProcessor getInstance() {
    return LocalSchemaProcessorHolder.INSTANCE;
  }

  protected LocalSchemaProcessor() {}
  // endregion

  // region methods in this region is only used for local schemaRegion management.

  // This interface involves database auto creation
  private ISchemaRegion getBelongedSchemaRegionWithAutoCreate(PartialPath path)
      throws MetadataException {
    return schemaEngine.getSchemaRegion(
        configManager.getBelongedSchemaRegionIdWithAutoCreate(path));
  }

  /**
   * Get the target SchemaRegion, which will be involved/covered by the given pathPattern. The path
   * may contain wildcards, * or **. This method is the first step when there's a task on multiple
   * paths represented by the given pathPattern. If isPrefixMatch, all databases under the
   * prefixPath that matches the given pathPattern will be collected.
   */
  private List<ISchemaRegion> getInvolvedSchemaRegions(
      PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException {
    List<SchemaRegionId> schemaRegionIds =
        configManager.getInvolvedSchemaRegionIds(pathPattern, isPrefixMatch);
    List<ISchemaRegion> schemaRegions = new ArrayList<>();
    for (SchemaRegionId schemaRegionId : schemaRegionIds) {
      schemaRegions.add(schemaEngine.getSchemaRegion(schemaRegionId));
    }
    return schemaRegions;
  }
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    try {
      getBelongedSchemaRegionWithAutoCreate(path)
          .createTimeseries(
              SchemaRegionPlanFactory.getCreateTimeSeriesPlan(
                  path, dataType, encoding, compressor, props, null, null, null),
              -1);
    } catch (PathAlreadyExistException
        | AliasAlreadyExistException
        | MeasurementAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore PathAlreadyExistException and AliasAlreadyExistException when Concurrent inserting"
                + " a non-exist time series {}",
            path);
      }
    }
  }

  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException {
    getBelongedSchemaRegionWithAutoCreate(prefixPath)
        .createAlignedTimeSeries(
            SchemaRegionPlanFactory.getCreateAlignedTimeSeriesPlan(
                prefixPath, measurements, dataTypes, encodings, compressors, null, null, null));
  }

  /**
   * Delete all timeseries matching the given path pattern, may cross different database
   *
   * @param pathPattern path to be deleted
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath pathPattern) throws MetadataException {
    List<ISchemaRegion> schemaRegions = getInvolvedSchemaRegions(pathPattern, false);
    if (schemaRegions.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }
    Set<String> failedNames = new HashSet<>();
    int deletedNum = 0;
    Pair<Integer, Set<String>> sgDeletionResult;
    for (ISchemaRegion schemaRegion : schemaRegions) {
      sgDeletionResult = schemaRegion.deleteTimeseries(pathPattern, false);
      deletedNum += sgDeletionResult.left;
      failedNames.addAll(sgDeletionResult.right);
    }

    if (deletedNum == 0 && failedNames.isEmpty()) {
      // In the cluster mode, the deletion of a timeseries will be forwarded to all the nodes. For
      // nodes that do not have the metadata of the timeseries, the coordinator expects a
      // PathNotExistException.
      throw new PathNotExistException(pathPattern.getFullPath());
    }

    return failedNames.isEmpty() ? null : String.join(",", failedNames);
  }
  // endregion

  // region Interfaces and Implementation for StorageGroup and TTL operation
  // including sg set and delete, and ttl set

  /**
   * CREATE DATABASE of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    configManager.setStorageGroup(storageGroup);
  }
  // endregion

  // region Interfaces for metadata info Query

  // region Interfaces for timeseries, measurement and schema info Query
  /**
   * Similar to method getMeasurementPaths(), but return Path with alias and filter the result by
   * limit and offset. If using prefix match, the path pattern is used to match prefix path. All
   * timeseries start with the matched prefix path will be collected.
   *
   * @param isPrefixMatch if true, the path pattern is used to match prefix path
   */
  public Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch, boolean withTags)
      throws MetadataException {
    List<MeasurementPath> measurementPaths = new LinkedList<>();
    Pair<List<MeasurementPath>, Integer> result;
    int resultOffset = 0;

    int tmpLimit = limit;
    int tmpOffset = offset;

    for (ISchemaRegion schemaRegion : getInvolvedSchemaRegions(pathPattern, isPrefixMatch)) {
      if (limit != 0 && tmpLimit == 0) {
        break;
      }
      result =
          schemaRegion.getMeasurementPathsWithAlias(
              pathPattern, tmpLimit, tmpOffset, isPrefixMatch, withTags);
      measurementPaths.addAll(result.left);
      resultOffset += result.right;
      if (limit != 0) {
        tmpOffset = Math.max(0, tmpOffset - result.right);
        tmpLimit -= result.left.size();
      }
    }

    result = new Pair<>(measurementPaths, resultOffset);
    return result;
  }
  // endregion
  // endregion
}
