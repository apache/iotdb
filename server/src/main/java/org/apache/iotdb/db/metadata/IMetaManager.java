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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 *
 * <p>Since there are too many interfaces and methods in this class, we use code region to help
 * manage code. The code region starts with //region and ends with //endregion. When using Intellij
 * Idea to develop, it's easy to fold the code region and see code region overview by collapsing
 * all.
 *
 * <p>The codes are divided into the following code regions:
 *
 * <ol>
 *   <li>MManager Singleton
 *   <li>Interfaces and Implementation of MManager initialization、snapshot、recover and clear
 *   <li>Interfaces for CQ
 *   <li>Interfaces and Implementation for Timeseries operation
 *   <li>Interfaces and Implementation for StorageGroup and TTL operation
 *   <li>Interfaces for get and auto create device
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
 *   <li>Interfaces only for Cluster module usage
 *   <li>Interfaces for lastCache operations
 *   <li>Interfaces and Implementation for InsertPlan process
 *   <li>Interfaces and Implementation for Template operations
 *   <li>TestOnly Interfaces
 * </ol>
 */
public interface IMetaManager {

  // region Interfaces and Implementation of MManager initialization、snapshot、recover and clear
  void init();

  void clear();

  void operation(PhysicalPlan plan) throws IOException, MetadataException;
  // endregion

  // region Interfaces for CQ
  void createContinuousQuery(CreateContinuousQueryPlan plan) throws MetadataException;

  void dropContinuousQuery(DropContinuousQueryPlan plan) throws MetadataException;

  void writeCreateContinuousQueryLog(CreateContinuousQueryPlan plan) throws IOException;

  void writeDropContinuousQueryLog(DropContinuousQueryPlan plan) throws IOException;
  // endregion

  // region Interfaces and Implementation for Timeseries operation
  // including create and delete
  void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException;

  void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException;

  void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws MetadataException;

  void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException;

  String deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  String deleteTimeseries(PartialPath pathPattern) throws MetadataException;
  // endregion

  // region Interfaces and Implementation for StorageGroup and TTL operation
  // including sg set and delete, and ttl set
  void setStorageGroup(PartialPath storageGroup) throws MetadataException;

  void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException;

  void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException;
  // endregion

  // region Interfaces for metadata info Query
  boolean isPathExist(PartialPath path) throws MetadataException;

  String getMetadataInString();

  long getTotalSeriesNumber();

  int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  int getAllTimeseriesCount(PartialPath pathPattern) throws MetadataException;

  int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  int getDevicesNum(PartialPath pathPattern) throws MetadataException;

  int getStorageGroupNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException;

  int getNodesCountInGivenLevel(PartialPath pathPattern, int level) throws MetadataException;

  Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException;
  // endregion

  // region Interfaces for level Node info Query
  List<PartialPath> getNodesListInGivenLevel(PartialPath pathPattern, int nodeLevel)
      throws MetadataException;

  List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, SchemaEngine.StorageGroupFilter filter)
      throws MetadataException;

  Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException;

  Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException;
  // endregion

  // region Interfaces for StorageGroup and TTL info Query
  boolean isStorageGroup(PartialPath path) throws MetadataException;

  boolean checkStorageGroupByPath(PartialPath path) throws MetadataException;

  PartialPath getBelongedStorageGroup(PartialPath path)
      throws StorageGroupNotSetException, IllegalPathException;

  List<PartialPath> getBelongedStorageGroups(PartialPath pathPattern) throws MetadataException;

  List<PartialPath> getMatchedStorageGroups(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<PartialPath> getAllStorageGroupPaths();

  Map<PartialPath, Long> getStorageGroupsTTL();
  // endregion

  // region Interfaces for Entity/Device info Query
  Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException;

  Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<ShowDevicesResult> getMatchedDevices(ShowDevicesPlan plan) throws MetadataException;
  // endregion

  // region Interfaces for timeseries, measurement and schema info Query
  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern) throws MetadataException;

  Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException;

  List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException;

  TSDataType getSeriesType(PartialPath fullPath) throws MetadataException;

  IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException;

  // attention: this path must be a device node
  List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException;

  IStorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException;

  IStorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException;

  List<IStorageGroupMNode> getAllStorageGroupNodes();

  IMNode getDeviceNode(PartialPath path) throws MetadataException;

  IMeasurementMNode[] getMeasurementMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException;

  IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException;
  // endregion

  // region Interfaces for alias and tag/attribute operations
  /**
   * upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException;

  void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException;

  void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException;

  void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException;

  void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException;

  void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException;

  void changeAlias(PartialPath path, String hello) throws MetadataException, IOException;
  // endregion

  // region Interfaces only for Cluster module usage
  void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas);

  void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas);

  Map<String, List<PartialPath>> groupPathByStorageGroup(PartialPath path) throws MetadataException;
  // end region

  void cacheMeta(PartialPath path, IMeasurementMNode measurementMNode, boolean needSetFullPath);

  // region Interfaces for lastCache operations
  void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime);

  void updateLastCache(
      IMeasurementMNode node,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime);

  TimeValuePair getLastCache(PartialPath seriesPath);

  TimeValuePair getLastCache(IMeasurementMNode node);

  void resetLastCache(PartialPath seriesPath);

  void deleteLastCacheByDevice(PartialPath deviceId) throws MetadataException;

  void deleteLastCacheByDevice(
      PartialPath deviceId, PartialPath originalPath, long startTime, long endTime)
      throws MetadataException;
  // endregion

  // region Interfaces and Implementation for InsertPlan process
  IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan) throws MetadataException, IOException;
  // endregion

  // region Interfaces and Implementation for Template operations
  void createSchemaTemplate(CreateTemplatePlan plan) throws MetadataException;

  void appendSchemaTemplate(AppendTemplatePlan plan) throws MetadataException;

  void pruneSchemaTemplate(PruneTemplatePlan plan) throws MetadataException;

  int countMeasurementsInTemplate(String templateName) throws MetadataException;

  boolean isMeasurementInTemplate(String templateName, String path) throws MetadataException;

  boolean isPathExistsInTemplate(String templateName, String path) throws MetadataException;

  List<String> getMeasurementsInTemplate(String templateName, String path) throws MetadataException;

  List<Pair<String, IMeasurementSchema>> getSchemasInTemplate(String templateName, String path)
      throws MetadataException;

  Set<String> getAllTemplates();

  Set<String> getPathsSetTemplate(String templateName) throws MetadataException;

  Set<String> getPathsUsingTemplate(String templateName) throws MetadataException;

  void dropSchemaTemplate(DropTemplatePlan plan) throws MetadataException;

  void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException;

  void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException;

  void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException;
  // endregion

  // region test only interfaces
  String getDeviceId(PartialPath devicePath);

  Template getTemplate(String templateName) throws MetadataException;

  // endregion
}
