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
package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
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
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ISchemaRegion {

  // Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  void init(IStorageGroupMNode storageGroupMNode) throws MetadataException;

  void clear();

  void forceMlog();

  // this method is mainly used for recover and metadata sync
  void operation(PhysicalPlan plan) throws IOException, MetadataException;

  void deleteSchemaRegion() throws MetadataException;

  void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException;

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException;

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

  Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  Pair<Integer, Set<String>> deleteTimeseries(PartialPath pathPattern) throws MetadataException;

  IMNode getDeviceNodeWithAutoCreate(PartialPath path, boolean autoCreateSchema)
      throws IOException, MetadataException;

  IMNode getDeviceNodeWithAutoCreate(PartialPath path) throws MetadataException, IOException;

  void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException;

  boolean isPathExist(PartialPath path) throws MetadataException;

  int getAllTimeseriesCount(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  int getDevicesNum(PartialPath pathPattern, boolean isPrefixMatch) throws MetadataException;

  int getDevicesNum(PartialPath pathPattern) throws MetadataException;

  int getNodesCountInGivenLevel(PartialPath pathPattern, int level, boolean isPrefixMatch)
      throws MetadataException;

  Map<PartialPath, Integer> getMeasurementCountGroupByLevel(
      PartialPath pathPattern, int level, boolean isPrefixMatch) throws MetadataException;

  // region Interfaces for level Node info Query
  List<PartialPath> getNodesListInGivenLevel(
      PartialPath pathPattern, int nodeLevel, LocalSchemaProcessor.StorageGroupFilter filter)
      throws MetadataException;

  Set<String> getChildNodePathInNextLevel(PartialPath pathPattern) throws MetadataException;

  Set<String> getChildNodeNameInNextLevel(PartialPath pathPattern) throws MetadataException;

  Set<PartialPath> getBelongedDevices(PartialPath timeseries) throws MetadataException;

  Set<PartialPath> getMatchedDevices(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  Pair<List<ShowDevicesResult>, Integer> getMatchedDevices(ShowDevicesPlan plan)
      throws MetadataException;

  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern, boolean isPrefixMatch)
      throws MetadataException;

  List<MeasurementPath> getMeasurementPaths(PartialPath pathPattern) throws MetadataException;

  Pair<List<MeasurementPath>, Integer> getMeasurementPathsWithAlias(
      PartialPath pathPattern, int limit, int offset, boolean isPrefixMatch)
      throws MetadataException;

  Pair<List<ShowTimeSeriesResult>, Integer> showTimeseries(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException;

  TSDataType getSeriesType(PartialPath fullPath) throws MetadataException;

  IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException;

  // attention: this path must be a device node
  List<MeasurementPath> getAllMeasurementByDevicePath(PartialPath devicePath)
      throws PathNotExistException;

  // region Interfaces and methods for MNode query
  IMNode getDeviceNode(PartialPath path) throws MetadataException;

  IMeasurementMNode[] getMeasurementMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException;

  IMeasurementMNode getMeasurementMNode(PartialPath fullPath) throws MetadataException;

  void changeAlias(PartialPath path, String alias) throws MetadataException, IOException;

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
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

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException;

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException;

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException;

  void collectMeasurementSchema(
      PartialPath prefixPath, List<IMeasurementSchema> measurementSchemas);

  void collectTimeseriesSchema(
      PartialPath prefixPath, Collection<TimeseriesSchema> timeseriesSchemas);

  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  IMNode getSeriesSchemasAndReadLockDevice(InsertPlan plan) throws MetadataException, IOException;

  Set<String> getPathsSetTemplate(String templateName) throws MetadataException;

  Set<String> getPathsUsingTemplate(String templateName) throws MetadataException;

  boolean isTemplateAppendable(Template template, List<String> measurements)
      throws MetadataException;

  void setSchemaTemplate(SetTemplatePlan plan) throws MetadataException;

  void unsetSchemaTemplate(UnsetTemplatePlan plan) throws MetadataException;

  void setUsingSchemaTemplate(ActivateTemplatePlan plan) throws MetadataException;

  IMNode setUsingSchemaTemplate(IMNode node) throws MetadataException;
}
