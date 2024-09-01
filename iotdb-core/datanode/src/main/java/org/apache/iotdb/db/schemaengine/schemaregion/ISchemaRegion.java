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

package org.apache.iotdb.db.schemaengine.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.metadata.SchemaQuotaExceededException;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.metric.ISchemaRegionMetric;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowDevicesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowNodesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IActivateTemplateInClusterPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.ICreateTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.IRollbackPreDeactivateTemplatePlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.IAlterLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.write.req.view.ICreateLogicalViewPlan;
import org.apache.iotdb.db.schemaengine.template.Template;

import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface defines all interfaces and behaviours that one SchemaRegion should support and
 * implement.
 *
 * <p>The interfaces are divided as following:
 *
 * <ol>
 *   <li>Interfaces for initialization、recover and clear
 *   <li>Interfaces for schema region Info query and operation
 *   <li>Interfaces for Timeseries operation
 *   <li>Interfaces for metadata info Query
 *       <ol>
 *         <li>Interfaces for Entity/Device info Query
 *         <li>Interfaces for timeseries, measurement and schema info Query
 *       </ol>
 *   <li>Interfaces for alias and tag/attribute operations
 *   <li>Interfaces for Template operations
 * </ol>
 */
public interface ISchemaRegion {

  // region Interfaces for initialization、recover and clear
  void init() throws MetadataException;

  /** clear all metadata components of this schemaRegion */
  void clear();

  void forceMlog();

  @TestOnly
  ISchemaRegionStatistics getSchemaRegionStatistics();

  ISchemaRegionMetric getSchemaRegionMetric();

  // endregion

  // region Interfaces for schema region Info query and operation
  SchemaRegionId getSchemaRegionId();

  String getDatabaseFullPath();

  // delete this schemaRegion and clear all resources
  void deleteSchemaRegion() throws MetadataException;

  boolean createSnapshot(File snapshotDir);

  void loadSnapshot(File latestSnapshotRootDir);

  // endregion

  // region Interfaces for Timeseries operation
  /**
   * Create timeseries.
   *
   * @param plan a plan describes how to create the timeseries.
   * @param offset
   * @throws MetadataException
   */
  void createTimeSeries(ICreateTimeSeriesPlan plan, long offset) throws MetadataException;

  /**
   * Create aligned timeseries.
   *
   * @param plan a plan describes how to create the timeseries.
   * @throws MetadataException
   */
  void createAlignedTimeSeries(ICreateAlignedTimeSeriesPlan plan) throws MetadataException;

  /**
   * Check whether measurement exists.
   *
   * @param devicePath the path of device that you want to check
   * @param measurementList a list of measurements that you want to check
   * @param aliasList a list of alias that you want to check
   * @return returns a map contains index of the measurements or alias that threw the exception, and
   *     exception details. The exceptions describe whether the measurement or alias exists. For
   *     example, a MeasurementAlreadyExistException means this measurement exists.
   */
  Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList);

  /**
   * Check whether time series can be created.
   *
   * @param devicePath the path of device that you want to check
   * @param timeSeriesNum the number of time series that you want to check
   * @throws SchemaQuotaExceededException if the number of time series or devices exceeds the limit
   */
  void checkSchemaQuota(PartialPath devicePath, int timeSeriesNum)
      throws SchemaQuotaExceededException;

  /**
   * Construct schema black list via setting matched time series to preDeleted.
   *
   * @param patternTree the patterns to construct black list
   * @throws MetadataException If write to mLog failed
   * @return {@link Pair}{@literal <}preDeletedNum, isAllLogicalView{@literal >}. If there are
   *     intersections of patterns in the patternTree, there may be more than are actually
   *     pre-deleted.
   */
  Pair<Long, Boolean> constructSchemaBlackList(final PathPatternTree patternTree)
      throws MetadataException;

  /**
   * Rollback schema black list via setting matched timeseries to not pre deleted.
   *
   * @param patternTree
   * @throws MetadataException
   */
  void rollbackSchemaBlackList(PathPatternTree patternTree) throws MetadataException;

  /**
   * Fetch schema black list (timeseries that has been pre deleted).
   *
   * @param patternTree
   * @throws MetadataException
   */
  Set<PartialPath> fetchSchemaBlackList(PathPatternTree patternTree) throws MetadataException;

  /**
   * Delete timeseries in schema black list.
   *
   * @param patternTree
   * @throws MetadataException
   */
  void deleteTimeseriesInBlackList(PathPatternTree patternTree) throws MetadataException;

  // endregion

  // region Interfaces for Logical View
  void createLogicalView(ICreateLogicalViewPlan createLogicalViewPlan) throws MetadataException;

  long constructLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException;

  void rollbackLogicalViewBlackList(PathPatternTree patternTree) throws MetadataException;

  void deleteLogicalView(PathPatternTree patternTree) throws MetadataException;

  void alterLogicalView(IAlterLogicalViewPlan alterLogicalViewPlan) throws MetadataException;

  // endregion

  // region Interfaces for metadata info Query

  // region Interfaces for timeSeries, measurement and schema info Query

  MeasurementPath fetchMeasurementPath(PartialPath fullPath) throws MetadataException;

  ClusterSchemaTree fetchSeriesSchema(
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      boolean withTags,
      boolean withAttributes,
      boolean withTemplate,
      boolean withAliasForce)
      throws MetadataException;

  /**
   * Fetch all the schema by the given patternTree in device level. Currently, devices with
   * isAligned!=null will be filtered out.
   *
   * @param patternTree devices' pattern
   * @param authorityScope the scope of the authority
   * @return pattern tree with all leaves as device nodes
   */
  ClusterSchemaTree fetchDeviceSchema(PathPatternTree patternTree, PathPatternTree authorityScope)
      throws MetadataException;

  // endregion
  // endregion

  // region Interfaces for alias and tag/attribute operations

  /**
   * Upsert alias, tags and attributes key-value for the timeseries if the key has existed, just use
   * the new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  void upsertAliasAndTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or attributes already exist
   */
  void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags already exist
   */
  void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Drop tags or attributes of the timeseries. It will not throw exception even if the key does not
   * exist.
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tags/attributes do not exist
   */
  void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException;

  /**
   * Rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or does not have tag/attribute or already has
   *     a tag/attribute named newKey
   */
  void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException;

  // endregion

  // region Interfaces for Template operations
  void activateSchemaTemplate(IActivateTemplateInClusterPlan plan, Template template)
      throws MetadataException;

  long constructSchemaBlackListWithTemplate(IPreDeactivateTemplatePlan plan)
      throws MetadataException;

  void rollbackSchemaBlackListWithTemplate(IRollbackPreDeactivateTemplatePlan plan)
      throws MetadataException;

  void deactivateTemplateInBlackList(IDeactivateTemplatePlan plan) throws MetadataException;

  long countPathsUsingTemplate(int templateId, PathPatternTree patternTree)
      throws MetadataException;

  // endregion

  // region table device management

  void createOrUpdateTableDevice(final CreateOrUpdateTableDeviceNode createOrUpdateTableDeviceNode)
      throws MetadataException;

  void updateTableDeviceAttribute(final TableDeviceAttributeUpdateNode updateNode)
      throws MetadataException;

  void deleteTableDevice(String table) throws MetadataException;

  // endregion

  // region Interfaces for SchemaReader

  ISchemaReader<IDeviceSchemaInfo> getDeviceReader(IShowDevicesPlan showDevicesPlan)
      throws MetadataException;

  ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReader(IShowTimeSeriesPlan showTimeSeriesPlan)
      throws MetadataException;

  ISchemaReader<INodeSchemaInfo> getNodeReader(IShowNodesPlan showNodesPlan)
      throws MetadataException;

  ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(final PartialPath pathPattern)
      throws MetadataException;

  ISchemaReader<IDeviceSchemaInfo> getTableDeviceReader(
      final String table, final List<Object[]> devicePathList) throws MetadataException;
  // endregion

  // endregion
}
