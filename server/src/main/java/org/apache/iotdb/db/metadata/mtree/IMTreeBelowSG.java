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
package org.apache.iotdb.db.metadata.mtree;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMTreeBelowSG {
  void clear();

  /**
   * Create MTree snapshot
   *
   * @param snapshotDir specify snapshot directory
   * @return false if failed to create snapshot; true if success
   */
  boolean createSnapshot(File snapshotDir);

  IMeasurementMNode createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException;

  /**
   * Create aligned timeseries with full paths from root to one leaf node. Before creating
   * timeseries, the * database should be set first, throw exception otherwise
   *
   * @param devicePath device path
   * @param measurements measurements list
   * @param dataTypes data types list
   * @param encodings encodings list
   * @param compressors compressor
   */
  List<IMeasurementMNode> createAlignedTimeseries(
      PartialPath devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> aliasList)
      throws MetadataException;

  /**
   * Check if measurements under device exists in MTree
   *
   * @param devicePath device full path
   * @param measurementList measurements list
   * @param aliasList alias of measurement
   * @return If all measurements not exists, return empty map. Otherwise, return a map whose key is
   *     index of measurement in list and value is exception.
   */
  Map<Integer, MetadataException> checkMeasurementExistence(
      PartialPath devicePath, List<String> measurementList, List<String> aliasList);

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  IMeasurementMNode deleteTimeseries(PartialPath path) throws MetadataException;

  boolean isEmptyInternalMNode(IMNode node) throws MetadataException;

  /**
   * Construct schema black list via setting matched timeseries to pre deleted.
   *
   * @param pathPattern path pattern
   * @return PartialPath of timeseries that has been set to pre deleted
   */
  List<PartialPath> constructSchemaBlackList(PartialPath pathPattern) throws MetadataException;

  /**
   * Rollback schema black list via setting matched timeseries to not pre deleted.
   *
   * @param pathPattern path pattern
   * @return PartialPath of timeseries that has been set to not pre deleted
   */
  List<PartialPath> rollbackSchemaBlackList(PartialPath pathPattern) throws MetadataException;

  /**
   * Get all pre-deleted timeseries matched by given pathPattern. For example, given path pattern
   * root.sg.*.s1 and pre-deleted timeseries root.sg.d1.s1, root.sg.d2.s1, then the result set is
   * {root.sg.d1.s1, root.sg.d2.s1}.
   *
   * @param pathPattern path pattern
   * @return all pre-deleted timeseries matched by given pathPattern
   */
  List<PartialPath> getPreDeletedTimeseries(PartialPath pathPattern) throws MetadataException;

  /**
   * Get all devices of pre-deleted timeseries matched by given pathPattern. For example, given path
   * pattern root.sg.*.s1 and pre-deleted timeseries root.sg.d1.s1, root.sg.d2.s1, then the result
   * set is {root.sg.d1, root.sg.d2}.
   *
   * @param pathPattern path pattern
   * @return all devices of pre-deleted timeseries matched by given pathPattern
   */
  Set<PartialPath> getDevicesOfPreDeletedTimeseries(PartialPath pathPattern)
      throws MetadataException;

  void setAlias(IMeasurementMNode measurementMNode, String alias) throws MetadataException;

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  IMNode getDeviceNodeWithAutoCreating(PartialPath deviceId) throws MetadataException;

  /**
   * Fetch all measurement path
   *
   * @param pathPattern a path pattern or a full path, may contain wildcard
   * @param templateMap <TemplateId, Template>
   * @param withTags whether returns all the tags of each timeseries as well.
   * @return schema
   */
  List<MeasurementPath> fetchSchema(
      PartialPath pathPattern, Map<Integer, Template> templateMap, boolean withTags)
      throws MetadataException;

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  IMNode getNodeByPath(PartialPath path) throws MetadataException;

  IMeasurementMNode getMeasurementMNode(PartialPath path) throws MetadataException;

  long countAllMeasurement() throws MetadataException;

  void activateTemplate(PartialPath activatePath, Template template) throws MetadataException;

  /**
   * constructSchemaBlackListWithTemplate
   *
   * @param templateSetInfo PathPattern and templateId to pre-deactivate
   * @return Actual full path and templateId that has been pre-deactivated
   */
  Map<PartialPath, List<Integer>> constructSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException;

  /**
   * rollbackSchemaBlackListWithTemplate
   *
   * @param templateSetInfo PathPattern and templateId to rollback pre-deactivate
   * @return Actual full path and templateId that has been rolled back
   */
  Map<PartialPath, List<Integer>> rollbackSchemaBlackListWithTemplate(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException;

  /**
   * deactivateTemplateInBlackList
   *
   * @param templateSetInfo PathPattern and templateId to rollback deactivate
   * @return Actual full path and templateId that has been deactivated
   */
  Map<PartialPath, List<Integer>> deactivateTemplateInBlackList(
      Map<PartialPath, List<Integer>> templateSetInfo) throws MetadataException;

  long countPathsUsingTemplate(PartialPath pathPattern, int templateId) throws MetadataException;
}
