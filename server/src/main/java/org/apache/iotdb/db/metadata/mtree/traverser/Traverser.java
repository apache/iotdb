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
package org.apache.iotdb.db.metadata.mtree.traverser;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_MULTI_LEVEL_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_ONE_LEVEL_WILDCARD;

// This class defines the main traversal framework and declares some methods for result process
// extension.
public abstract class Traverser {

  protected IMNode startNode;
  protected String[] nodes;

  // if isMeasurementTraverser, measurement in template should be processed
  protected boolean isMeasurementTraverser = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

  // level query option
  protected boolean isLevelTraverser = false;
  protected int targetLevel;

  // traverse for specific storage group
  protected StorageGroupFilter storageGroupFilter = null;

  public Traverser(IMNode startNode, PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(startNode.getName())) {
      throw new IllegalPathException(
          path.getFullPath(), path.getFullPath() + " doesn't start with " + startNode.getName());
    }
    this.startNode = startNode;
    this.nodes = nodes;
  }

  public void traverse() throws MetadataException {
    traverse(startNode, 0, false, 0);
  }

  /**
   * The recursive method for MTree traversal.
   *
   * @param node current node that match the targetName in given path
   * @param idx the index of targetName in given path
   * @param multiLevelWildcard whether the current targetName is **
   * @param level the level of current node in MTree
   * @throws MetadataException some result process may throw MetadataException
   */
  protected void traverse(IMNode node, int idx, boolean multiLevelWildcard, int level)
      throws MetadataException {

    if (storageGroupFilter != null
        && node.isStorageGroup()
        && !storageGroupFilter.satisfy(node.getFullPath())) {
      return;
    }

    if (isLevelTraverser && level > targetLevel) {
      return;
    }

    if (idx >= nodes.length - 1) {
      if (isValid(node)) {
        if (isLevelTraverser) {
          if (targetLevel == level) {
            processValidNode(node, idx);
            return;
          }
        } else {
          processValidNode(node, idx);
        }
      }

      if (!multiLevelWildcard && !isPrefixMatch) {
        return;
      }

      processMultiLevelWildcard(node, idx, level);

      return;
    }

    if (isValid(node) || node.isMeasurement()) {
      if (processInternalValid(node, idx) || node.isMeasurement()) {
        return;
      }
    }

    String targetName = nodes[idx + 1];
    if (PATH_MULTI_LEVEL_WILDCARD.equals(targetName)) {
      processMultiLevelWildcard(node, idx, level);
    } else if (targetName.contains(PATH_ONE_LEVEL_WILDCARD)) {
      processOneLevelWildcard(node, idx, multiLevelWildcard, level);
    } else {
      processNameMatch(node, idx, multiLevelWildcard, level);
    }
  }

  /**
   * Check whether the node is desired target. Different case could have different implementation.
   *
   * @param node current node
   * @return whether the node is desired target
   */
  protected abstract boolean isValid(IMNode node);

  /**
   * Process the desired node to get result. Different case could have different implementation.
   *
   * @param node current node
   * @param idx the index of targetName that current node matches in given path
   */
  protected abstract void processValidNode(IMNode node, int idx) throws MetadataException;

  /**
   * Process the desired internal node to get result. The return boolean value means whether the
   * recursion should return. For example, suppose the case to collect related storage group for
   * timeseries. After process the storageGroupMNode, there's no need to process the nodes in
   * subtree, thus return tree. Different case could have different implementation.
   *
   * @param node current node
   * @param idx the index of targetName that current node matches in given path
   * @return whether the recursion should return.
   */
  protected abstract boolean processInternalValid(IMNode node, int idx) throws MetadataException;

  protected void processMultiLevelWildcard(IMNode node, int idx, int level)
      throws MetadataException {
    for (IMNode child : node.getChildren().values()) {
      traverse(child, idx + 1, true, level + 1);
    }

    if (!isMeasurementTraverser || !node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
      traverse(
          new MeasurementMNode(node, schema.getMeasurementId(), schema, null),
          idx + 1,
          true,
          level + 1);
    }
  }

  protected void processOneLevelWildcard(
      IMNode node, int idx, boolean multiLevelWildcard, int level) throws MetadataException {
    String targetNameRegex = nodes[idx + 1].replace("*", ".*");
    for (IMNode child : node.getChildren().values()) {
      if (!Pattern.matches(targetNameRegex, child.getName())) {
        continue;
      }
      traverse(child, idx + 1, false, level + 1);
    }
    if (multiLevelWildcard) {
      for (IMNode child : node.getChildren().values()) {
        traverse(child, idx, true, level + 1);
      }
    }

    if (!isMeasurementTraverser || !node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
      if (!Pattern.matches(targetNameRegex, schema.getMeasurementId())) {
        continue;
      }
      traverse(
          new MeasurementMNode(node, schema.getMeasurementId(), schema, null),
          idx + 1,
          false,
          level + 1);
    }
    if (multiLevelWildcard) {
      for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
        traverse(
            new MeasurementMNode(node, schema.getMeasurementId(), schema, null),
            idx,
            true,
            level + 1);
      }
    }
  }

  protected void processNameMatch(IMNode node, int idx, boolean multiLevelWildcard, int level)
      throws MetadataException {
    String targetName = nodes[idx + 1];
    IMNode next = node.getChild(targetName);
    if (next != null) {
      traverse(next, idx + 1, false, level + 1);
    }
    if (multiLevelWildcard) {
      for (IMNode child : node.getChildren().values()) {
        traverse(child, idx, true, level + 1);
      }
    }

    if (!isMeasurementTraverser || !node.isUseTemplate()) {
      return;
    }

    Template upperTemplate = node.getUpperTemplate();
    IMeasurementSchema targetSchema = upperTemplate.getSchemaMap().get(targetName);
    if (targetSchema != null) {
      traverse(
          new MeasurementMNode(node, targetSchema.getMeasurementId(), targetSchema, null),
          idx + 1,
          false,
          level + 1);
    }

    if (multiLevelWildcard) {
      for (IMeasurementSchema schema : upperTemplate.getSchemaMap().values()) {
        traverse(
            new MeasurementMNode(node, schema.getMeasurementId(), schema, null),
            idx,
            true,
            level + 1);
      }
    }
  }

  public void setTargetLevel(int targetLevel) {
    this.targetLevel = targetLevel;
    if (targetLevel > 0) {
      isLevelTraverser = true;
    }
  }

  public void setStorageGroupFilter(StorageGroupFilter storageGroupFilter) {
    this.storageGroupFilter = storageGroupFilter;
  }

  public void setPrefixMatch(boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
  }
}
