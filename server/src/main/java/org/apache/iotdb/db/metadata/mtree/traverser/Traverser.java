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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.regex.Pattern;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

// This class defines the main traversal framework and declares some methods for result process
// extension.
public abstract class Traverser {

  protected IMNode startNode;
  protected String[] nodes;

  // if isMeasurementTraverser, measurement in template should be processed
  protected boolean isMeasurementTraverser = false;

  // default false means fullPath pattern match
  protected boolean isPrefixMatch = false;

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

    if (processMatchedMNode(node, idx, level)) {
      return;
    }

    if (idx >= nodes.length - 1) {
      if (multiLevelWildcard || isPrefixMatch) {
        processMultiLevelWildcard(node, idx, level);
      }
      return;
    }

    if (node.isMeasurement()) {
      return;
    }

    String targetName = nodes[idx + 1];
    if (MULTI_LEVEL_PATH_WILDCARD.equals(targetName)) {
      processMultiLevelWildcard(node, idx, level);
    } else if (targetName.contains(ONE_LEVEL_PATH_WILDCARD)) {
      processOneLevelWildcard(node, idx, multiLevelWildcard, level);
    } else {
      processNameMatch(node, idx, multiLevelWildcard, level);
    }
  }

  private boolean processMatchedMNode(IMNode node, int idx, int level) throws MetadataException {
    if (idx < nodes.length - 1) {
      return processInternalMatchedMNode(node, idx, level);
    } else {
      return processFullMatchedMNode(node, idx, level);
    }
  }

  protected abstract boolean processInternalMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

  protected abstract boolean processFullMatchedMNode(IMNode node, int idx, int level)
      throws MetadataException;

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

  public void setPrefixMatch(boolean isPrefixMatch) {
    this.isPrefixMatch = isPrefixMatch;
  }
}
