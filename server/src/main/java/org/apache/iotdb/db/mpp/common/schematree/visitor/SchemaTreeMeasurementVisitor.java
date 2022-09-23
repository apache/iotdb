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

package org.apache.iotdb.db.mpp.common.schematree.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaMeasurementNode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import java.util.regex.Pattern;

public class SchemaTreeMeasurementVisitor extends SchemaTreeVisitor<MeasurementPath> {

  public SchemaTreeMeasurementVisitor(
      SchemaNode root, PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    super(root, pathPattern, slimit, soffset, isPrefixMatch);
  }

  @Override
  protected boolean checkOneLevelWildcardMatch(String regex, SchemaNode node) {
    if (!node.isMeasurement()) {
      return Pattern.matches(regex, node.getName());
    }

    SchemaMeasurementNode measurementNode = node.getAsMeasurementNode();

    return Pattern.matches(regex, measurementNode.getName())
        || Pattern.matches(regex, measurementNode.getAlias());
  }

  @Override
  protected boolean checkNameMatch(String targetName, SchemaNode node) {
    if (node.isMeasurement()) {
      return targetName.equals(node.getName())
          || targetName.equals(node.getAsMeasurementNode().getAlias());
    }
    return targetName.equals(node.getName());
  }

  @Override
  protected boolean processInternalMatchedNode(SchemaNode node) {
    return true;
  }

  @Override
  protected boolean processFullMatchedNode(SchemaNode node) {
    if (node.isMeasurement()) {
      nextMatchedNode = node;
      return false;
    }
    return true;
  }

  @Override
  protected MeasurementPath generateResult() {
    MeasurementPath result =
        new MeasurementPath(
            generateFullPathNodes(nextMatchedNode),
            nextMatchedNode.getAsMeasurementNode().getSchema());
    result.setUnderAlignedEntity(ancestorStack.peek().getNode().getAsEntityNode().isAligned());
    String alias = nextMatchedNode.getAsMeasurementNode().getAlias();
    if (nodes[nodes.length - 1].equals(alias)) {
      result.setMeasurementAlias(alias);
    }

    return result;
  }
}
