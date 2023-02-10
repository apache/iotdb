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
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import java.util.Map;

public class SchemaTreeMeasurementVisitor extends SchemaTreeVisitor<MeasurementPath> {

  private final String tailNode;

  public SchemaTreeMeasurementVisitor(
      SchemaNode root, PartialPath pathPattern, boolean isPrefixMatch) {
    super(root, pathPattern, isPrefixMatch);
    tailNode = pathPattern.getTailNode();
  }

  @Override
  protected IFAState tryGetNextState(
      SchemaNode node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    IFATransition transition;
    IFAState state;
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementNode().getAlias();
      if (alias != null) {
        transition = preciseMatchTransitionMap.get(alias);
        if (transition != null) {
          state = patternFA.getNextState(sourceState, transition);
          if (state.isFinal()) {
            return state;
          }
        }
      }
      transition = preciseMatchTransitionMap.get(node.getName());
      if (transition != null) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      return null;
    }

    transition = preciseMatchTransitionMap.get(node.getName());
    if (transition == null) {
      return null;
    }
    return patternFA.getNextState(sourceState, transition);
  }

  @Override
  protected IFAState tryGetNextState(
      SchemaNode node, IFAState sourceState, IFATransition transition) {
    IFAState state;
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementNode().getAlias();
      if (alias != null && transition.isMatch(alias)) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      if (transition.isMatch(node.getName())) {
        state = patternFA.getNextState(sourceState, transition);
        if (state.isFinal()) {
          return state;
        }
      }
      return null;
    }

    if (transition.isMatch(node.getName())) {
      return patternFA.getNextState(sourceState, transition);
    }
    return null;
  }

  @Override
  protected boolean acceptInternalMatchedNode(SchemaNode node) {
    return false;
  }

  @Override
  protected boolean acceptFullMatchedNode(SchemaNode node) {
    return node.isMeasurement();
  }

  @Override
  protected MeasurementPath generateResult(SchemaNode nextMatchedNode) {
    MeasurementPath result =
        new MeasurementPath(
            getFullPathFromRootToNode(nextMatchedNode),
            nextMatchedNode.getAsMeasurementNode().getSchema());
    result.setTagMap(nextMatchedNode.getAsMeasurementNode().getTagMap());
    result.setUnderAlignedEntity(getParentOfNextMatchedNode().getAsEntityNode().isAligned());
    String alias = nextMatchedNode.getAsMeasurementNode().getAlias();
    if (tailNode.equals(alias)) {
      result.setMeasurementAlias(alias);
    }

    return result;
  }
}
