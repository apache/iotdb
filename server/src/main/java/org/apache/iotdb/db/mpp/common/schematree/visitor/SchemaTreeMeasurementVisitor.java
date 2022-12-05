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
import org.apache.iotdb.commons.path.dfa.IFAState;
import org.apache.iotdb.commons.path.dfa.IFATransition;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import java.util.Map;

public class SchemaTreeMeasurementVisitor extends SchemaTreeVisitor<MeasurementPath> {

  private final String tailNode;

  public SchemaTreeMeasurementVisitor(
      SchemaNode root, PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    super(root, pathPattern, slimit, soffset, isPrefixMatch);
    tailNode = pathPattern.getTailNode();
  }

  @Override
  protected boolean checkIsMatch(SchemaNode node, IFAState sourceState, IFATransition transition) {
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementNode().getAlias();
      if (alias != null) {
        if (transition.isMatch(alias)
            && patternFA.getNextState(sourceState, transition).isFinal()) {
          return true;
        }
      }
      return transition.isMatch(node.getName())
          && patternFA.getNextState(sourceState, transition).isFinal();
    }
    return transition.isMatch(node.getName());
  }

  @Override
  protected IFATransition getMatchedTransition(
      SchemaNode node, IFAState sourceState, Map<String, IFATransition> preciseMatchTransitionMap) {
    if (node.isMeasurement()) {
      String alias = node.getAsMeasurementNode().getAlias();
      IFATransition transition = null;
      if (alias != null) {
        transition = preciseMatchTransitionMap.get(alias);
      }
      if (transition == null) {
        transition = preciseMatchTransitionMap.get(node.getName());
      }

      if (transition == null || !patternFA.getNextState(sourceState, transition).isFinal()) {
        return null;
      } else {
        return transition;
      }
    }
    return preciseMatchTransitionMap.get(node.getName());
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
            generateFullPathNodes(), nextMatchedNode.getAsMeasurementNode().getSchema());
    result.setTagMap(nextMatchedNode.getAsMeasurementNode().getTagMap());
    result.setUnderAlignedEntity(
        ancestorStack.get(ancestorStack.size() - 1).getNode().getAsEntityNode().isAligned());
    String alias = nextMatchedNode.getAsMeasurementNode().getAlias();
    if (tailNode.equals(alias)) {
      result.setMeasurementAlias(alias);
    }

    return result;
  }
}
