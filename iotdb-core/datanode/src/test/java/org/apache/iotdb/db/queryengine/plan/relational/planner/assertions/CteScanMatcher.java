/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CteScanNode;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;

public class CteScanMatcher implements Matcher {
  protected final String expectedCteName;
  // this field empty means no need to match
  protected final List<String> outputSymbols;

  public CteScanMatcher(String expectedCteName, List<String> outputSymbols) {
    this.expectedCteName = expectedCteName;
    this.outputSymbols = outputSymbols;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof CteScanNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());

    CteScanNode cteScanNode = (CteScanNode) node;
    String actualCteName = cteScanNode.getQualifiedName().toString();

    if (!expectedCteName.equalsIgnoreCase(actualCteName)) {
      return NO_MATCH;
    }

    if (!outputSymbols.isEmpty()
        && !outputSymbols.equals(
            cteScanNode.getOutputSymbols().stream()
                .map(Symbol::getName)
                .collect(Collectors.toList()))) {
      return NO_MATCH;
    }

    return new MatchResult(true);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .omitNullValues()
        .add("expectedCteName", expectedCteName)
        .add("outputSymbols", outputSymbols)
        .toString();
  }
}
