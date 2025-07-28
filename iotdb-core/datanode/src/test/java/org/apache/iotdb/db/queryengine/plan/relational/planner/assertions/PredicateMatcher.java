/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;

import java.util.function.Predicate;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.match;

public class PredicateMatcher<T extends PlanNode> implements Matcher {
  private final Predicate<T> predicate;

  public PredicateMatcher(Predicate<T> predicate) {
    this.predicate = predicate;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return predicate.test((T) node);
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    return match();
  }

  @Override
  public String toString() {
    return "(predicated)";
  }
}
