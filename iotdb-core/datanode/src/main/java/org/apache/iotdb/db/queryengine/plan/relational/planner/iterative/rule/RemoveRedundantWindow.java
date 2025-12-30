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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isEmpty;

public class RemoveRedundantWindow implements Rule<WindowNode> {
  private static final Pattern<WindowNode> PATTERN = window();

  @Override
  public Pattern<WindowNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(WindowNode window, Captures captures, Context context) {
    if (isEmpty(window.getChild(), context.getLookup())) {
      return Result.ofPlanNode(
          new ValuesNode(window.getPlanNodeId(), window.getOutputSymbols(), ImmutableList.of()));
    }
    return Result.empty();
  }
}
