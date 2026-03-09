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

import org.apache.iotdb.db.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.window;

public class ReplaceWindowWithRowNumber implements Rule<WindowNode> {
  private final Pattern<WindowNode> pattern;

  public ReplaceWindowWithRowNumber(Metadata metadata) {
    this.pattern =
        window()
            .matching(
                window -> {
                  if (window.getWindowFunctions().size() != 1) {
                    return false;
                  }
                  BoundSignature signature =
                      getOnlyElement(window.getWindowFunctions().values())
                          .getResolvedFunction()
                          .getSignature();
                  return signature.getArgumentTypes().isEmpty()
                      && signature.getName().equals("row_number");
                })
            .matching(window -> !window.getSpecification().getOrderingScheme().isPresent());
  }

  @Override
  public Pattern<WindowNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(WindowNode node, Captures captures, Context context) {
    return Result.ofPlanNode(
        new RowNumberNode(
            node.getPlanNodeId(),
            node.getChild(),
            node.getSpecification().getPartitionBy(),
            false,
            getOnlyElement(node.getWindowFunctions().keySet()),
            Optional.empty()));
  }
}
