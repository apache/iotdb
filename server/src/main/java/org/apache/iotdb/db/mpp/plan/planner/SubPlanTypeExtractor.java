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

package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.SimplePlanVisitor;

public class SubPlanTypeExtractor {

  public static TypeProvider extractor(PlanNode root, TypeProvider allTypes) {
    TypeProvider typeProvider = new TypeProvider();
    root.accept(new Visitor(typeProvider, allTypes), null);
    return typeProvider;
  }

  private static class Visitor extends SimplePlanVisitor<Void> {

    private final TypeProvider typeProvider;
    private final TypeProvider allTypes;

    public Visitor(TypeProvider typeProvider, TypeProvider allTypes) {
      this.typeProvider = typeProvider;
      this.allTypes = allTypes;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      node.getOutputColumnNames()
          .forEach(name -> typeProvider.setType(name, allTypes.getType(name)));
      for (PlanNode source : node.getChildren()) {
        source.accept(this, context);
      }
      return null;
    }
  }
}
