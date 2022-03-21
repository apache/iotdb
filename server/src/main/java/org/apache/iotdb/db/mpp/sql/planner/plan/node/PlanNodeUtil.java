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
package org.apache.iotdb.db.mpp.sql.planner.plan.node;

public class PlanNodeUtil {
  public static void printPlanNode(PlanNode root) {
    printPlanNodeWithLevel(root, 0);
  }

  private static void printPlanNodeWithLevel(PlanNode root, int level) {
    printTab(level);
    System.out.println(root.toString());
    for (PlanNode child : root.getChildren()) {
      printPlanNodeWithLevel(child, level + 1);
    }
  }

  private static void printTab(int count) {
    for (int i = 0; i < count; i++) {
      System.out.print("\t");
    }
  }
}
